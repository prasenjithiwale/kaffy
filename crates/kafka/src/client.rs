use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::StreamExt;
use kaffy_config::Profile;
use kaffy_schema::Registry;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::admin::{self, Admin};
use crate::groups::Groups;
use crate::replay;
use crate::{
    Command, EVENT_BUFFER, Error, Event, Header, Operation, PartitionInfo, Payload, ProduceRequest,
    Record, StartAt, TopicInfo, Worker,
};

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts a worker connected to the cluster described by `profile`.
///
/// Fails fast on configuration problems (missing secrets, unknown librdkafka
/// properties). Connection errors arrive later as [`Event::Error`].
pub fn spawn(profile: &Profile) -> Result<Worker, Error> {
    let mut base = ClientConfig::new();
    base.set("client.id", "kaffy");
    for (key, value) in profile.librdkafka_properties()? {
        base.set(key, value);
    }
    let metadata: Arc<BaseConsumer> = Arc::new(viewer_config(&base).create()?);
    let registry = match profile.resolved_schema_registry()? {
        Some(sr) => Some(Arc::new(
            Registry::new(&sr.url, sr.basic_auth).map_err(Error::SchemaRegistry)?,
        )),
        None => None,
    };

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::channel(EVENT_BUFFER);
    let ctx = Context {
        groups: Arc::new(Groups::new(base.clone(), metadata.clone())),
        base,
        metadata,
        registry,
        producer: Arc::new(OnceLock::new()),
        admin: Arc::new(OnceLock::new()),
        read_only: profile.read_only,
        events: event_tx,
    };
    tokio::spawn(run(ctx, command_rx));
    Ok(Worker {
        commands: command_tx,
        events: event_rx,
    })
}

/// Consumer settings that never touch real consumer groups: partitions are
/// assigned manually, so the group id is never joined and nothing is committed.
fn viewer_config(base: &ClientConfig) -> ClientConfig {
    let mut config = base.clone();
    config
        .set("group.id", format!("kaffy-viewer-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest");
    config
}

/// Producer settings shared by produce and replay.
fn producer_config(base: &ClientConfig) -> ClientConfig {
    let mut config = base.clone();
    config
        // Same key-to-partition mapping as the Java client, so keyed records land
        // where Java producers would put them.
        .set("partitioner", "murmur2_random")
        // Keeps per-partition order on retries without needing idempotence ACLs.
        .set("max.in.flight.requests.per.connection", "1")
        .set("linger.ms", "5");
    config
}

struct Tail {
    control: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
struct Context {
    base: ClientConfig,
    metadata: Arc<BaseConsumer>,
    groups: Arc<Groups>,
    registry: Option<Arc<Registry>>,
    /// Created on the first write, so read-only sessions never open one.
    producer: Arc<OnceLock<FutureProducer>>,
    /// Created on first use, like the producer.
    admin: Arc<OnceLock<Admin>>,
    read_only: bool,
    events: mpsc::Sender<Event>,
}

impl Context {
    fn producer(&self) -> Result<FutureProducer, String> {
        if let Some(producer) = self.producer.get() {
            return Ok(producer.clone());
        }
        let producer: FutureProducer = producer_config(&self.base)
            .create()
            .map_err(|e| format!("creating producer: {e}"))?;
        Ok(self.producer.get_or_init(|| producer).clone())
    }

    fn admin(&self) -> Result<&Admin, String> {
        if let Some(admin) = self.admin.get() {
            return Ok(admin);
        }
        let admin: Admin = self
            .base
            .create()
            .map_err(|e| format!("creating admin client: {e}"))?;
        Ok(self.admin.get_or_init(|| admin))
    }

    /// Runs async `work` that needs the admin client and reports its result.
    fn with_admin<T, F>(
        &self,
        operation: Operation,
        work: impl FnOnce(Context) -> F + Send + 'static,
        to_event: impl FnOnce(T) -> Event + Send + 'static,
    ) where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, String>> + Send + 'static,
    {
        let ctx = self.clone();
        tokio::spawn(async move {
            let events = ctx.events.clone();
            let event = match work(ctx).await {
                Ok(value) => to_event(value),
                Err(message) => Event::Failed { operation, message },
            };
            let _ = events.send(event).await;
        });
    }

    /// Runs blocking `work` off the async threads and reports its result.
    fn blocking<T: Send + 'static>(
        &self,
        operation: Operation,
        work: impl FnOnce() -> Result<T, String> + Send + 'static,
        to_event: impl FnOnce(T) -> Event + Send + 'static,
    ) {
        let events = self.events.clone();
        tokio::spawn(async move {
            let event = match tokio::task::spawn_blocking(work).await {
                Ok(Ok(value)) => to_event(value),
                Ok(Err(message)) => Event::Failed { operation, message },
                Err(e) => Event::Failed {
                    operation,
                    message: format!("internal error: {e}"),
                },
            };
            let _ = events.send(event).await;
        });
    }

    /// Writes are refused here as well as in the UI.
    fn refuse_if_read_only(&self, operation: Operation) -> bool {
        if self.read_only {
            let events = self.events.clone();
            tokio::spawn(async move {
                let _ = events
                    .send(Event::Failed {
                        operation,
                        message: READ_ONLY.to_owned(),
                    })
                    .await;
            });
        }
        self.read_only
    }
}

const READ_ONLY: &str =
    "this profile is read-only; set read_only = false in its config to allow changes";

async fn run(ctx: Context, mut commands: mpsc::UnboundedReceiver<Command>) {
    let mut tail: Option<Tail> = None;
    let mut replay: Option<(oneshot::Sender<()>, JoinHandle<()>)> = None;
    while let Some(command) = commands.recv().await {
        match command {
            Command::ListTopics => {
                let metadata = ctx.metadata.clone();
                let events = ctx.events.clone();
                tokio::spawn(async move {
                    let event = match tokio::task::spawn_blocking(move || fetch_topics(&metadata))
                        .await
                        .expect("metadata task panicked")
                    {
                        Ok((brokers, topics)) => Event::Metadata { brokers, topics },
                        Err(e) => Event::Error(format!("metadata: {e}")),
                    };
                    let _ = events.send(event).await;
                });
            }
            Command::Tail {
                session,
                topic,
                start,
            } => {
                if let Some(old) = tail.take() {
                    old.task.abort();
                }
                let (control, control_rx) = mpsc::unbounded_channel();
                let task = tokio::spawn(run_tail(
                    viewer_config(&ctx.base),
                    ctx.registry.clone(),
                    session,
                    topic,
                    start,
                    control_rx,
                    ctx.events.clone(),
                ));
                tail = Some(Tail { control, task });
            }
            Command::StopTail => {
                if let Some(old) = tail.take() {
                    old.task.abort();
                }
            }
            Command::Pause | Command::Resume => {
                if let Some(tail) = &tail {
                    let _ = tail.control.send(command);
                }
            }
            Command::ListGroups => {
                let groups = ctx.groups.clone();
                ctx.blocking(
                    Operation::ListGroups,
                    move || groups.list().map_err(|e| e.to_string()),
                    Event::Groups,
                );
            }
            Command::DescribeGroup(name) => {
                let groups = ctx.groups.clone();
                ctx.blocking(
                    Operation::DescribeGroup,
                    move || groups.describe(&name).map_err(|e| e.to_string()),
                    Event::GroupDetail,
                );
            }
            Command::PlanReset {
                group,
                topic,
                target,
            } => {
                let groups = ctx.groups.clone();
                ctx.blocking(
                    Operation::PlanReset,
                    move || groups.plan_reset(&group, topic.as_deref(), target),
                    Event::ResetPlan,
                );
            }
            Command::ApplyReset { group, offsets } => {
                if ctx.refuse_if_read_only(Operation::ApplyReset) {
                    continue;
                }
                let groups = ctx.groups.clone();
                ctx.blocking(
                    Operation::ApplyReset,
                    move || {
                        let partitions = groups.apply_reset(&group, &offsets)?;
                        Ok((group, partitions))
                    },
                    |(group, partitions)| Event::ResetApplied { group, partitions },
                );
            }
            Command::Produce(request) => {
                if ctx.refuse_if_read_only(Operation::Produce) {
                    continue;
                }
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let id = request.id;
                    let topic = request.topic.clone();
                    let event = match produce(&ctx, request).await {
                        Ok((partition, offset)) => Event::Produced {
                            id,
                            topic,
                            partition,
                            offset,
                        },
                        Err(message) => Event::Failed {
                            operation: Operation::Produce,
                            message,
                        },
                    };
                    let _ = ctx.events.send(event).await;
                });
            }
            Command::PlanReplay {
                source,
                target,
                start,
                end,
            } => {
                let metadata = ctx.metadata.clone();
                ctx.blocking(
                    Operation::PlanReplay,
                    move || replay::plan(&metadata, &source, &target, start, end),
                    Event::ReplayPlan,
                );
            }
            Command::Replay { id, plan, filter } => {
                if ctx.refuse_if_read_only(Operation::Replay) {
                    continue;
                }
                if replay.as_ref().is_some_and(|(_, task)| !task.is_finished()) {
                    let _ = ctx
                        .events
                        .send(Event::Failed {
                            operation: Operation::Replay,
                            message: "a replay is already running".to_owned(),
                        })
                        .await;
                    continue;
                }
                let producer = match ctx.producer() {
                    Ok(producer) => producer,
                    Err(message) => {
                        let _ = ctx
                            .events
                            .send(Event::Failed {
                                operation: Operation::Replay,
                                message,
                            })
                            .await;
                        continue;
                    }
                };
                let (cancel_tx, cancel_rx) = oneshot::channel();
                let job = replay::Job {
                    config: viewer_config(&ctx.base),
                    producer,
                    registry: ctx.registry.clone(),
                    id,
                    plan,
                    filter,
                };
                let task = tokio::spawn(replay::run(job, cancel_rx, ctx.events.clone()));
                replay = Some((cancel_tx, task));
            }
            Command::CancelReplay => {
                if let Some((cancel, _)) = replay.take() {
                    let _ = cancel.send(());
                }
            }
            Command::CreateTopic(spec) => {
                if ctx.refuse_if_read_only(Operation::CreateTopic) {
                    continue;
                }
                ctx.with_admin(
                    Operation::CreateTopic,
                    |ctx| async move {
                        admin::create_topic(ctx.admin()?, &spec).await?;
                        Ok(spec.name)
                    },
                    |name| Event::TopicCreated { name },
                );
            }
            Command::DeleteTopic(name) => {
                if ctx.refuse_if_read_only(Operation::DeleteTopic) {
                    continue;
                }
                ctx.with_admin(
                    Operation::DeleteTopic,
                    |ctx| async move {
                        admin::delete_topic(ctx.admin()?, &name).await?;
                        Ok(name)
                    },
                    |name| Event::TopicDeleted { name },
                );
            }
            Command::DescribeTopicConfig(topic) => {
                ctx.with_admin(
                    Operation::TopicConfig,
                    |ctx| async move {
                        let entries = admin::describe_config(ctx.admin()?, &topic).await?;
                        Ok((topic, entries))
                    },
                    |(topic, entries)| Event::TopicConfig { topic, entries },
                );
            }
            Command::AlterTopicConfig { topic, changes } => {
                if ctx.refuse_if_read_only(Operation::AlterTopicConfig) {
                    continue;
                }
                ctx.with_admin(
                    Operation::AlterTopicConfig,
                    |ctx| async move {
                        let changed = admin::alter_config(ctx.admin()?, &topic, &changes).await?;
                        Ok((topic, changed))
                    },
                    |(topic, changed)| Event::TopicConfigAltered { topic, changed },
                );
            }
            Command::PlanDeleteRecords { topic, before } => {
                let metadata = ctx.metadata.clone();
                ctx.blocking(
                    Operation::PlanDeleteRecords,
                    move || admin::plan_delete_records(&metadata, &topic, before),
                    Event::DeleteRecordsPlan,
                );
            }
            Command::DeleteRecords { topic, offsets } => {
                if ctx.refuse_if_read_only(Operation::DeleteRecords) {
                    continue;
                }
                ctx.with_admin(
                    Operation::DeleteRecords,
                    |ctx| async move {
                        let records =
                            admin::delete_records(ctx.admin()?, &ctx.metadata, &topic, &offsets)
                                .await?;
                        Ok((topic, records))
                    },
                    |(topic, records)| Event::RecordsDeleted { topic, records },
                );
            }
        }
    }
}

async fn produce(ctx: &Context, request: ProduceRequest) -> Result<(i32, i64), String> {
    // Check the topic first: the broker may otherwise auto-create a mistyped one.
    let partitions = {
        let metadata = ctx.metadata.clone();
        let topic = request.topic.clone();
        tokio::task::spawn_blocking(move || topic_partition_count(&metadata, &topic))
            .await
            .map_err(|e| e.to_string())??
    };
    if let Some(p) = request.partition {
        if p < 0 || p as usize >= partitions {
            return Err(format!(
                "topic {} has partitions 0 to {}; there is no partition {p}",
                request.topic,
                partitions - 1
            ));
        }
    }
    let value = match request.value {
        None => None,
        Some(Payload::Bytes(bytes)) => Some(bytes),
        Some(Payload::Avro { schema_id, json }) => {
            let registry = ctx
                .registry
                .as_ref()
                .ok_or("this profile has no Schema Registry to encode Avro with")?;
            Some(registry.encode_avro(schema_id, &json).await?)
        }
    };
    let mut headers = OwnedHeaders::new_with_capacity(request.headers.len());
    for h in &request.headers {
        headers = headers.insert(rdkafka::message::Header {
            key: &h.key,
            value: h.value.as_deref(),
        });
    }
    let mut record = FutureRecord::<[u8], [u8]>::to(&request.topic).headers(headers);
    if let Some(key) = request.key.as_deref() {
        record = record.key(key);
    }
    if let Some(value) = value.as_deref() {
        record = record.payload(value);
    }
    if let Some(partition) = request.partition {
        record = record.partition(partition);
    }
    let delivery = ctx
        .producer()?
        .send(record, Duration::from_secs(30))
        .await
        .map_err(|(e, _)| format!("producing to {}: {e}", request.topic))?;
    Ok((delivery.partition, delivery.offset))
}

fn topic_partition_count(admin: &BaseConsumer, topic: &str) -> Result<usize, String> {
    let metadata = admin
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .map_err(|e| format!("{topic}: {e}"))?;
    match metadata.topics().first() {
        Some(t) if t.error().is_none() && !t.partitions().is_empty() => Ok(t.partitions().len()),
        _ => Err(format!("topic {topic} does not exist")),
    }
}

fn fetch_topics(consumer: &BaseConsumer) -> KafkaResult<(usize, Vec<TopicInfo>)> {
    let metadata = consumer.fetch_metadata(None, METADATA_TIMEOUT)?;
    let mut topics: Vec<TopicInfo> = metadata
        .topics()
        .iter()
        .map(|topic| TopicInfo {
            name: topic.name().to_owned(),
            partitions: topic
                .partitions()
                .iter()
                .map(|p| PartitionInfo {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect(),
        })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((metadata.brokers().len(), topics))
}

async fn run_tail(
    config: ClientConfig,
    registry: Option<Arc<Registry>>,
    session: u64,
    topic: String,
    start: StartAt,
    control: mpsc::UnboundedReceiver<Command>,
    events: mpsc::Sender<Event>,
) {
    if let Err(e) = tail(config, registry, session, &topic, start, control, &events).await {
        let _ = events
            .send(Event::Error(format!("tail {topic}: {e}")))
            .await;
    }
}

async fn tail(
    config: ClientConfig,
    registry: Option<Arc<Registry>>,
    session: u64,
    topic: &str,
    start: StartAt,
    mut control: mpsc::UnboundedReceiver<Command>,
    events: &mpsc::Sender<Event>,
) -> KafkaResult<()> {
    let consumer: Arc<StreamConsumer> = Arc::new(config.create()?);
    let partitions = {
        let consumer = consumer.clone();
        let topic = topic.to_owned();
        tokio::task::spawn_blocking(move || assign(&consumer, &topic, start))
            .await
            .expect("assign task panicked")?
    };
    if events
        .send(Event::TailStarted {
            session,
            partitions,
        })
        .await
        .is_err()
    {
        return Ok(());
    }

    let mut stream = consumer.stream();
    loop {
        tokio::select! {
            message = stream.next() => match message {
                Some(Ok(message)) => {
                    let mut record = to_record(&message);
                    drop(message);
                    if let Some(registry) = &registry {
                        decode_record(registry, &mut record).await;
                    }
                    let event = Event::Record {
                        session,
                        record: Box::new(record),
                    };
                    if events.send(event).await.is_err() {
                        return Ok(());
                    }
                }
                Some(Err(e)) => {
                    tracing::warn!(%topic, error = %e, "consume error");
                    let _ = events.send(Event::Error(format!("consume {topic}: {e}"))).await;
                }
                None => return Ok(()),
            },
            command = control.recv() => match command {
                Some(Command::Pause) => consumer.pause(&consumer.assignment()?)?,
                Some(Command::Resume) => consumer.resume(&consumer.assignment()?)?,
                Some(_) => {}
                None => return Ok(()),
            },
        }
    }
}

/// Assigns every partition of `topic` at the requested start position and
/// returns the partition count. Blocking: call from `spawn_blocking`.
fn assign(consumer: &StreamConsumer, topic: &str, start: StartAt) -> KafkaResult<usize> {
    let metadata = consumer.fetch_metadata(Some(topic), METADATA_TIMEOUT)?;
    let topic_md = metadata
        .topics()
        .first()
        .ok_or(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopic))?;
    if let Some(err) = topic_md.error() {
        return Err(KafkaError::MetadataFetch(err.into()));
    }

    let offset = match start {
        StartAt::Beginning => Offset::Beginning,
        StartAt::End => Offset::End,
        StartAt::LastN(n) => Offset::OffsetTail(n),
        StartAt::Offset(n) => Offset::Offset(n),
        // offsets_for_times() takes the timestamp in the offset field.
        StartAt::Timestamp(ms) => Offset::Offset(ms),
    };
    let mut tpl = TopicPartitionList::new();
    for partition in topic_md.partitions() {
        let offset = match start {
            // An out-of-range offset would trigger auto.offset.reset and replay the
            // whole partition, so clamp it to what the partition actually holds.
            StartAt::Offset(n) => {
                let (low, high) =
                    consumer.fetch_watermarks(topic, partition.id(), METADATA_TIMEOUT)?;
                Offset::Offset(n.clamp(low, high))
            }
            _ => offset,
        };
        tpl.add_partition_offset(topic, partition.id(), offset)?;
    }
    if let StartAt::Timestamp(_) = start {
        tpl = consumer.offsets_for_times(tpl, METADATA_TIMEOUT)?;
    }
    consumer.assign(&tpl)?;
    Ok(topic_md.partitions().len())
}

async fn decode(registry: &Registry, bytes: Option<&[u8]>) -> Option<kaffy_schema::Decoded> {
    registry.decode(bytes?).await
}

/// Fills in the decoded key and value of a Schema Registry payload.
pub(crate) async fn decode_record(registry: &Registry, record: &mut Record) {
    record.key_decoded = decode(registry, record.key.as_deref()).await;
    record.value_decoded = decode(registry, record.value.as_deref()).await;
}

pub(crate) fn to_record(message: &BorrowedMessage<'_>) -> Record {
    Record {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp_ms: message.timestamp().to_millis(),
        key: message.key().map(<[u8]>::to_vec),
        value: message.payload().map(<[u8]>::to_vec),
        headers: message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|h| Header {
                        key: h.key.to_owned(),
                        value: h.value.map(<[u8]>::to_vec),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        key_decoded: None,
        value_decoded: None,
    }
}
