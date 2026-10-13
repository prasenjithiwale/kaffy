use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kaffy_config::Profile;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{
    Command, EVENT_BUFFER, Error, Event, Header, PartitionInfo, Record, StartAt, TopicInfo, Worker,
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

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::channel(EVENT_BUFFER);
    tokio::spawn(run(base, metadata, command_rx, event_tx));
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

struct Tail {
    control: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
}

async fn run(
    base: ClientConfig,
    metadata: Arc<BaseConsumer>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::Sender<Event>,
) {
    let mut tail: Option<Tail> = None;
    while let Some(command) = commands.recv().await {
        match command {
            Command::ListTopics => {
                let metadata = metadata.clone();
                let events = events.clone();
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
                    viewer_config(&base),
                    session,
                    topic,
                    start,
                    control_rx,
                    events.clone(),
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
        }
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
    session: u64,
    topic: String,
    start: StartAt,
    control: mpsc::UnboundedReceiver<Command>,
    events: mpsc::Sender<Event>,
) {
    if let Err(e) = tail(config, session, &topic, start, control, &events).await {
        let _ = events
            .send(Event::Error(format!("tail {topic}: {e}")))
            .await;
    }
}

async fn tail(
    config: ClientConfig,
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
                    let event = Event::Record { session, record: to_record(&message) };
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
        tpl.add_partition_offset(topic, partition.id(), offset)?;
    }
    if let StartAt::Timestamp(_) = start {
        tpl = consumer.offsets_for_times(tpl, METADATA_TIMEOUT)?;
    }
    consumer.assign(&tpl)?;
    Ok(topic_md.partitions().len())
}

fn to_record(message: &BorrowedMessage<'_>) -> Record {
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
    }
}
