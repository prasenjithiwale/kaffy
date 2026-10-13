//! End-to-end tests against a real broker. They are ignored by default; run with
//!
//! ```sh
//! cargo test -p kaffy-kafka --test local_broker -- --ignored
//! ```
//!
//! `KAFFY_TEST_BOOTSTRAP` overrides the broker (default `localhost:9092`).
//! A mock Schema Registry is started in-process.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use apache_avro::Schema;
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use kaffy_config::{Profile, SchemaRegistry};
use kaffy_kafka::{Command, Event, Record, StartAt, Worker};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SCHEMA: &str = r#"{"type": "record", "name": "Order", "fields": [
    {"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}"#;
const SCHEMA_ID: u32 = 42;
const TIMEOUT: Duration = Duration::from_secs(20);

fn bootstrap() -> String {
    std::env::var("KAFFY_TEST_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_owned())
}

/// Serves `GET /schemas/ids/42` and nothing else.
async fn mock_registry() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = if request.starts_with(&format!("GET /schemas/ids/{SCHEMA_ID} ")) {
                ("200 OK", json!({ "schema": SCHEMA }).to_string())
            } else {
                (
                    "404 Not Found",
                    r#"{"error_code":40403,"message":"Schema not found"}"#.to_owned(),
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    url
}

/// Creates a fresh two-partition topic and deletes it when dropped.
struct TestTopic {
    name: String,
    admin: AdminClient<DefaultClientContext>,
}

impl TestTopic {
    async fn create() -> Self {
        let nanos = unique_suffix();
        let name = format!("kaffy-test-{nanos}");
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap())
            .create()
            .unwrap();
        let results = admin
            .create_topics(
                &[NewTopic::new(&name, 2, TopicReplication::Fixed(1))],
                &AdminOptions::new().operation_timeout(Some(TIMEOUT)),
            )
            .await
            .unwrap();
        results.into_iter().for_each(|r| {
            r.unwrap();
        });
        Self { name, admin }
    }
}

impl Drop for TestTopic {
    fn drop(&mut self) {
        let name = self.name.clone();
        let admin = &self.admin;
        let _ = futures::executor::block_on(admin.delete_topics(&[&name], &AdminOptions::new()));
    }
}

fn avro_order(id: &str, amount: f64) -> Vec<u8> {
    let schema = Schema::parse_str(SCHEMA).unwrap();
    let value = AvroValue::Record(vec![
        ("id".into(), AvroValue::String(id.into())),
        ("amount".into(), AvroValue::Double(amount)),
    ]);
    let body = GenericDatumWriter::builder(&schema)
        .build()
        .unwrap()
        .write_value_to_vec(value)
        .unwrap();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&SCHEMA_ID.to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes
}

async fn produce(topic: &str, partition: i32, key: &str, value: &[u8]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .unwrap();
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key(key)
                .payload(value),
            TIMEOUT,
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
}

/// Starts a tail and collects records until `count` arrive or nothing more comes.
async fn tail(worker: &mut Worker, topic: &str, start: StartAt, count: usize) -> Vec<Record> {
    worker
        .commands
        .send(Command::Tail {
            session: 1,
            topic: topic.to_owned(),
            start,
        })
        .unwrap();
    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    // After the expected records, wait briefly to catch unexpected extras.
    let mut quiet_until = None;
    loop {
        let until = quiet_until.unwrap_or(deadline);
        match tokio::time::timeout_at(until, worker.events.recv()).await {
            Ok(Some(Event::Record { record, .. })) => {
                records.push(*record);
                if records.len() >= count {
                    quiet_until = Some(tokio::time::Instant::now() + Duration::from_secs(2));
                }
            }
            Ok(Some(Event::Error(e))) => panic!("worker error: {e}"),
            Ok(Some(_)) => {}
            Ok(None) => panic!("worker stopped"),
            Err(_) => break,
        }
    }
    worker.commands.send(Command::StopTail).unwrap();
    records.sort_by_key(|r| (r.partition, r.offset));
    records
}

#[tokio::test]
#[ignore = "needs a Kafka broker"]
async fn decodes_avro_through_schema_registry() {
    let topic = TestTopic::create().await;
    produce(&topic.name, 0, "cust-1", &avro_order("ord-1", 12.5)).await;
    produce(&topic.name, 0, "cust-2", &avro_order("ord-2", 99.0)).await;
    produce(&topic.name, 1, "cust-3", br#"{"plain":"json"}"#).await;

    let mut profile = Profile::adhoc(bootstrap());
    profile.schema_registry = Some(SchemaRegistry {
        url: mock_registry().await,
        username: None,
        password_env: None,
    });
    let mut worker = kaffy_kafka::spawn(&profile).unwrap();
    let records = tail(&mut worker, &topic.name, StartAt::Beginning, 3).await;

    assert_eq!(records.len(), 3);
    let decoded = records[0].value_decoded.as_ref().expect("avro is decoded");
    assert_eq!(decoded.schema_id, SCHEMA_ID);
    assert_eq!(decoded.result, Ok(json!({"id": "ord-1", "amount": 12.5})));
    assert_eq!(
        records[1].value_decoded.as_ref().unwrap().result,
        Ok(json!({"id": "ord-2", "amount": 99.0}))
    );
    assert_eq!(
        records[2].value_decoded, None,
        "plain JSON is not wire format"
    );
    assert_eq!(records[0].key.as_deref(), Some(&b"cust-1"[..]));
}

#[tokio::test]
#[ignore = "needs a Kafka broker"]
async fn offset_seek_is_clamped_per_partition() {
    let topic = TestTopic::create().await;
    for i in 0..4 {
        produce(&topic.name, 0, &format!("k{i}"), b"p0").await;
    }
    produce(&topic.name, 1, "k", b"p1").await;

    let mut worker = kaffy_kafka::spawn(&Profile::adhoc(bootstrap())).unwrap();
    // Partition 1 only has offset 0; seeking to 2 must not replay it from the start.
    let records = tail(&mut worker, &topic.name, StartAt::Offset(2), 2).await;
    let positions: Vec<_> = records.iter().map(|r| (r.partition, r.offset)).collect();
    assert_eq!(positions, vec![(0, 2), (0, 3)]);
}

// ---- Phase 2: consumer groups, offset resets, produce, replay ----

use kaffy_kafka::{
    GroupState, Header, Operation, Payload, ProduceRequest, RecordFilter, ReplayEnd, ResetTarget,
};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{Offset, TopicPartitionList};

/// Waits for the first event `pick` accepts; fails on an unexpected `Failed`.
async fn expect<T>(worker: &mut Worker, mut pick: impl FnMut(Event) -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, worker.events.recv()).await {
            Ok(Some(event)) => {
                let failure = match &event {
                    Event::Failed { operation, message } => {
                        Some(format!("{operation:?}: {message}"))
                    }
                    _ => None,
                };
                if let Some(value) = pick(event) {
                    return value;
                }
                if let Some(failure) = failure {
                    panic!("unexpected failure: {failure}");
                }
            }
            Ok(None) => panic!("worker stopped"),
            Err(_) => panic!("timed out waiting for an event"),
        }
    }
}

/// Waits for a `Failed` event of `operation` and returns its message.
async fn expect_failure(worker: &mut Worker, operation: Operation) -> String {
    expect(worker, |e| match e {
        Event::Failed {
            operation: op,
            message,
        } if op == operation => Some(message),
        _ => None,
    })
    .await
}

fn writable() -> Profile {
    let mut profile = Profile::adhoc(bootstrap());
    profile.read_only = false;
    profile
}

fn unique(prefix: &str) -> String {
    let nanos = unique_suffix();
    format!("{prefix}-{nanos}")
}

/// Commits offsets for `group` without joining it, leaving the group Empty.
fn commit_offsets(group: &str, topic: &str, offsets: &[(i32, i64)]) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for &(partition, offset) in offsets {
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .unwrap();
    }
    consumer.commit(&tpl, CommitMode::Sync).unwrap();
}

#[tokio::test]
#[ignore = "needs a Kafka broker"]
async fn group_lag_and_offset_reset() {
    let topic = TestTopic::create().await;
    for i in 0..5 {
        produce(&topic.name, 0, &format!("k{i}"), b"p0").await;
    }
    for i in 0..3 {
        produce(&topic.name, 1, &format!("k{i}"), b"p1").await;
    }
    let group = unique("kaffy-test-group");
    commit_offsets(&group, &topic.name, &[(0, 2), (1, 1)]);

    let mut worker = kaffy_kafka::spawn(&writable()).unwrap();
    worker.commands.send(Command::ListGroups).unwrap();
    let summary = expect(&mut worker, |e| match e {
        Event::Groups(groups) => groups.into_iter().find(|g| g.name == group),
        _ => None,
    })
    .await;
    assert_eq!(summary.state, GroupState::Empty);
    assert_eq!(summary.topics, vec![topic.name.clone()]);
    assert_eq!(summary.lag, Some(3 + 2));

    worker
        .commands
        .send(Command::DescribeGroup(group.clone()))
        .unwrap();
    let detail = expect(&mut worker, |e| match e {
        Event::GroupDetail(d) => Some(d),
        _ => None,
    })
    .await;
    let lags: Vec<_> = detail
        .partitions
        .iter()
        .map(|p| (p.partition, p.committed, p.end))
        .collect();
    assert_eq!(lags, vec![(0, 2, 5), (1, 1, 3)]);

    // Each target, previewed without changing anything.
    let mut plan_for = async |target| {
        worker
            .commands
            .send(Command::PlanReset {
                group: group.clone(),
                topic: None,
                target,
            })
            .unwrap();
        expect(&mut worker, |e| match e {
            Event::ResetPlan(p) => Some(p),
            _ => None,
        })
        .await
    };
    let new_offsets =
        |plan: &kaffy_kafka::ResetPlan| plan.rows.iter().map(|r| r.new).collect::<Vec<_>>();
    assert_eq!(
        new_offsets(&plan_for(ResetTarget::Latest).await),
        vec![5, 3]
    );
    assert_eq!(
        new_offsets(&plan_for(ResetTarget::Offset(4)).await),
        vec![4, 3],
        "clamped to the end"
    );
    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 3_600_000;
    assert_eq!(
        new_offsets(&plan_for(ResetTarget::Timestamp(future)).await),
        vec![5, 3]
    );
    let earliest = plan_for(ResetTarget::Earliest).await;
    assert_eq!(new_offsets(&earliest), vec![0, 0]);
    assert_eq!(earliest.rows[0].current, Some(2));

    // Read-only profiles refuse to apply.
    let mut read_only = kaffy_kafka::spawn(&Profile::adhoc(bootstrap())).unwrap();
    read_only
        .commands
        .send(Command::ApplyReset {
            group: group.clone(),
            offsets: earliest.offsets(),
        })
        .unwrap();
    assert!(
        expect_failure(&mut read_only, Operation::ApplyReset)
            .await
            .contains("read-only")
    );

    worker
        .commands
        .send(Command::ApplyReset {
            group: group.clone(),
            offsets: earliest.offsets(),
        })
        .unwrap();
    let applied = expect(&mut worker, |e| match e {
        Event::ResetApplied { partitions, .. } => Some(partitions),
        _ => None,
    })
    .await;
    assert_eq!(applied, 2);
    worker
        .commands
        .send(Command::DescribeGroup(group.clone()))
        .unwrap();
    let detail = expect(&mut worker, |e| match e {
        Event::GroupDetail(d) => Some(d),
        _ => None,
    })
    .await;
    assert_eq!(detail.total_lag(), 8);

    // An active member blocks the reset.
    let member: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    member.subscribe(&[&topic.name]).unwrap();
    let joined = tokio::time::Instant::now() + TIMEOUT;
    while member.assignment().unwrap().count() == 0 {
        assert!(tokio::time::Instant::now() < joined, "member never joined");
        member.poll(Duration::from_millis(200));
    }
    worker
        .commands
        .send(Command::ApplyReset {
            group: group.clone(),
            offsets: earliest.offsets(),
        })
        .unwrap();
    let error = expect_failure(&mut worker, Operation::ApplyReset).await;
    assert!(error.contains("active member"), "{error}");

    // Leave no test group behind on the cluster.
    member.unsubscribe();
    drop(member);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .unwrap();
    for _ in 0..20 {
        let deleted = admin
            .delete_groups(&[&group], &AdminOptions::new())
            .await
            .unwrap();
        if deleted.iter().all(|r| r.is_ok()) {
            break;
        }
        // The departed member may still be leaving; deletion needs an empty group.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore = "needs a Kafka broker"]
async fn produce_and_filtered_replay() {
    let source = TestTopic::create().await;
    let target = TestTopic::create().await;
    let mut worker = kaffy_kafka::spawn(&writable()).unwrap();

    // Produce through the worker, with headers and an explicit partition.
    for (i, value) in ["keep-1", "drop-1", "keep-2", "drop-2", "keep-3"]
        .iter()
        .enumerate()
    {
        let request = ProduceRequest {
            id: i as u64,
            topic: source.name.clone(),
            partition: Some((i % 2) as i32),
            key: Some(format!("key-{i}").into_bytes()),
            value: Some(Payload::Bytes(value.as_bytes().to_vec())),
            headers: vec![Header {
                key: "trace".into(),
                value: Some(format!("t{i}").into_bytes()),
            }],
        };
        worker.commands.send(Command::Produce(request)).unwrap();
        let (id, partition) = expect(&mut worker, |e| match e {
            Event::Produced { id, partition, .. } => Some((id, partition)),
            Event::Failed { message, .. } => panic!("produce failed: {message}"),
            _ => None,
        })
        .await;
        assert_eq!((id, partition), (i as u64, (i % 2) as i32));
    }

    // A mistyped topic is refused, not auto-created.
    let missing = unique("kaffy-test-missing");
    worker
        .commands
        .send(Command::Produce(ProduceRequest {
            id: 99,
            topic: missing.clone(),
            partition: None,
            key: None,
            value: Some(Payload::Bytes(b"x".to_vec())),
            headers: vec![],
        }))
        .unwrap();
    assert!(
        expect_failure(&mut worker, Operation::Produce)
            .await
            .contains("does not exist")
    );

    worker
        .commands
        .send(Command::PlanReplay {
            source: source.name.clone(),
            target: target.name.clone(),
            start: StartAt::Beginning,
            end: ReplayEnd::Now,
        })
        .unwrap();
    let plan = expect(&mut worker, |e| match e {
        Event::ReplayPlan(p) => Some(p),
        Event::Failed { message, .. } => panic!("plan failed: {message}"),
        _ => None,
    })
    .await;
    assert_eq!(plan.total(), 5);
    assert!(plan.keep_partitions);

    let filter =
        RecordFilter::new(|r: &Record| r.value.as_deref().is_some_and(|v| v.starts_with(b"keep")));
    worker
        .commands
        .send(Command::Replay {
            id: 7,
            plan,
            filter: Some(filter),
        })
        .unwrap();
    let done = expect(&mut worker, |e| match e {
        Event::ReplayProgress(p) if p.done => Some(p),
        _ => None,
    })
    .await;
    assert_eq!((done.scanned, done.produced, done.error), (5, 3, None));

    // The copies keep key, headers and partition.
    let copies = tail(&mut worker, &target.name, StartAt::Beginning, 3).await;
    let summary: Vec<_> = copies
        .iter()
        .map(|r| {
            (
                r.partition,
                String::from_utf8_lossy(r.key.as_deref().unwrap()).into_owned(),
                String::from_utf8_lossy(r.value.as_deref().unwrap()).into_owned(),
                String::from_utf8_lossy(r.headers[0].value.as_deref().unwrap()).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            (0, "key-0".into(), "keep-1".into(), "t0".into()),
            (0, "key-2".into(), "keep-2".into(), "t2".into()),
            (0, "key-4".into(), "keep-3".into(), "t4".into()),
        ]
    );
    // Confirm the mistyped topic really was not created.
    let admin: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .unwrap();
    let metadata = admin.fetch_metadata(None, TIMEOUT).unwrap();
    assert!(metadata.topics().iter().all(|t| t.name() != missing));
}

// ---- Phase 3: topic administration ----

use kaffy_kafka::{ConfigSource, NewTopic as TopicSpec};

/// Clock time plus a counter: macOS clocks tick in microseconds, so parallel
/// tests could otherwise pick the same name.
fn unique_suffix() -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{micros}-{n}")
}

fn config_value(
    entries: &[kaffy_kafka::ConfigEntry],
    name: &str,
) -> (Option<String>, ConfigSource) {
    let e = entries
        .iter()
        .find(|e| e.name == name)
        .expect("config exists");
    (e.value.clone(), e.source)
}

#[tokio::test]
#[ignore = "needs a Kafka broker"]
async fn topic_create_config_truncate_delete() {
    let name = unique("kaffy-test-admin");
    let mut worker = kaffy_kafka::spawn(&writable()).unwrap();

    worker
        .commands
        .send(Command::CreateTopic(TopicSpec {
            name: name.clone(),
            partitions: 2,
            replication: 1,
            configs: vec![("cleanup.policy".into(), "compact,delete".into())],
        }))
        .unwrap();
    expect(&mut worker, |e| match e {
        Event::TopicCreated { name: n } if n == name => Some(()),
        _ => None,
    })
    .await;

    // Creating it again reports the broker's reason.
    worker
        .commands
        .send(Command::CreateTopic(TopicSpec {
            name: name.clone(),
            partitions: 1,
            replication: 1,
            configs: vec![],
        }))
        .unwrap();
    assert!(
        expect_failure(&mut worker, Operation::CreateTopic)
            .await
            .contains("already exists")
    );

    let describe = async |worker: &mut Worker| {
        worker
            .commands
            .send(Command::DescribeTopicConfig(name.clone()))
            .unwrap();
        expect(worker, |e| match e {
            Event::TopicConfig { entries, .. } => Some(entries),
            _ => None,
        })
        .await
    };
    let entries = describe(&mut worker).await;
    assert_eq!(
        config_value(&entries, "cleanup.policy"),
        (Some("compact,delete".into()), ConfigSource::Topic)
    );

    // Changing one setting keeps the others (AlterConfigs replaces them all).
    worker
        .commands
        .send(Command::AlterTopicConfig {
            topic: name.clone(),
            changes: vec![("retention.ms".into(), Some("3600000".into()))],
        })
        .unwrap();
    expect(&mut worker, |e| match e {
        Event::TopicConfigAltered { changed: 1, .. } => Some(()),
        _ => None,
    })
    .await;
    let entries = describe(&mut worker).await;
    assert_eq!(
        config_value(&entries, "retention.ms"),
        (Some("3600000".into()), ConfigSource::Topic)
    );
    assert_eq!(
        config_value(&entries, "cleanup.policy").0.as_deref(),
        Some("compact,delete")
    );

    // Resetting returns it to the inherited value.
    worker
        .commands
        .send(Command::AlterTopicConfig {
            topic: name.clone(),
            changes: vec![("retention.ms".into(), None)],
        })
        .unwrap();
    expect(&mut worker, |e| match e {
        Event::TopicConfigAltered { .. } => Some(()),
        _ => None,
    })
    .await;
    assert_ne!(
        config_value(&describe(&mut worker).await, "retention.ms").1,
        ConfigSource::Topic
    );

    worker
        .commands
        .send(Command::AlterTopicConfig {
            topic: name.clone(),
            changes: vec![("no.such.setting".into(), Some("1".into()))],
        })
        .unwrap();
    assert!(
        expect_failure(&mut worker, Operation::AlterTopicConfig)
            .await
            .contains("not a topic config")
    );

    // DeleteRecords: drop the first 3 of 5 records in partition 0.
    for i in 0..5 {
        produce(&name, 0, &format!("k{i}"), b"v").await;
    }
    worker
        .commands
        .send(Command::PlanDeleteRecords {
            topic: name.clone(),
            before: StartAt::Offset(3),
        })
        .unwrap();
    let plan = expect(&mut worker, |e| match e {
        Event::DeleteRecordsPlan(p) => Some(p),
        _ => None,
    })
    .await;
    assert_eq!(
        plan.total(),
        3,
        "partition 1 is empty, so only partition 0 changes"
    );
    assert_eq!(plan.offsets(), vec![(0, 3)]);

    let mut read_only = kaffy_kafka::spawn(&Profile::adhoc(bootstrap())).unwrap();
    read_only
        .commands
        .send(Command::DeleteRecords {
            topic: name.clone(),
            offsets: plan.offsets(),
        })
        .unwrap();
    assert!(
        expect_failure(&mut read_only, Operation::DeleteRecords)
            .await
            .contains("read-only")
    );

    worker
        .commands
        .send(Command::DeleteRecords {
            topic: name.clone(),
            offsets: plan.offsets(),
        })
        .unwrap();
    let deleted = expect(&mut worker, |e| match e {
        Event::RecordsDeleted { records, .. } => Some(records),
        _ => None,
    })
    .await;
    assert_eq!(deleted, 3);
    let remaining = tail(&mut worker, &name, StartAt::Beginning, 2).await;
    assert_eq!(
        remaining.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![3, 4]
    );

    worker
        .commands
        .send(Command::DeleteTopic(name.clone()))
        .unwrap();
    expect(&mut worker, |e| match e {
        Event::TopicDeleted { name: n } if n == name => Some(()),
        _ => None,
    })
    .await;
    let admin: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .unwrap();
    let gone = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let metadata = admin.fetch_metadata(None, TIMEOUT).unwrap();
        if metadata.topics().iter().all(|t| t.name() != name) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < gone,
            "topic still listed after delete"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
