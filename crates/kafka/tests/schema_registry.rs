//! End-to-end tests against Kafka plus a real Confluent Schema Registry.
//! Ignored by default:
//!
//! ```sh
//! docker compose -f dev/docker-compose.yml up -d --wait
//! cargo test -p kaffy-kafka --test schema_registry -- --ignored
//! ```
//!
//! `KAFFY_TEST_SR_BOOTSTRAP` (default localhost:29092) and `KAFFY_TEST_SR`
//! (default http://localhost:8081) point at other servers.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kaffy_config::{Profile, ProtobufTopic, SchemaRegistry};
use kaffy_kafka::{Command, Event, Payload, ProduceRequest, Record, SchemaType, StartAt, Worker};
use kaffy_schema::Registry;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use serde_json::json;

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

const TIMEOUT: Duration = Duration::from_secs(20);

fn bootstrap() -> String {
    std::env::var("KAFFY_TEST_SR_BOOTSTRAP").unwrap_or_else(|_| "localhost:29092".to_owned())
}

fn registry_url() -> String {
    std::env::var("KAFFY_TEST_SR").unwrap_or_else(|_| "http://localhost:8081".to_owned())
}

fn unique(prefix: &str) -> String {
    let nanos = unique_suffix();
    format!("{prefix}-{nanos}")
}

fn admin() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .unwrap()
}

async fn create_topic(name: &str) {
    for r in admin()
        .create_topics(
            &[NewTopic::new(name, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap()
    {
        r.unwrap();
    }
}

async fn delete_topic(name: &str) {
    let _ = admin().delete_topics(&[name], &AdminOptions::new()).await;
}

async fn delete_subject(subject: &str) {
    let client = reqwest::Client::new();
    let base = format!("{}/subjects/{subject}", registry_url());
    let _ = client.delete(&base).send().await;
    let _ = client.delete(format!("{base}?permanent=true")).send().await;
}

async fn produce(worker: &mut Worker, topic: &str, payload: Payload) {
    let request = ProduceRequest {
        id: 1,
        topic: topic.to_owned(),
        partition: None,
        key: Some(b"k1".to_vec()),
        value: Some(payload),
        headers: vec![],
    };
    worker.commands.send(Command::Produce(request)).unwrap();
    loop {
        match tokio::time::timeout(TIMEOUT, worker.events.recv())
            .await
            .expect("produce answer")
        {
            Some(Event::Produced { .. }) => return,
            Some(Event::Failed { message, .. }) => panic!("produce failed: {message}"),
            Some(_) => {}
            None => panic!("worker stopped"),
        }
    }
}

async fn first_record(worker: &mut Worker, topic: &str) -> Record {
    worker
        .commands
        .send(Command::Tail {
            session: 1,
            topic: topic.to_owned(),
            start: StartAt::Beginning,
            page: None,
        })
        .unwrap();
    loop {
        match tokio::time::timeout(TIMEOUT, worker.events.recv())
            .await
            .expect("a record")
        {
            Some(Event::Record { record, .. }) => return *record,
            Some(Event::Error(e)) => panic!("{e}"),
            Some(_) => {}
            None => panic!("worker stopped"),
        }
    }
}

const ORDERS_PROTO: &str = r#"syntax = "proto3";
package acme;
import "google/protobuf/timestamp.proto";

message Order {
  string order_id = 1;
  repeated Item items = 2;
  google.protobuf.Timestamp placed_at = 3;
  message Item {
    string sku = 1;
    int32 qty = 2;
  }
}
"#;

#[tokio::test]
#[ignore = "needs Kafka and a Schema Registry"]
async fn protobuf_through_the_registry() {
    let topic = unique("kaffy-test-proto");
    let subject = format!("{topic}-value");
    create_topic(&topic).await;
    let registry = Registry::new(&registry_url(), None).unwrap();
    let (schema_id, _) = registry
        .register(&subject, SchemaType::Protobuf, ORDERS_PROTO)
        .await
        .unwrap();

    let mut profile = Profile::adhoc(bootstrap());
    profile.read_only = false;
    profile.schema_registry = Some(SchemaRegistry {
        url: registry_url(),
        username: None,
        password_env: None,
    });
    let mut worker = kaffy_kafka::spawn(&profile).unwrap();

    // A nested message, so the message-index path is [0, 0], not the [0] shortcut.
    let item = json!({"sku": "SKU-1", "qty": 3});
    produce(
        &mut worker,
        &topic,
        Payload::Protobuf {
            schema_id: Some(schema_id),
            message: "acme.Order.Item".into(),
            json: item.clone(),
        },
    )
    .await;
    let record = first_record(&mut worker, &topic).await;
    let decoded = record.value_decoded.expect("decoded");
    assert_eq!(decoded.schema_id, schema_id);
    assert_eq!(decoded.message.as_deref(), Some("acme.Order.Item"));
    assert_eq!(decoded.result.unwrap(), item);

    delete_topic(&topic).await;
    delete_subject(&subject).await;
}

#[tokio::test]
#[ignore = "needs Kafka"]
async fn protobuf_with_local_proto_files() {
    let topic = unique("kaffy-test-proto-local");
    create_topic(&topic).await;
    let dir = std::env::temp_dir().join(unique("kaffy-proto"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("orders.proto"), ORDERS_PROTO).unwrap();

    let mut profile = Profile::adhoc(bootstrap());
    profile.read_only = false;
    profile.protobuf = vec![ProtobufTopic {
        topic: topic.clone(),
        files: vec![dir.join("orders.proto")],
        include: vec![],
        value: Some("acme.Order".into()),
        key: None,
    }];
    let mut worker = kaffy_kafka::spawn(&profile).unwrap();

    let order = json!({
        "order_id": "o-7",
        "items": [{"sku": "A", "qty": 1}],
        "placed_at": "2026-10-13T09:30:00Z"
    });
    produce(
        &mut worker,
        &topic,
        Payload::Protobuf {
            schema_id: None,
            message: "acme.Order".into(),
            json: order.clone(),
        },
    )
    .await;
    let record = first_record(&mut worker, &topic).await;
    assert_ne!(
        record.value.as_deref().unwrap()[0],
        0,
        "plain Protobuf, no registry header"
    );
    let decoded = record.value_decoded.expect("decoded with the local files");
    assert_eq!(
        (decoded.schema_id, decoded.message.as_deref()),
        (0, Some("acme.Order"))
    );
    assert_eq!(decoded.result.unwrap(), order);

    // A broken mapping is reported at startup, not while browsing.
    let mut broken = Profile::adhoc(bootstrap());
    broken.protobuf = vec![ProtobufTopic {
        topic: topic.clone(),
        files: vec![dir.join("orders.proto")],
        include: vec![],
        value: Some("acme.Nope".into()),
        key: None,
    }];
    let error = kaffy_kafka::spawn(&broken)
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("acme.Nope"), "{error}");

    delete_topic(&topic).await;
    std::fs::remove_dir_all(&dir).unwrap();
}
