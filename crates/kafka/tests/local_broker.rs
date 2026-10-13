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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
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
