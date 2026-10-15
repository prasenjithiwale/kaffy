//! A fake cluster for running the UI without a broker (`kaffy --demo`).
//!
//! Records are derived from (topic, partition, offset), so seeking back to the
//! same position shows the same data, like a real log.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::sync::OnceLock;

use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use kaffy_schema::{AvroSchema, Decoded};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{
    BrokerInfo, ClusterInfo, Command, EVENT_BUFFER, Event, Header, Operation, Record, StartAt,
    Worker,
};

mod ops;

const PARTITIONS: i32 = 3;
/// Messages already in each partition when the demo starts.
const HISTORY: i64 = 400;
/// Spacing of historical timestamps.
const HISTORY_STEP_MS: i64 = 1_500;
const LIVE_INTERVAL: Duration = Duration::from_millis(120);

const TOPICS: [&str; 5] = [
    "__consumer_offsets",
    "orders",
    "payments",
    "sensor-readings",
    "user-events",
];

pub fn spawn_demo() -> Worker {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::channel(EVENT_BUFFER);
    tokio::spawn(run(command_rx, event_tx));
    Worker {
        commands: command_tx,
        events: event_rx,
    }
}

async fn run(mut commands: mpsc::UnboundedReceiver<Command>, events: mpsc::Sender<Event>) {
    let started_ms = now_ms();
    let cluster = ops::Cluster::new(started_ms);
    let mut tail: Option<(mpsc::UnboundedSender<Command>, JoinHandle<()>)> = None;
    // Session and topic of the running tail, to show produced records in it.
    let mut tailing: Option<(u64, String)> = None;
    let mut replay: Option<(tokio::sync::oneshot::Sender<()>, JoinHandle<()>)> = None;
    let failed = |operation, message| Event::Failed { operation, message };
    while let Some(command) = commands.recv().await {
        let lock = || cluster.lock().expect("demo cluster");
        match command {
            Command::ListTopics => {
                let topics = lock().topic_infos();
                let _ = events.send(Event::Metadata { brokers: 3, topics }).await;
            }
            Command::DescribeCluster => {
                let topics = lock().topic_infos();
                let brokers = (1..=3)
                    .map(|id| BrokerInfo {
                        id,
                        host: format!("broker-{id}.demo"),
                        port: 9092,
                    })
                    .collect();
                let cluster = ClusterInfo {
                    cluster_id: Some("kaffy-demo-cluster".to_owned()),
                    controller: Some(1),
                    brokers,
                    topics,
                };
                let _ = events.send(Event::Cluster(cluster)).await;
            }
            // Demo partitions hold fewer records than a page, so paging never kicks in.
            Command::Tail {
                session,
                topic,
                start,
                page: _,
            } => {
                if let Some((_, task)) = tail.take() {
                    task.abort();
                }
                let partitions = lock().partitions_of(&topic);
                let Some(partitions) = partitions else {
                    let _ = events
                        .send(Event::Error(format!("tail {topic}: unknown topic")))
                        .await;
                    continue;
                };
                // Topics created in the demo exist but have no generated records.
                let Some(topic_idx) = TOPICS.iter().position(|t| *t == topic) else {
                    tailing = Some((session, topic.clone()));
                    let _ = events
                        .send(Event::TailStarted {
                            session,
                            partitions: partitions as usize,
                        })
                        .await;
                    continue;
                };
                let lows: Vec<i64> = {
                    let cluster = lock();
                    (0..PARTITIONS).map(|p| cluster.low(&topic, p)).collect()
                };
                tailing = Some((session, topic.clone()));
                let (control, control_rx) = mpsc::unbounded_channel();
                let task = tokio::spawn(run_tail(
                    topic_idx,
                    lows,
                    session,
                    start,
                    started_ms,
                    control_rx,
                    events.clone(),
                ));
                tail = Some((control, task));
            }
            Command::StopTail => {
                tailing = None;
                if let Some((_, task)) = tail.take() {
                    task.abort();
                }
            }
            Command::NextPage => {}
            Command::Pause | Command::Resume => {
                if let Some((control, _)) = &tail {
                    let _ = control.send(command);
                }
            }
            Command::ListGroups => {
                let groups = lock().list();
                let _ = events.send(Event::Groups(groups)).await;
            }
            Command::DescribeGroup(name) => {
                let event = match lock().describe(&name) {
                    Ok(detail) => Event::GroupDetail(detail),
                    Err(e) => failed(Operation::DescribeGroup, e),
                };
                let _ = events.send(event).await;
            }
            Command::PlanReset {
                group,
                topic,
                target,
            } => {
                let event = match lock().plan_reset(&group, topic.as_deref(), target) {
                    Ok(plan) => Event::ResetPlan(plan),
                    Err(e) => failed(Operation::PlanReset, e),
                };
                let _ = events.send(event).await;
            }
            Command::ApplyReset { group, offsets } => {
                let event = match lock().apply_reset(&group, &offsets) {
                    Ok(partitions) => Event::ResetApplied { group, partitions },
                    Err(e) => failed(Operation::ApplyReset, e),
                };
                let _ = events.send(event).await;
            }
            Command::Produce(request) => {
                let result = lock().produce(&request);
                match result {
                    Ok(record) => {
                        let produced = Event::Produced {
                            id: request.id,
                            topic: record.topic.clone(),
                            partition: record.partition,
                            offset: record.offset,
                        };
                        let _ = events.send(produced).await;
                        if let Some((session, topic)) = &tailing {
                            if *topic == record.topic {
                                let shown = Event::Record {
                                    session: *session,
                                    record: Box::new(record),
                                };
                                let _ = events.send(shown).await;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = events.send(failed(Operation::Produce, e)).await;
                    }
                }
            }
            Command::PlanReplay {
                source,
                target,
                start,
                end,
            } => {
                let event = match lock().plan_replay(&source, &target, start, end) {
                    Ok(plan) => Event::ReplayPlan(plan),
                    Err(e) => failed(Operation::PlanReplay, e),
                };
                let _ = events.send(event).await;
            }
            Command::Replay { id, plan, filter } => {
                if replay.as_ref().is_some_and(|(_, task)| !task.is_finished()) {
                    let event = failed(Operation::Replay, "a replay is already running".to_owned());
                    let _ = events.send(event).await;
                    continue;
                }
                let (cancel, cancel_rx) = tokio::sync::oneshot::channel();
                let task = tokio::spawn(ops::replay(
                    cluster.clone(),
                    id,
                    plan,
                    filter,
                    cancel_rx,
                    events.clone(),
                ));
                replay = Some((cancel, task));
            }
            Command::CancelReplay => {
                if let Some((cancel, _)) = replay.take() {
                    let _ = cancel.send(());
                }
            }
            Command::CreateTopic(spec) => {
                let event = match lock().create_topic(&spec) {
                    Ok(()) => Event::TopicCreated { name: spec.name },
                    Err(e) => failed(Operation::CreateTopic, e),
                };
                let _ = events.send(event).await;
            }
            Command::DeleteTopic(name) => {
                let event = match lock().delete_topic(&name) {
                    Ok(()) => Event::TopicDeleted { name },
                    Err(e) => failed(Operation::DeleteTopic, e),
                };
                let _ = events.send(event).await;
            }
            Command::DescribeTopicConfig(topic) => {
                let event = match lock().describe_config(&topic) {
                    Ok(entries) => Event::TopicConfig { topic, entries },
                    Err(e) => failed(Operation::TopicConfig, e),
                };
                let _ = events.send(event).await;
            }
            Command::AlterTopicConfig { topic, changes } => {
                let event = match lock().alter_config(&topic, &changes) {
                    Ok(changed) => Event::TopicConfigAltered { topic, changed },
                    Err(e) => failed(Operation::AlterTopicConfig, e),
                };
                let _ = events.send(event).await;
            }
            Command::PlanDeleteRecords { topic, before } => {
                let event = match lock().plan_delete_records(&topic, before) {
                    Ok(plan) => Event::DeleteRecordsPlan(plan),
                    Err(e) => failed(Operation::PlanDeleteRecords, e),
                };
                let _ = events.send(event).await;
            }
            Command::DeleteRecords { topic, offsets } => {
                let event = match lock().delete_records(&topic, &offsets) {
                    Ok(records) => Event::RecordsDeleted { topic, records },
                    Err(e) => failed(Operation::DeleteRecords, e),
                };
                let _ = events.send(event).await;
            }
            Command::ListSubjects => {
                let subjects = lock().latest_schemas();
                let _ = events.send(Event::Subjects(subjects)).await;
            }
            Command::DescribeSubject(subject) => {
                let event = match lock().describe_subject(&subject) {
                    Ok((versions, compatibility)) => Event::SubjectDetail {
                        subject,
                        versions,
                        compatibility,
                    },
                    Err(e) => failed(Operation::DescribeSubject, e),
                };
                let _ = events.send(event).await;
            }
            Command::FetchSchema { subject, version } => {
                let event = match lock().fetch_schema(&subject, version) {
                    Ok(schema) => Event::SchemaVersion(schema),
                    Err(e) => failed(Operation::FetchSchema, e),
                };
                let _ = events.send(event).await;
            }
            Command::CheckSchema {
                subject,
                schema_type,
                schema,
            } => {
                let compatibility = lock().check_schema(&subject, schema_type, &schema);
                let event = Event::SchemaChecked {
                    subject,
                    schema_type,
                    schema,
                    compatibility,
                };
                let _ = events.send(event).await;
            }
            Command::RegisterSchema {
                subject,
                schema_type,
                schema,
            } => {
                let event = match lock().register_schema(&subject, schema_type, &schema) {
                    Ok((id, version)) => Event::SchemaRegistered {
                        subject,
                        id,
                        version,
                    },
                    Err(e) => failed(Operation::RegisterSchema, e),
                };
                let _ = events.send(event).await;
            }
        }
    }
}

async fn run_tail(
    topic_idx: usize,
    lows: Vec<i64>,
    session: u64,
    start: StartAt,
    started_ms: i64,
    mut control: mpsc::UnboundedReceiver<Command>,
    events: mpsc::Sender<Event>,
) {
    if events
        .send(Event::TailStarted {
            session,
            partitions: PARTITIONS as usize,
        })
        .await
        .is_err()
    {
        return;
    }

    // Historical records, interleaved across partitions in timestamp order.
    let first = match start {
        StartAt::Beginning => 0,
        StartAt::End => HISTORY,
        StartAt::LastN(n) => (HISTORY - n).max(0),
        StartAt::Offset(n) => n.clamp(0, HISTORY),
        StartAt::Timestamp(ms) => {
            let first_ms = started_ms - HISTORY * HISTORY_STEP_MS;
            ((ms - first_ms) / HISTORY_STEP_MS).clamp(0, HISTORY)
        }
    };
    for offset in first..HISTORY {
        for partition in 0..PARTITIONS {
            if offset < lows[partition as usize] {
                continue; // deleted by DeleteRecords
            }
            let ts = started_ms - (HISTORY - offset) * HISTORY_STEP_MS;
            let record = record(topic_idx, partition, offset, ts);
            if events
                .send(Event::Record {
                    session,
                    record: Box::new(record),
                })
                .await
                .is_err()
            {
                return;
            }
        }
    }

    let mut next_offset = [HISTORY; PARTITIONS as usize];
    let mut paused = false;
    let mut tick = tokio::time::interval(LIVE_INTERVAL);
    let mut seq: u64 = 0;
    loop {
        tokio::select! {
            _ = tick.tick(), if !paused => {
                seq += 1;
                let partition = (mix(seq ^ topic_idx as u64) % PARTITIONS as u64) as i32;
                let offset = next_offset[partition as usize];
                next_offset[partition as usize] += 1;
                let record = record(topic_idx, partition, offset, now_ms());
                if events.send(Event::Record { session, record: Box::new(record) }).await.is_err() {
                    return;
                }
            }
            command = control.recv() => match command {
                Some(Command::Pause) => paused = true,
                Some(Command::Resume) => paused = false,
                Some(_) => {}
                None => return,
            },
        }
    }
}

fn record(topic_idx: usize, partition: i32, offset: i64, timestamp_ms: i64) -> Record {
    let h = mix(((topic_idx as u64) << 48) ^ ((partition as u64) << 40) ^ offset as u64);
    let pick =
        |items: &[&'static str], salt: u64| items[(mix(h ^ salt) % items.len() as u64) as usize];
    let topic = TOPICS[topic_idx];
    let customer = format!("cust-{:03}", h % 250);

    let (key, value, headers): (Option<Vec<u8>>, Option<Vec<u8>>, Vec<Header>) = match topic {
        "orders" => {
            let status = pick(&["CREATED", "PAID", "SHIPPED", "CANCELLED"], 1);
            let value = format!(
                r#"{{"order_id":"ord-{partition}-{offset:06}","customer":"{customer}","status":"{status}","amount":{}.{:02},"currency":"{}","items":[{{"sku":"SKU-{}","qty":{}}}]}}"#,
                h % 500,
                h % 100,
                pick(&["EUR", "USD", "INR"], 2),
                h % 9000 + 1000,
                h % 4 + 1,
            );
            // Every 50th order is a tombstone, as on a compacted topic.
            let value = (offset % 50 != 49).then(|| value.into_bytes());
            (Some(customer.into_bytes()), value, trace_headers(h))
        }
        "payments" => {
            let value = format!(
                r#"{{"payment_id":"pay-{h:016x}","customer":"{customer}","method":"{}","approved":{}}}"#,
                pick(&["card", "upi", "sepa", "wallet"], 3),
                h % 10 != 0,
            );
            (
                Some(customer.into_bytes()),
                Some(value.into_bytes()),
                trace_headers(h),
            )
        }
        "user-events" => {
            let value = format!(
                "user {customer} {} on {}",
                pick(&["clicked", "viewed", "searched", "logged in"], 4),
                pick(&["/home", "/cart", "/checkout", "/search?q=kafka"], 5),
            );
            (
                Some(customer.into_bytes()),
                Some(value.into_bytes()),
                Vec::new(),
            )
        }
        "sensor-readings" => {
            let sensor = format!("sensor-{}", h % 16);
            let value = sensor_reading(&sensor, h, offset);
            return Record {
                topic: topic.to_owned(),
                partition,
                offset,
                timestamp_ms: Some(timestamp_ms),
                key: Some(sensor.into_bytes()),
                value_decoded: Some(decode_sensor_reading(&value)),
                value: Some(value),
                headers: Vec::new(),
                key_decoded: None,
            };
        }
        _ => (
            Some(h.to_be_bytes().to_vec()),
            Some(mix(h).to_le_bytes().repeat(3)),
            Vec::new(),
        ),
    };

    Record {
        topic: topic.to_owned(),
        partition,
        offset,
        timestamp_ms: Some(timestamp_ms),
        key,
        value,
        headers,
        key_decoded: None,
        value_decoded: None,
    }
}

/// Avro schema of `sensor-readings`, registered as id 7 in the pretend registry.
const SENSOR_SCHEMA: &str = r#"{
    "type": "record", "name": "Reading", "namespace": "demo.sensors",
    "fields": [
        {"name": "sensor", "type": "string"},
        {"name": "celsius", "type": "double"},
        {"name": "battery", "type": ["null", "int"], "default": null},
        {"name": "state", "type": {"type": "enum", "name": "State", "symbols": ["OK", "WARN", "FAIL"]}}
    ]
}"#;
const SENSOR_SCHEMA_ID: u32 = 7;
/// Every 40th reading claims a schema id the pretend registry doesn't know.
const UNKNOWN_SCHEMA_ID: u32 = 8;

fn sensor_schema() -> &'static AvroSchema {
    static SCHEMA: OnceLock<AvroSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| AvroSchema::parse_str(SENSOR_SCHEMA).expect("valid demo schema"))
}

/// An Avro-encoded reading in the Confluent wire format.
fn sensor_reading(sensor: &str, h: u64, offset: i64) -> Vec<u8> {
    // Mostly OK, some WARN, the occasional FAIL.
    let state: u32 = match h % 20 {
        0 => 2,
        1..=3 => 1,
        _ => 0,
    };
    let battery = if h % 7 == 0 {
        AvroValue::Union(0, Box::new(AvroValue::Null))
    } else {
        AvroValue::Union(1, Box::new(AvroValue::Int((h % 100) as i32)))
    };
    let reading = AvroValue::Record(vec![
        ("sensor".into(), AvroValue::String(sensor.to_owned())),
        (
            "celsius".into(),
            AvroValue::Double(15.0 + (h % 200) as f64 / 10.0),
        ),
        ("battery".into(), battery),
        (
            "state".into(),
            AvroValue::Enum(state, ["OK", "WARN", "FAIL"][state as usize].into()),
        ),
    ]);
    let body = GenericDatumWriter::builder(sensor_schema())
        .build()
        .and_then(|writer| writer.write_value_to_vec(reading))
        .expect("demo reading matches its schema");
    let schema_id = if offset % 40 == 39 {
        UNKNOWN_SCHEMA_ID
    } else {
        SENSOR_SCHEMA_ID
    };
    let mut bytes = vec![0];
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes
}

/// What a registry holding only schema 7 would answer.
fn decode_sensor_reading(bytes: &[u8]) -> Decoded {
    let decoded = kaffy_schema::decode_avro(sensor_schema(), bytes).expect("wire format");
    if decoded.schema_id == SENSOR_SCHEMA_ID {
        return decoded;
    }
    Decoded {
        schema_id: decoded.schema_id,
        schema_type: None,
        message: None,
        result: Err(format!(
            "schema registry: 404 Not Found: Schema {} not found",
            decoded.schema_id
        )),
    }
}

fn trace_headers(h: u64) -> Vec<Header> {
    vec![
        Header {
            key: "trace-id".to_owned(),
            value: Some(format!("{:032x}", u128::from(h) << 64 | u128::from(mix(h))).into_bytes()),
        },
        Header {
            key: "source".to_owned(),
            value: Some(b"demo-service".to_vec()),
        },
    ]
}

/// splitmix64 finalizer: cheap, deterministic pseudo-randomness.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_deterministic() {
        assert_eq!(record(1, 2, 10, 0), record(1, 2, 10, 0));
        assert_ne!(record(1, 2, 10, 0).value, record(1, 2, 11, 0).value);
    }

    #[tokio::test]
    async fn last_n_replays_history_per_partition() {
        let mut worker = spawn_demo();
        worker
            .commands
            .send(Command::Tail {
                session: 7,
                topic: "orders".to_owned(),
                start: StartAt::LastN(2),
                page: None,
            })
            .unwrap();
        let Some(Event::TailStarted {
            session: 7,
            partitions,
        }) = worker.events.recv().await
        else {
            panic!("expected TailStarted");
        };
        assert_eq!(partitions, PARTITIONS as usize);
        for _ in 0..(2 * PARTITIONS) {
            let Some(Event::Record { record, .. }) = worker.events.recv().await else {
                panic!("expected a record");
            };
            assert!(record.offset >= HISTORY - 2);
        }
    }
}
