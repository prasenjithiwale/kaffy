//! A fake cluster for running the UI without a broker (`kaffy --demo`).
//!
//! Records are derived from (topic, partition, offset), so seeking back to the
//! same position shows the same data, like a real log.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{
    Command, EVENT_BUFFER, Event, Header, PartitionInfo, Record, StartAt, TopicInfo, Worker,
};

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
    let mut tail: Option<(mpsc::UnboundedSender<Command>, JoinHandle<()>)> = None;
    while let Some(command) = commands.recv().await {
        match command {
            Command::ListTopics => {
                let _ = events
                    .send(Event::Metadata {
                        brokers: 3,
                        topics: topics(),
                    })
                    .await;
            }
            Command::Tail {
                session,
                topic,
                start,
            } => {
                if let Some((_, task)) = tail.take() {
                    task.abort();
                }
                let Some(topic_idx) = TOPICS.iter().position(|t| *t == topic) else {
                    let _ = events
                        .send(Event::Error(format!("tail {topic}: unknown topic")))
                        .await;
                    continue;
                };
                let (control, control_rx) = mpsc::unbounded_channel();
                let task = tokio::spawn(run_tail(
                    topic_idx,
                    session,
                    start,
                    started_ms,
                    control_rx,
                    events.clone(),
                ));
                tail = Some((control, task));
            }
            Command::StopTail => {
                if let Some((_, task)) = tail.take() {
                    task.abort();
                }
            }
            Command::Pause | Command::Resume => {
                if let Some((control, _)) = &tail {
                    let _ = control.send(command);
                }
            }
        }
    }
}

fn topics() -> Vec<TopicInfo> {
    TOPICS
        .iter()
        .enumerate()
        .map(|(i, name)| TopicInfo {
            name: (*name).to_owned(),
            partitions: (0..PARTITIONS)
                .map(|id| PartitionInfo {
                    id,
                    leader: id % 3 + 1,
                    replicas: vec![1, 2, 3],
                    // One under-replicated partition, so the column has something to show.
                    isr: if i == 3 && id == 1 {
                        vec![1, 3]
                    } else {
                        vec![1, 2, 3]
                    },
                })
                .collect(),
        })
        .collect()
}

async fn run_tail(
    topic_idx: usize,
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
            let ts = started_ms - (HISTORY - offset) * HISTORY_STEP_MS;
            let record = record(topic_idx, partition, offset, ts);
            if events
                .send(Event::Record { session, record })
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
                if events.send(Event::Record { session, record }).await.is_err() {
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
            // Confluent wire format: magic byte, schema id 7, then an opaque body.
            let mut value = vec![0, 0, 0, 0, 7];
            value.extend_from_slice(&h.to_be_bytes());
            value.extend_from_slice(&(offset as u32).to_be_bytes());
            (
                Some(format!("sensor-{}", h % 16).into_bytes()),
                Some(value),
                Vec::new(),
            )
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
