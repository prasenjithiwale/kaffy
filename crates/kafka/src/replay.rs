//! Replay: copy a range of records to the same or another topic.
//!
//! Keys, values and headers are copied as-is. Timestamps are not: the copies get
//! the produce time, so retention doesn't delete them straight away.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use kaffy_schema::Registry;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header as KafkaHeader, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::{mpsc, oneshot};

use crate::client::{decode_record, to_record};
use crate::groups::{EARLIEST, LATEST, query_offsets};
use crate::{Event, PartitionRange, RecordFilter, ReplayEnd, ReplayPlan, ReplayProgress, StartAt};

const TIMEOUT: Duration = Duration::from_secs(10);
/// Deliveries awaited at once; bounds memory on large replays.
const MAX_IN_FLIGHT: usize = 10_000;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// Resolves `start` and `end` to offsets for every partition of `source`. Blocks.
pub(crate) fn plan(
    admin: &BaseConsumer,
    source: &str,
    target: &str,
    start: StartAt,
    end: ReplayEnd,
) -> Result<ReplayPlan, String> {
    let source_partitions = partitions(admin, source)?;
    let target_partitions = partitions(admin, target)?.len();

    let keys: BTreeSet<(String, i32)> = source_partitions
        .iter()
        .map(|&p| (source.to_owned(), p))
        .collect();
    let lookup = |ts| query_offsets(admin, &keys, ts).map_err(|e| e.to_string());
    let low = lookup(EARLIEST)?;
    let high = lookup(LATEST)?;
    let start_times = match start {
        StartAt::Timestamp(ms) => Some(lookup(ms)?),
        _ => None,
    };
    let end_times = match end {
        ReplayEnd::Timestamp(ms) => Some(lookup(ms)?),
        _ => None,
    };

    let ranges = source_partitions
        .iter()
        .map(|&partition| {
            let key = (source.to_owned(), partition);
            let lo = low.get(&key).copied().unwrap_or(0).max(0);
            let hi = high.get(&key).copied().unwrap_or(lo).max(lo);
            // -1 from a time lookup means "nothing that new": the end.
            let at = |m: &Option<std::collections::BTreeMap<(String, i32), i64>>| match m
                .as_ref()
                .and_then(|m| m.get(&key))
                .copied()
            {
                Some(o) if o >= 0 => o,
                _ => hi,
            };
            let from = match start {
                StartAt::Beginning => lo,
                StartAt::End => hi,
                StartAt::LastN(n) => (hi - n).max(lo),
                StartAt::Offset(n) => n.clamp(lo, hi),
                StartAt::Timestamp(_) => at(&start_times),
            };
            let to = match end {
                ReplayEnd::Now => hi,
                ReplayEnd::Offset(n) => n.clamp(lo, hi),
                ReplayEnd::Timestamp(_) => at(&end_times),
            };
            PartitionRange {
                partition,
                from,
                to: to.max(from),
            }
        })
        .collect();

    let plan = ReplayPlan {
        source: source.to_owned(),
        target: target.to_owned(),
        ranges,
        target_partitions,
        keep_partitions: target_partitions == source_partitions.len(),
    };
    if plan.total() == 0 {
        return Err("nothing to replay: the range has no messages".to_owned());
    }
    Ok(plan)
}

fn partitions(admin: &BaseConsumer, topic: &str) -> Result<Vec<i32>, String> {
    let metadata = admin
        .fetch_metadata(Some(topic), TIMEOUT)
        .map_err(|e| format!("{topic}: {e}"))?;
    let t = metadata
        .topics()
        .first()
        .ok_or_else(|| format!("topic {topic} does not exist"))?;
    if t.error().is_some() || t.partitions().is_empty() {
        return Err(format!("topic {topic} does not exist"));
    }
    Ok(t.partitions().iter().map(|p| p.id()).collect())
}

/// Copies the plan's records, sending progress events until done, failed or cancelled.
/// Everything a replay needs to run.
pub(crate) struct Job {
    /// Consumer settings for reading the source.
    pub config: ClientConfig,
    pub producer: FutureProducer,
    pub registry: Option<Arc<Registry>>,
    pub id: u64,
    pub plan: ReplayPlan,
    pub filter: Option<RecordFilter>,
}

pub(crate) async fn run(job: Job, mut cancel: oneshot::Receiver<()>, events: mpsc::Sender<Event>) {
    let mut progress = ReplayProgress {
        id: job.id,
        scanned: 0,
        produced: 0,
        done: false,
        error: None,
    };
    let result = copy(job, &mut cancel, &mut progress, &events).await;
    progress.done = true;
    progress.error = result.err();
    let _ = events.send(Event::ReplayProgress(progress)).await;
}

async fn copy(
    job: Job,
    cancel: &mut oneshot::Receiver<()>,
    progress: &mut ReplayProgress,
    events: &mpsc::Sender<Event>,
) -> Result<(), String> {
    let Job {
        mut config,
        producer,
        registry,
        plan,
        filter,
        ..
    } = job;
    let (producer, plan) = (&producer, &plan);
    // EOF events tell us a partition has nothing more right now, which also ends
    // ranges whose last offsets were removed by compaction.
    config.set("enable.partition.eof", "true");
    let consumer: StreamConsumer = config.create().map_err(|e| e.to_string())?;
    let mut tpl = TopicPartitionList::new();
    let mut remaining: HashMap<i32, i64> = HashMap::new();
    for range in plan.ranges.iter().filter(|r| r.to > r.from) {
        tpl.add_partition_offset(&plan.source, range.partition, Offset::Offset(range.from))
            .map_err(|e| e.to_string())?;
        remaining.insert(range.partition, range.to);
    }
    consumer.assign(&tpl).map_err(|e| e.to_string())?;

    let mut deliveries = FuturesUnordered::new();
    let mut stream = consumer.stream();
    let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
    let mut cancelled = false;

    while !remaining.is_empty() {
        tokio::select! {
            _ = &mut *cancel => {
                cancelled = true;
                break;
            }
            _ = ticker.tick() => {
                let _ = events.send(Event::ReplayProgress(progress.clone())).await;
            }
            Some(delivered) = deliveries.next(), if !deliveries.is_empty() => {
                check_delivery(delivered)?;
                progress.produced += 1;
            }
            message = stream.next() => match message {
                Some(Ok(message)) => {
                    let partition = rdkafka::Message::partition(&message);
                    let offset = rdkafka::Message::offset(&message);
                    let Some(&to) = remaining.get(&partition) else { continue };
                    if offset >= to {
                        finish(&consumer, &plan.source, partition, &mut remaining)?;
                        continue;
                    }
                    let mut record = to_record(&message);
                    drop(message);
                    progress.scanned += 1;
                    if offset + 1 >= to {
                        finish(&consumer, &plan.source, partition, &mut remaining)?;
                    }
                    if let Some(filter) = &filter {
                        // The filter sees what the UI shows, so decode first.
                        if let Some(registry) = &registry {
                            decode_record(registry, &mut record).await;
                        }
                        if !filter.matches(&record) {
                            continue;
                        }
                    }
                    let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
                    for h in &record.headers {
                        headers = headers.insert(KafkaHeader { key: &h.key, value: h.value.as_deref() });
                    }
                    let mut out = FutureRecord::<[u8], [u8]>::to(&plan.target).headers(headers);
                    if let Some(key) = record.key.as_deref() {
                        out = out.key(key);
                    }
                    if let Some(value) = record.value.as_deref() {
                        out = out.payload(value);
                    }
                    if plan.keep_partitions {
                        out = out.partition(partition);
                    }
                    let delivery = enqueue(producer, out, &mut deliveries, progress).await?;
                    deliveries.push(delivery);
                }
                Some(Err(KafkaError::PartitionEOF(partition))) => {
                    finish(&consumer, &plan.source, partition, &mut remaining)?;
                }
                Some(Err(e)) => return Err(format!("reading {}: {e}", plan.source)),
                None => return Err("the source stream ended unexpectedly".to_owned()),
            }
        }
    }

    // Everything enqueued is delivered even when cancelled, so counts stay true.
    while let Some(delivered) = deliveries.next().await {
        check_delivery(delivered)?;
        progress.produced += 1;
    }
    if cancelled {
        return Err("cancelled".to_owned());
    }
    Ok(())
}

/// Enqueues a record, waiting for earlier deliveries while the queue is full.
async fn enqueue(
    producer: &FutureProducer,
    mut record: FutureRecord<'_, [u8], [u8]>,
    deliveries: &mut FuturesUnordered<rdkafka::producer::DeliveryFuture>,
    progress: &mut ReplayProgress,
) -> Result<rdkafka::producer::DeliveryFuture, String> {
    loop {
        if deliveries.len() >= MAX_IN_FLIGHT {
            if let Some(delivered) = deliveries.next().await {
                check_delivery(delivered)?;
                progress.produced += 1;
            }
        }
        match producer.send_result(record) {
            Ok(future) => return Ok(future),
            Err((
                KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull),
                r,
            )) => {
                record = r;
                match deliveries.next().await {
                    Some(delivered) => {
                        check_delivery(delivered)?;
                        progress.produced += 1;
                    }
                    None => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
            Err((e, _)) => return Err(format!("producing: {e}")),
        }
    }
}

fn check_delivery(
    delivered: Result<
        rdkafka::producer::future_producer::OwnedDeliveryResult,
        futures::channel::oneshot::Canceled,
    >,
) -> Result<(), String> {
    match delivered {
        Ok(Ok(_)) => Ok(()),
        Ok(Err((e, _))) => Err(format!("producing: {e}")),
        Err(_) => Err("producer shut down".to_owned()),
    }
}

/// Stops reading a partition once its range is copied.
fn finish(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    remaining: &mut HashMap<i32, i64>,
) -> Result<(), String> {
    if remaining.remove(&partition).is_some() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        consumer.pause(&tpl).map_err(|e| e.to_string())?;
    }
    Ok(())
}
