//! The demo cluster's consumer groups, produce and replay.
//!
//! End offsets grow with time, as if producers were running. Two groups keep up
//! with small, varying lag; one has periodic spikes; one is stopped (Empty) and
//! falls further behind, so it can be reset.

use std::collections::{BTreeMap, HashMap};
use std::f64::consts::TAU;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::{HISTORY, HISTORY_STEP_MS, PARTITIONS, TOPICS, mix, now_ms, record, sensor_schema};
use crate::{
    Event, GroupDetail, GroupMember, GroupState, GroupSummary, PartitionLag, PartitionOffset,
    PartitionRange, Payload, ProduceRequest, Record, RecordFilter, ReplayEnd, ReplayPlan,
    ReplayProgress, ResetPlan, ResetRow, ResetTarget, StartAt,
};

/// Each partition gains a message about every 360 ms (3 partitions at 120 ms).
const LIVE_STEP_MS: i64 = 360;
/// Replay speed, so progress is visible.
const REPLAY_RATE_PER_SEC: u64 = 4_000;

#[derive(Clone, Copy)]
enum Lag {
    /// Lag rises and falls between 0 and `amplitude` every `period_s`.
    Wave { amplitude: f64, period_s: f64 },
    /// Mostly caught up, with a backlog every `every_s` that drains.
    Spikes { height: f64, every_s: f64 },
    /// Not consuming: committed offsets stay put.
    Stopped,
}

struct Group {
    name: &'static str,
    state: GroupState,
    hosts: &'static [&'static str],
    topics: &'static [&'static str],
    lag: Lag,
    /// Committed offsets of a stopped group.
    committed: HashMap<(String, i32), i64>,
}

pub(super) struct Cluster {
    started_ms: i64,
    groups: Vec<Group>,
    /// Records produced or replayed into each partition.
    added: HashMap<(String, i32), i64>,
}

pub(super) type Shared = Arc<Mutex<Cluster>>;

impl Cluster {
    pub(super) fn new(started_ms: i64) -> Shared {
        let stopped: HashMap<_, _> = ["orders", "user-events"]
            .iter()
            .flat_map(|t| {
                (0..PARTITIONS).map(move |p| ((t.to_string(), p), HISTORY - 260 + p as i64 * 15))
            })
            .collect();
        let group = |name, state, hosts, topics, lag| Group {
            name,
            state,
            hosts,
            topics,
            lag,
            committed: HashMap::new(),
        };
        Arc::new(Mutex::new(Self {
            started_ms,
            groups: vec![
                Group {
                    committed: stopped,
                    ..group(
                        "analytics-backfill",
                        GroupState::Empty,
                        &[],
                        &["orders", "user-events"],
                        Lag::Stopped,
                    )
                },
                group(
                    "orders-service",
                    GroupState::Stable,
                    &["orders-7f9c-1", "orders-7f9c-2", "orders-7f9c-3"],
                    &["orders"],
                    Lag::Wave {
                        amplitude: 40.0,
                        period_s: 30.0,
                    },
                ),
                group(
                    "payment-processor",
                    GroupState::Stable,
                    &["payments-0", "payments-1"],
                    &["payments"],
                    Lag::Wave {
                        amplitude: 12.0,
                        period_s: 18.0,
                    },
                ),
                group(
                    "sensor-alerts",
                    GroupState::Stable,
                    &["alerts-a"],
                    &["sensor-readings"],
                    Lag::Spikes {
                        height: 900.0,
                        every_s: 45.0,
                    },
                ),
            ],
            added: HashMap::new(),
        }))
    }

    fn elapsed_ms(&self) -> i64 {
        now_ms() - self.started_ms
    }

    fn end(&self, topic: &str, partition: i32) -> i64 {
        HISTORY
            + self.elapsed_ms() / LIVE_STEP_MS
            + self
                .added
                .get(&(topic.to_owned(), partition))
                .copied()
                .unwrap_or(0)
    }

    fn committed(&self, group: &Group, topic: &str, partition: i32) -> i64 {
        let end = self.end(topic, partition);
        let t = self.elapsed_ms() as f64 / 1000.0 + partition as f64 * 4.0;
        let lag = match group.lag {
            Lag::Wave {
                amplitude,
                period_s,
            } => amplitude * (0.5 - 0.5 * (TAU * t / period_s).cos()),
            Lag::Spikes { height, every_s } => {
                // Backlog appears at the start of each cycle and drains over it.
                let phase = (t % every_s) / every_s;
                height * (1.0 - phase).powi(3)
            }
            Lag::Stopped => {
                let key = (topic.to_owned(), partition);
                return group.committed.get(&key).copied().unwrap_or(0).min(end);
            }
        };
        (end - lag.round() as i64).max(0)
    }

    fn group(&self, name: &str) -> Result<&Group, String> {
        self.groups
            .iter()
            .find(|g| g.name == name)
            .ok_or_else(|| format!("group {name} does not exist"))
    }

    fn lags(&self, group: &Group) -> Vec<PartitionLag> {
        group
            .topics
            .iter()
            .flat_map(|topic| {
                (0..PARTITIONS).map(move |partition| PartitionLag {
                    topic: topic.to_string(),
                    partition,
                    committed: self.committed(group, topic, partition),
                    end: self.end(topic, partition),
                })
            })
            .collect()
    }

    pub(super) fn list(&self) -> Vec<GroupSummary> {
        self.groups
            .iter()
            .map(|g| GroupSummary {
                name: g.name.to_owned(),
                state: g.state.clone(),
                members: g.hosts.len(),
                topics: g.topics.iter().map(|t| t.to_string()).collect(),
                lag: Some(self.lags(g).iter().map(PartitionLag::lag).sum()),
            })
            .collect()
    }

    pub(super) fn describe(&self, name: &str) -> Result<GroupDetail, String> {
        let group = self.group(name)?;
        Ok(GroupDetail {
            name: group.name.to_owned(),
            state: group.state.clone(),
            members: group
                .hosts
                .iter()
                .map(|h| GroupMember {
                    client_id: format!("{}-client", group.name),
                    host: format!("{h}.demo.internal"),
                })
                .collect(),
            partitions: self.lags(group),
        })
    }

    pub(super) fn plan_reset(
        &self,
        name: &str,
        topic: Option<&str>,
        target: ResetTarget,
    ) -> Result<ResetPlan, String> {
        let group = self.group(name)?;
        let topics: Vec<&str> = match topic {
            Some(t) if TOPICS.contains(&t) => vec![t],
            Some(t) => return Err(format!("topic {t} does not exist")),
            None => group.topics.to_vec(),
        };
        let rows = topics
            .iter()
            .flat_map(|topic| {
                (0..PARTITIONS).map(move |partition| {
                    let end = self.end(topic, partition);
                    let new = match target {
                        ResetTarget::Earliest => 0,
                        ResetTarget::Latest => end,
                        ResetTarget::Timestamp(ms) => offset_at(self.started_ms, ms).min(end),
                        ResetTarget::Offset(n) => n.clamp(0, end),
                    };
                    let current = group
                        .topics
                        .contains(topic)
                        .then(|| self.committed(group, topic, partition));
                    ResetRow {
                        topic: topic.to_string(),
                        partition,
                        current,
                        new,
                        end,
                    }
                })
            })
            .collect();
        Ok(ResetPlan {
            group: name.to_owned(),
            state: group.state.clone(),
            target,
            rows,
        })
    }

    pub(super) fn apply_reset(
        &mut self,
        name: &str,
        offsets: &[PartitionOffset],
    ) -> Result<usize, String> {
        let group = self
            .groups
            .iter_mut()
            .find(|g| g.name == name)
            .ok_or_else(|| format!("group {name} does not exist"))?;
        if group.state != GroupState::Empty {
            return Err(format!(
                "group {name} is {} with {} active member(s); stop its consumers and try again",
                group.state.label(),
                group.hosts.len()
            ));
        }
        for o in offsets {
            group
                .committed
                .insert((o.topic.clone(), o.partition), o.offset);
        }
        Ok(offsets.len())
    }

    /// Returns the stored record, for showing in a running tail.
    pub(super) fn produce(&mut self, request: &ProduceRequest) -> Result<Record, String> {
        let Some(topic_idx) = TOPICS.iter().position(|t| *t == request.topic) else {
            return Err(format!("topic {} does not exist", request.topic));
        };
        let partition = match request.partition {
            Some(p) if (0..PARTITIONS).contains(&p) => p,
            Some(p) => {
                return Err(format!(
                    "topic {} has partitions 0 to {}; there is no partition {p}",
                    request.topic,
                    PARTITIONS - 1
                ));
            }
            None => request.key.as_deref().map_or(0, |k| {
                (mix(k.iter().map(|&b| b as u64).sum()) % PARTITIONS as u64) as i32
            }),
        };
        let (value, value_decoded) = match &request.value {
            None => (None, None),
            Some(Payload::Bytes(bytes)) => (Some(bytes.clone()), None),
            Some(Payload::Avro { schema_id: 7, json }) => {
                let bytes = kaffy_schema::encode_avro(sensor_schema(), 7, json)?;
                let decoded = kaffy_schema::decode_avro(sensor_schema(), &bytes);
                (Some(bytes), decoded)
            }
            Some(Payload::Avro { schema_id, .. }) => {
                return Err(format!(
                    "schema registry: 404 Not Found: Schema {schema_id} not found"
                ));
            }
        };
        let offset = self.end(&request.topic, partition);
        *self
            .added
            .entry((request.topic.clone(), partition))
            .or_default() += 1;
        let _ = topic_idx;
        Ok(Record {
            topic: request.topic.clone(),
            partition,
            offset,
            timestamp_ms: Some(now_ms()),
            key: request.key.clone(),
            value,
            headers: request.headers.clone(),
            key_decoded: None,
            value_decoded,
        })
    }

    pub(super) fn plan_replay(
        &self,
        source: &str,
        target: &str,
        start: StartAt,
        end: ReplayEnd,
    ) -> Result<ReplayPlan, String> {
        for topic in [source, target] {
            if !TOPICS.contains(&topic) {
                return Err(format!("topic {topic} does not exist"));
            }
        }
        let ranges: Vec<PartitionRange> = (0..PARTITIONS)
            .map(|partition| {
                let hi = self.end(source, partition);
                let from = match start {
                    StartAt::Beginning => 0,
                    StartAt::End => hi,
                    StartAt::LastN(n) => (hi - n).max(0),
                    StartAt::Offset(n) => n.clamp(0, hi),
                    StartAt::Timestamp(ms) => offset_at(self.started_ms, ms).min(hi),
                };
                let to = match end {
                    ReplayEnd::Now => hi,
                    ReplayEnd::Offset(n) => n.clamp(0, hi),
                    ReplayEnd::Timestamp(ms) => offset_at(self.started_ms, ms).min(hi),
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
            target_partitions: PARTITIONS as usize,
            keep_partitions: true,
        };
        if plan.total() == 0 {
            return Err("nothing to replay: the range has no messages".to_owned());
        }
        Ok(plan)
    }
}

/// The first offset at or after `ms`, matching the timestamps `record` uses.
fn offset_at(started_ms: i64, ms: i64) -> i64 {
    let first_ms = started_ms - HISTORY * HISTORY_STEP_MS;
    if ms <= started_ms {
        ((ms - first_ms) as f64 / HISTORY_STEP_MS as f64)
            .ceil()
            .max(0.0) as i64
    } else {
        HISTORY + (ms - started_ms) / LIVE_STEP_MS
    }
}

fn timestamp_of(started_ms: i64, offset: i64) -> i64 {
    if offset < HISTORY {
        started_ms - (HISTORY - offset) * HISTORY_STEP_MS
    } else {
        started_ms + (offset - HISTORY) * LIVE_STEP_MS
    }
}

/// Generates the plan's records, applies the filter and "produces" the matches.
pub(super) async fn replay(
    cluster: Shared,
    id: u64,
    plan: ReplayPlan,
    filter: Option<RecordFilter>,
    mut cancel: oneshot::Receiver<()>,
    events: mpsc::Sender<Event>,
) {
    let started_ms = cluster.lock().expect("demo cluster").started_ms;
    let topic_idx = TOPICS.iter().position(|t| *t == plan.source).unwrap_or(1);
    let mut progress = ReplayProgress {
        id,
        scanned: 0,
        produced: 0,
        done: false,
        error: None,
    };
    let mut produced_per_partition: BTreeMap<i32, i64> = BTreeMap::new();
    let batch = REPLAY_RATE_PER_SEC / 4;
    'ranges: for range in &plan.ranges {
        for offset in range.from..range.to {
            let mut r = record(
                topic_idx,
                range.partition,
                offset,
                timestamp_of(started_ms, offset),
            );
            r.topic = plan.source.clone();
            progress.scanned += 1;
            if filter.as_ref().is_none_or(|f| f.matches(&r)) {
                progress.produced += 1;
                *produced_per_partition.entry(range.partition).or_default() += 1;
            }
            if progress.scanned % batch == 0 {
                let _ = events.send(Event::ReplayProgress(progress.clone())).await;
                tokio::select! {
                    _ = &mut cancel => {
                        progress.error = Some("cancelled".to_owned());
                        break 'ranges;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
            }
        }
    }
    {
        let mut cluster = cluster.lock().expect("demo cluster");
        for (partition, n) in produced_per_partition {
            *cluster
                .added
                .entry((plan.target.clone(), partition))
                .or_default() += n;
        }
    }
    progress.done = true;
    let _ = events.send(Event::ReplayProgress(progress)).await;
}
