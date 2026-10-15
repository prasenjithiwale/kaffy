//! Kafka I/O for kaffy.
//!
//! All broker access happens in background tokio tasks. The UI talks to them
//! through a [`Worker`]: it sends [`Command`]s and receives [`Event`]s, so the
//! render loop never blocks on the network.

mod admin;
mod client;
mod cluster;
mod decoders;
mod demo;
mod group_offsets;
mod groups;
mod msk_iam;
mod replay;

use std::sync::Arc;

use tokio::sync::mpsc;

pub use client::spawn;
pub use cluster::{BrokerHealth, BrokerInfo, ClusterInfo, PartitionProblem, ProblemKind};
pub use demo::spawn_demo;
pub use kaffy_schema::{Compatibility, Decoded, SchemaType, SubjectSchema};

/// Capacity of the event channel. When the UI falls behind, the consumer task
/// waits instead of buffering without bound.
const EVENT_BUFFER: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] kaffy_config::ConfigError),
    #[error("kafka client: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("{0}")]
    SchemaRegistry(String),
    #[error("{0}")]
    Protobuf(String),
    #[error("{0}")]
    Auth(String),
}

pub struct Worker {
    pub commands: mpsc::UnboundedSender<Command>,
    pub events: mpsc::Receiver<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Fetch cluster metadata; answered with [`Event::Metadata`].
    ListTopics,
    /// Brokers, controller and partition health; answered with [`Event::Cluster`].
    DescribeCluster,
    /// Replace any running tail with a new one. `session` is echoed back on
    /// every event so stale records from the previous tail can be ignored.
    Tail {
        session: u64,
        topic: String,
        start: StartAt,
    },
    StopTail,
    Pause,
    Resume,
    /// Consumer groups with their total lag; answered with [`Event::Groups`].
    ListGroups,
    /// Per-partition lag and members; answered with [`Event::GroupDetail`].
    DescribeGroup(String),
    /// Work out new offsets without changing anything; answered with [`Event::ResetPlan`].
    PlanReset {
        group: String,
        /// Only this topic, or every topic the group has offsets for.
        topic: Option<String>,
        target: ResetTarget,
    },
    /// Commit the offsets of a plan; answered with [`Event::ResetApplied`].
    /// Refused unless the group has no active members.
    ApplyReset {
        group: String,
        offsets: Vec<PartitionOffset>,
    },
    /// Answered with [`Event::Produced`].
    Produce(ProduceRequest),
    /// Resolve a range to copy; answered with [`Event::ReplayPlan`].
    PlanReplay {
        source: String,
        target: String,
        start: StartAt,
        end: ReplayEnd,
    },
    /// Copy the plan's records to its target, reporting [`Event::ReplayProgress`].
    Replay {
        id: u64,
        plan: ReplayPlan,
        filter: Option<RecordFilter>,
    },
    CancelReplay,
    /// Answered with [`Event::TopicCreated`].
    CreateTopic(NewTopic),
    /// Answered with [`Event::TopicDeleted`]. Permanent.
    DeleteTopic(String),
    /// Answered with [`Event::TopicConfig`].
    DescribeTopicConfig(String),
    /// Set (`Some`) or reset to the default (`None`) topic configs; answered
    /// with [`Event::TopicConfigAltered`].
    AlterTopicConfig {
        topic: String,
        changes: Vec<(String, Option<String>)>,
    },
    /// Work out what deleting records before a position would remove;
    /// answered with [`Event::DeleteRecordsPlan`].
    PlanDeleteRecords {
        topic: String,
        before: StartAt,
    },
    /// Delete every record before these offsets; answered with
    /// [`Event::RecordsDeleted`]. Permanent.
    DeleteRecords {
        topic: String,
        offsets: Vec<(i32, i64)>,
    },
    /// The latest schema of every subject; answered with [`Event::Subjects`].
    ListSubjects,
    /// Versions and compatibility rule; answered with [`Event::SubjectDetail`].
    DescribeSubject(String),
    /// One version (`None` = latest); answered with [`Event::SchemaVersion`].
    FetchSchema {
        subject: String,
        version: Option<i32>,
    },
    /// Answered with [`Event::SchemaChecked`].
    CheckSchema {
        subject: String,
        schema_type: SchemaType,
        schema: String,
    },
    /// Answered with [`Event::SchemaRegistered`].
    RegisterSchema {
        subject: String,
        schema_type: SchemaType,
        schema: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    pub name: String,
    pub partitions: i32,
    pub replication: i32,
    pub configs: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub name: String,
    /// `None` for sensitive values the broker does not reveal.
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Set on this topic.
    Topic,
    /// Inherited from the broker configuration.
    Broker,
    Default,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPlan {
    pub topic: String,
    pub rows: Vec<TruncateRow>,
}

impl DeleteRecordsPlan {
    /// Records that will be deleted.
    pub fn total(&self) -> i64 {
        self.rows.iter().map(|r| (r.new_low - r.low).max(0)).sum()
    }

    pub fn offsets(&self) -> Vec<(i32, i64)> {
        self.rows
            .iter()
            .filter(|r| r.new_low > r.low)
            .map(|r| (r.partition, r.new_low))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateRow {
    pub partition: i32,
    /// First offset currently kept.
    pub low: i64,
    pub high: i64,
    /// First offset kept afterwards.
    pub new_low: i64,
}

/// The operation an [`Event::Failed`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    DescribeCluster,
    ListGroups,
    DescribeGroup,
    PlanReset,
    ApplyReset,
    Produce,
    PlanReplay,
    Replay,
    CreateTopic,
    DeleteTopic,
    TopicConfig,
    AlterTopicConfig,
    PlanDeleteRecords,
    DeleteRecords,
    ListSubjects,
    DescribeSubject,
    FetchSchema,
    CheckSchema,
    RegisterSchema,
}

/// Where an offset reset moves a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetTarget {
    Earliest,
    Latest,
    /// The first offset at or after this time (epoch ms); the end if none.
    Timestamp(i64),
    /// This offset in every partition, clamped to what the partition holds.
    Offset(i64),
}

/// Where a replay stops (exclusive), applied to every partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayEnd {
    /// The end of each partition when the plan is made.
    Now,
    Offset(i64),
    /// Epoch milliseconds.
    Timestamp(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub id: u64,
    pub topic: String,
    /// `None` lets the partitioner choose from the key.
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    /// `None` produces a tombstone.
    pub value: Option<Payload>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    Bytes(Vec<u8>),
    /// JSON to encode as Avro with this Schema Registry schema.
    Avro {
        schema_id: u32,
        json: serde_json::Value,
    },
    /// JSON to encode as Protobuf message `message`, with this Schema Registry
    /// schema, or with the topic's local `.proto` files when `None`.
    Protobuf {
        schema_id: Option<u32>,
        message: String,
        json: serde_json::Value,
    },
}

/// A predicate on records, used to replay only what the current filter shows.
#[derive(Clone)]
pub struct RecordFilter(Arc<dyn Fn(&Record) -> bool + Send + Sync>);

impl RecordFilter {
    pub fn new(f: impl Fn(&Record) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    pub fn matches(&self, record: &Record) -> bool {
        (self.0)(record)
    }
}

impl std::fmt::Debug for RecordFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordFilter(..)")
    }
}

impl PartialEq for RecordFilter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RecordFilter {}

/// Where a tail starts reading, applied to every partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAt {
    Beginning,
    End,
    /// The last N messages of each partition.
    LastN(i64),
    Offset(i64),
    /// Epoch milliseconds.
    Timestamp(i64),
}

#[derive(Debug, Clone)]
pub enum Event {
    Metadata {
        brokers: usize,
        topics: Vec<TopicInfo>,
    },
    Cluster(ClusterInfo),
    TailStarted {
        session: u64,
        partitions: usize,
    },
    Record {
        session: u64,
        /// Boxed: records are much larger than the other events.
        record: Box<Record>,
    },
    Groups(Vec<GroupSummary>),
    GroupDetail(GroupDetail),
    ResetPlan(ResetPlan),
    ResetApplied {
        group: String,
        partitions: usize,
    },
    Produced {
        id: u64,
        topic: String,
        partition: i32,
        offset: i64,
    },
    ReplayPlan(ReplayPlan),
    ReplayProgress(ReplayProgress),
    TopicCreated {
        name: String,
    },
    TopicDeleted {
        name: String,
    },
    TopicConfig {
        topic: String,
        entries: Vec<ConfigEntry>,
    },
    TopicConfigAltered {
        topic: String,
        changed: usize,
    },
    DeleteRecordsPlan(DeleteRecordsPlan),
    RecordsDeleted {
        topic: String,
        records: i64,
    },
    Subjects(Vec<SubjectSchema>),
    SubjectDetail {
        subject: String,
        versions: Vec<i32>,
        compatibility: String,
    },
    SchemaVersion(SubjectSchema),
    SchemaChecked {
        subject: String,
        schema_type: SchemaType,
        schema: String,
        compatibility: Compatibility,
    },
    SchemaRegistered {
        subject: String,
        id: u32,
        version: i32,
    },
    /// A command failed; the UI closes whatever was waiting for it.
    Failed {
        operation: Operation,
        message: String,
    },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    Stable,
    /// No members: offsets can be reset.
    Empty,
    Rebalancing,
    Dead,
    Other(String),
}

impl GroupState {
    pub fn parse(state: &str) -> Self {
        match state {
            "Stable" => Self::Stable,
            "Empty" => Self::Empty,
            "PreparingRebalance" | "CompletingRebalance" | "AwaitingSync" => Self::Rebalancing,
            "Dead" => Self::Dead,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn label(&self) -> &str {
        match self {
            Self::Stable => "Stable",
            Self::Empty => "Empty",
            Self::Rebalancing => "Rebalancing",
            Self::Dead => "Dead",
            Self::Other(s) => s,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSummary {
    pub name: String,
    pub state: GroupState,
    pub members: usize,
    /// Topics the group has committed offsets for.
    pub topics: Vec<String>,
    /// Sum of per-partition lag; `None` when offsets could not be read.
    pub lag: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDetail {
    pub name: String,
    pub state: GroupState,
    pub members: Vec<GroupMember>,
    pub partitions: Vec<PartitionLag>,
}

impl GroupDetail {
    pub fn total_lag(&self) -> i64 {
        self.partitions.iter().map(PartitionLag::lag).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub client_id: String,
    pub host: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed: i64,
    /// High watermark: the offset the next message will get.
    pub end: i64,
}

impl PartitionLag {
    pub fn lag(&self) -> i64 {
        (self.end - self.committed).max(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetPlan {
    pub group: String,
    pub state: GroupState,
    pub target: ResetTarget,
    pub rows: Vec<ResetRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetRow {
    pub topic: String,
    pub partition: i32,
    /// `None` when the group has no committed offset for the partition.
    pub current: Option<i64>,
    pub new: i64,
    pub end: i64,
}

impl ResetPlan {
    pub fn offsets(&self) -> Vec<PartitionOffset> {
        self.rows
            .iter()
            .map(|r| PartitionOffset {
                topic: r.topic.clone(),
                partition: r.partition,
                offset: r.new,
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPlan {
    pub source: String,
    pub target: String,
    pub ranges: Vec<PartitionRange>,
    pub target_partitions: usize,
    /// Records keep their partition number (when the partition counts match);
    /// otherwise the key decides, as with Java clients.
    pub keep_partitions: bool,
}

impl ReplayPlan {
    /// Offsets in range; compacted or filtered topics may yield fewer records.
    pub fn total(&self) -> i64 {
        self.ranges.iter().map(|r| (r.to - r.from).max(0)).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRange {
    pub partition: i32,
    /// First offset to copy.
    pub from: i64,
    /// Offset to stop before.
    pub to: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayProgress {
    pub id: u64,
    /// Records read from the source.
    pub scanned: u64,
    /// Records written to the target (after the filter).
    pub produced: u64,
    pub done: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}

impl TopicInfo {
    pub fn replication_factor(&self) -> usize {
        self.partitions
            .iter()
            .map(|p| p.replicas.len())
            .max()
            .unwrap_or(0)
    }

    pub fn under_replicated(&self) -> usize {
        self.partitions
            .iter()
            .filter(|p| p.isr.len() < p.replicas.len())
            .count()
    }

    pub fn is_internal(&self) -> bool {
        self.name.starts_with("__") || self.name.starts_with("_confluent")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
    /// Set when the key or value is in the Schema Registry wire format and a
    /// registry is configured.
    pub key_decoded: Option<Decoded>,
    pub value_decoded: Option<Decoded>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
