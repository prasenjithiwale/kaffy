//! Kafka I/O for kaffy.
//!
//! All broker access happens in background tokio tasks. The UI talks to them
//! through a [`Worker`]: it sends [`Command`]s and receives [`Event`]s, so the
//! render loop never blocks on the network.

mod client;
mod demo;

use tokio::sync::mpsc;

pub use client::spawn;
pub use demo::spawn_demo;

/// Capacity of the event channel. When the UI falls behind, the consumer task
/// waits instead of buffering without bound.
const EVENT_BUFFER: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] kaffy_config::ConfigError),
    #[error("kafka client: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

pub struct Worker {
    pub commands: mpsc::UnboundedSender<Command>,
    pub events: mpsc::Receiver<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Fetch cluster metadata; answered with [`Event::Metadata`].
    ListTopics,
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
}

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
    TailStarted {
        session: u64,
        partitions: usize,
    },
    Record {
        session: u64,
        record: Record,
    },
    Error(String),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
