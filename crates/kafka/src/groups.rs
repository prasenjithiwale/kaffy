//! Consumer groups: listing, lag, and offset resets. All functions block;
//! call them from `spawn_blocking`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kaffy_config::NameFilter;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::{Offset, TopicPartitionList};

use crate::msk_iam::Auth;
use crate::{
    GroupDetail, GroupMember, GroupState, GroupSummary, PartitionLag, PartitionOffset, ResetPlan,
    ResetRow, ResetTarget,
};

const TIMEOUT: Duration = Duration::from_secs(10);

/// librdkafka's special timestamps for offsets_for_times.
pub(crate) const LATEST: i64 = -1;
pub(crate) const EARLIEST: i64 = -2;

pub(crate) struct Groups {
    base: ClientConfig,
    /// Metadata, group list and watermark queries.
    admin: Arc<BaseConsumer>,
    /// One client per group, configured with that group.id to read and commit
    /// its offsets. They never subscribe, so they never join the group.
    clients: Mutex<HashMap<String, Arc<BaseConsumer>>>,
    auth: Auth,
    /// The profile's `groups` patterns: other groups are skipped before any
    /// offset lookups, which is where listing groups spends its time.
    scope: NameFilter,
}

impl Groups {
    pub(crate) fn new(
        base: ClientConfig,
        admin: Arc<BaseConsumer>,
        auth: Auth,
        scope: NameFilter,
    ) -> Self {
        Self {
            base,
            admin,
            clients: Mutex::new(HashMap::new()),
            auth,
            scope,
        }
    }

    pub(crate) fn list(&self) -> KafkaResult<Vec<GroupSummary>> {
        let groups = self.admin.fetch_group_list(None, TIMEOUT)?;
        let partitions = self.all_partitions()?;
        let mut summaries = Vec::new();
        let mut committed_by_group = Vec::new();
        for group in groups.groups() {
            // Only consumer groups have offsets; skip Connect workers and the like.
            if !matches!(group.protocol_type(), "consumer" | "")
                || !self.scope.matches(group.name())
            {
                continue;
            }
            let committed = self.committed(group.name(), &partitions);
            committed_by_group.push(committed);
            summaries.push(GroupSummary {
                name: group.name().to_owned(),
                state: GroupState::parse(group.state()),
                members: group.members().len(),
                topics: Vec::new(),
                lag: None,
            });
        }
        // One batched end-offset lookup for every partition any group uses.
        let used: BTreeSet<(String, i32)> = committed_by_group
            .iter()
            .flatten()
            .flat_map(|c| c.keys().cloned())
            .collect();
        let ends = self.end_offsets(&used)?;
        for (summary, committed) in summaries.iter_mut().zip(committed_by_group) {
            if let Ok(committed) = committed {
                summary.topics = committed
                    .keys()
                    .map(|(t, _)| t.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                summary.lag = Some(
                    committed
                        .iter()
                        .map(|(tp, &offset)| {
                            (ends.get(tp).copied().unwrap_or(offset) - offset).max(0)
                        })
                        .sum(),
                );
            }
        }
        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(summaries)
    }

    pub(crate) fn describe(&self, name: &str) -> KafkaResult<GroupDetail> {
        let (state, members) = self.group_info(name)?;
        let committed = self.committed(name, &self.all_partitions()?)?;
        let ends = self.end_offsets(&committed.keys().cloned().collect())?;
        let partitions = committed
            .into_iter()
            .map(|((topic, partition), offset)| PartitionLag {
                end: ends
                    .get(&(topic.clone(), partition))
                    .copied()
                    .unwrap_or(offset),
                topic,
                partition,
                committed: offset,
            })
            .collect();
        Ok(GroupDetail {
            name: name.to_owned(),
            state,
            members,
            partitions,
        })
    }

    pub(crate) fn plan_reset(
        &self,
        group: &str,
        topic: Option<&str>,
        target: ResetTarget,
    ) -> Result<ResetPlan, String> {
        let (state, _) = self.group_info(group).map_err(|e| e.to_string())?;
        let partitions = match topic {
            Some(topic) => self.topic_partitions(topic).map_err(|e| e.to_string())?,
            None => Vec::new(),
        };
        let mut committed = self
            .committed(group, &self.all_partitions().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        if let Some(topic) = topic {
            committed.retain(|(t, _), _| t == topic);
        }
        // Include partitions of the chosen topic the group never committed.
        let mut keys: BTreeSet<(String, i32)> = committed.keys().cloned().collect();
        keys.extend(partitions);
        if keys.is_empty() {
            return Err(match topic {
                Some(t) => format!("topic {t} has no partitions"),
                None => format!("group {group} has no committed offsets; pick a topic"),
            });
        }

        let earliest = self
            .query_offsets(&keys, EARLIEST)
            .map_err(|e| e.to_string())?;
        let latest = self
            .query_offsets(&keys, LATEST)
            .map_err(|e| e.to_string())?;
        let at_time = match target {
            ResetTarget::Timestamp(ms) => {
                Some(self.query_offsets(&keys, ms).map_err(|e| e.to_string())?)
            }
            _ => None,
        };
        let rows = keys
            .into_iter()
            .map(|tp| {
                let low = earliest.get(&tp).copied().unwrap_or(0);
                let high = latest.get(&tp).copied().unwrap_or(low);
                let new = match target {
                    ResetTarget::Earliest => low,
                    ResetTarget::Latest => high,
                    // offsets_for_times answers -1 when no message is that new.
                    ResetTarget::Timestamp(_) => match at_time.as_ref().and_then(|m| m.get(&tp)) {
                        Some(&o) if o >= 0 => o,
                        _ => high,
                    },
                    ResetTarget::Offset(n) => n.clamp(low, high),
                };
                ResetRow {
                    current: committed.get(&tp).copied(),
                    topic: tp.0,
                    partition: tp.1,
                    new,
                    end: high,
                }
            })
            .collect();
        Ok(ResetPlan {
            group: group.to_owned(),
            state,
            target,
            rows,
        })
    }

    /// Commits `offsets` for `group`, after checking again that it has no members:
    /// the broker would reject the commit otherwise, and a running consumer would
    /// overwrite it anyway.
    pub(crate) fn apply_reset(
        &self,
        group: &str,
        offsets: &[PartitionOffset],
    ) -> Result<usize, String> {
        let (state, members) = self.group_info(group).map_err(|e| e.to_string())?;
        if !members.is_empty() || !matches!(state, GroupState::Empty | GroupState::Dead) {
            return Err(format!(
                "group {group} is {} with {} active member(s); stop its consumers and try again",
                state.label(),
                members.len()
            ));
        }
        let mut tpl = TopicPartitionList::new();
        for o in offsets {
            tpl.add_partition_offset(&o.topic, o.partition, Offset::Offset(o.offset))
                .map_err(|e| e.to_string())?;
        }
        let client = self.client(group).map_err(|e| e.to_string())?;
        client
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| format!("commit failed: {e}"))?;
        // Read back so a silently ignored commit is reported, not assumed.
        let committed = client
            .committed_offsets(tpl, TIMEOUT)
            .map_err(|e| e.to_string())?;
        for o in offsets {
            let actual = committed
                .find_partition(&o.topic, o.partition)
                .map(|p| p.offset());
            if actual != Some(Offset::Offset(o.offset)) {
                return Err(format!(
                    "{} partition {} is at {actual:?} after the commit, not {}",
                    o.topic, o.partition, o.offset
                ));
            }
        }
        Ok(offsets.len())
    }

    fn group_info(&self, name: &str) -> KafkaResult<(GroupState, Vec<GroupMember>)> {
        let list = self.admin.fetch_group_list(Some(name), TIMEOUT)?;
        let Some(group) = list.groups().iter().find(|g| g.name() == name) else {
            return Ok((GroupState::Dead, Vec::new()));
        };
        let members = group
            .members()
            .iter()
            .map(|m| GroupMember {
                client_id: m.client_id().to_owned(),
                host: m.client_host().trim_start_matches('/').to_owned(),
            })
            .collect();
        Ok((GroupState::parse(group.state()), members))
    }

    fn client(&self, group: &str) -> KafkaResult<Arc<BaseConsumer>> {
        let mut clients = self.clients.lock().expect("group clients lock");
        if let Some(client) = clients.get(group) {
            return Ok(client.clone());
        }
        let client: Arc<BaseConsumer> = Arc::new(
            self.base
                .clone()
                .set("group.id", group)
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .create()?,
        );
        self.auth
            .attach(&client)
            .map_err(KafkaError::ClientCreation)?;
        clients.insert(group.to_owned(), client.clone());
        Ok(client)
    }

    fn all_partitions(&self) -> KafkaResult<Vec<(String, i32)>> {
        let metadata = self.admin.fetch_metadata(None, TIMEOUT)?;
        Ok(metadata
            .topics()
            .iter()
            .flat_map(|t| t.partitions().iter().map(|p| (t.name().to_owned(), p.id())))
            .collect())
    }

    fn topic_partitions(&self, topic: &str) -> KafkaResult<Vec<(String, i32)>> {
        let metadata = self.admin.fetch_metadata(Some(topic), TIMEOUT)?;
        let t = metadata.topics().first().ok_or(KafkaError::MetadataFetch(
            rdkafka::types::RDKafkaErrorCode::UnknownTopic,
        ))?;
        if let Some(err) = t.error() {
            return Err(KafkaError::MetadataFetch(err.into()));
        }
        Ok(t.partitions()
            .iter()
            .map(|p| (topic.to_owned(), p.id()))
            .collect())
    }

    /// Committed offsets of `group` among `partitions`; partitions without a
    /// commit are left out.
    fn committed(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> KafkaResult<BTreeMap<(String, i32), i64>> {
        let mut tpl = TopicPartitionList::new();
        for (topic, partition) in partitions {
            tpl.add_partition(topic, *partition);
        }
        let committed = self.client(group)?.committed_offsets(tpl, TIMEOUT)?;
        Ok(committed
            .elements()
            .iter()
            .filter_map(|e| match e.offset() {
                Offset::Offset(o) => Some(((e.topic().to_owned(), e.partition()), o)),
                _ => None,
            })
            .collect())
    }

    fn end_offsets(
        &self,
        partitions: &BTreeSet<(String, i32)>,
    ) -> KafkaResult<HashMap<(String, i32), i64>> {
        Ok(self
            .query_offsets(partitions, LATEST)?
            .into_iter()
            .collect())
    }

    fn query_offsets(
        &self,
        partitions: &BTreeSet<(String, i32)>,
        timestamp: i64,
    ) -> KafkaResult<BTreeMap<(String, i32), i64>> {
        query_offsets(&self.admin, partitions, timestamp)
    }
}

/// One batched ListOffsets request: `timestamp` is epoch ms, or LATEST/EARLIEST.
/// Partitions with no message at or after the timestamp map to -1.
pub(crate) fn query_offsets(
    consumer: &BaseConsumer,
    partitions: &BTreeSet<(String, i32)>,
    timestamp: i64,
) -> KafkaResult<BTreeMap<(String, i32), i64>> {
    if partitions.is_empty() {
        return Ok(BTreeMap::new());
    }
    // The special timestamps travel as the matching logical offsets; rdkafka
    // rejects negative Offset::Offset values.
    let query = match timestamp {
        LATEST => Offset::End,
        EARLIEST => Offset::Beginning,
        ms => Offset::Offset(ms),
    };
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition_offset(topic, *partition, query)?;
    }
    let result = consumer.offsets_for_times(tpl, TIMEOUT)?;
    Ok(result
        .elements()
        .iter()
        .map(|e| {
            let offset = match e.offset() {
                Offset::Offset(o) => o,
                _ => -1,
            };
            ((e.topic().to_owned(), e.partition()), offset)
        })
        .collect())
}
