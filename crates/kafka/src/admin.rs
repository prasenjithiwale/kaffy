//! Topic administration: create, delete, configs and DeleteRecords.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigSource as KafkaConfigSource,
    NewTopic as KafkaNewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};

use crate::groups::{EARLIEST, LATEST, query_offsets};
use crate::{ConfigEntry, ConfigSource, DeleteRecordsPlan, NewTopic, StartAt, TruncateRow};

const TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) type Admin = AdminClient<DefaultClientContext>;

fn options() -> AdminOptions {
    AdminOptions::new().operation_timeout(Some(TIMEOUT))
}

pub(crate) async fn create_topic(admin: &Admin, spec: &NewTopic) -> Result<(), String> {
    if spec.name.trim().is_empty() {
        return Err("a topic needs a name".to_owned());
    }
    if spec.partitions < 1 || spec.replication < 1 {
        return Err("partitions and replication factor must be at least 1".to_owned());
    }
    let mut topic = KafkaNewTopic::new(
        &spec.name,
        spec.partitions,
        TopicReplication::Fixed(spec.replication),
    );
    for (key, value) in &spec.configs {
        topic = topic.set(key, value);
    }
    let results = admin
        .create_topics(&[topic], &options())
        .await
        .map_err(|e| format!("creating {}: {e}", spec.name))?;
    for result in results {
        result.map_err(|(name, code)| format!("creating {name}: {code}"))?;
    }
    Ok(())
}

pub(crate) async fn delete_topic(admin: &Admin, name: &str) -> Result<(), String> {
    let results = admin
        .delete_topics(&[name], &options())
        .await
        .map_err(|e| format!("deleting {name}: {e}"))?;
    for result in results {
        result.map_err(|(name, code)| format!("deleting {name}: {code}"))?;
    }
    Ok(())
}

pub(crate) async fn describe_config(
    admin: &Admin,
    topic: &str,
) -> Result<Vec<ConfigEntry>, String> {
    let results = admin
        .describe_configs(&[ResourceSpecifier::Topic(topic)], &options())
        .await
        .map_err(|e| format!("reading the config of {topic}: {e}"))?;
    let resource = results
        .into_iter()
        .next()
        .ok_or_else(|| format!("no config returned for {topic}"))?
        .map_err(|code| format!("reading the config of {topic}: {code}"))?;
    let mut entries: Vec<ConfigEntry> = resource
        .entries
        .into_iter()
        .map(|e| ConfigEntry {
            source: match e.source {
                KafkaConfigSource::DynamicTopic => ConfigSource::Topic,
                KafkaConfigSource::DynamicBroker
                | KafkaConfigSource::DynamicDefaultBroker
                | KafkaConfigSource::StaticBroker => ConfigSource::Broker,
                KafkaConfigSource::Default => ConfigSource::Default,
                KafkaConfigSource::Unknown => ConfigSource::Unknown,
            },
            name: e.name,
            value: e.value,
            read_only: e.is_read_only,
            sensitive: e.is_sensitive,
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

/// Applies `changes` (`None` resets to the default) and returns how many there were.
///
/// rdkafka only offers the legacy AlterConfigs call, which replaces all of a
/// topic's settings. So this reads the current topic-level settings, applies
/// the changes on top, sends the full set, and reads back to confirm.
pub(crate) async fn alter_config(
    admin: &Admin,
    topic: &str,
    changes: &[(String, Option<String>)],
) -> Result<usize, String> {
    let current = describe_config(admin, topic).await?;
    let mut settings: BTreeMap<String, String> = BTreeMap::new();
    for entry in current.iter().filter(|e| e.source == ConfigSource::Topic) {
        let value = entry.value.clone().ok_or_else(|| {
            format!(
                "{topic} has a hidden (sensitive) setting {}; changing configs would wipe it",
                entry.name
            )
        })?;
        settings.insert(entry.name.clone(), value);
    }
    for (name, value) in changes {
        let entry = current
            .iter()
            .find(|e| e.name == *name)
            .ok_or_else(|| format!("{name} is not a topic config"))?;
        if entry.read_only {
            return Err(format!("{name} is read-only"));
        }
        match value {
            Some(value) => settings.insert(name.clone(), value.clone()),
            None => settings.remove(name),
        };
    }

    let mut alter = AlterConfig::new(ResourceSpecifier::Topic(topic));
    for (name, value) in &settings {
        alter = alter.set(name, value);
    }
    let results = admin
        .alter_configs(&[alter], &options())
        .await
        .map_err(|e| format!("changing the config of {topic}: {e}"))?;
    for result in results {
        result.map_err(|(_, code)| format!("changing the config of {topic}: {code}"))?;
    }

    let after = describe_config(admin, topic).await?;
    for (name, value) in changes {
        let entry = after.iter().find(|e| e.name == *name);
        let applied = match (value, entry) {
            (Some(value), Some(e)) => e.value.as_deref() == Some(value.as_str()),
            (None, Some(e)) => e.source != ConfigSource::Topic,
            (_, None) => false,
        };
        if !applied {
            return Err(format!(
                "{name} did not take the new value; the broker may have rejected it"
            ));
        }
    }
    Ok(changes.len())
}

/// Works out the new start of each partition. Blocks.
pub(crate) fn plan_delete_records(
    consumer: &BaseConsumer,
    topic: &str,
    before: StartAt,
) -> Result<DeleteRecordsPlan, String> {
    let metadata = consumer
        .fetch_metadata(Some(topic), TIMEOUT)
        .map_err(|e| format!("{topic}: {e}"))?;
    let t = metadata
        .topics()
        .first()
        .filter(|t| t.error().is_none() && !t.partitions().is_empty())
        .ok_or_else(|| format!("topic {topic} does not exist"))?;
    let keys: BTreeSet<(String, i32)> = t
        .partitions()
        .iter()
        .map(|p| (topic.to_owned(), p.id()))
        .collect();
    let lookup = |ts| query_offsets(consumer, &keys, ts).map_err(|e| e.to_string());
    let low = lookup(EARLIEST)?;
    let high = lookup(LATEST)?;
    let at_time = match before {
        StartAt::Timestamp(ms) => Some(lookup(ms)?),
        _ => None,
    };
    let rows = keys
        .iter()
        .map(|key| {
            let lo = low.get(key).copied().unwrap_or(0).max(0);
            let hi = high.get(key).copied().unwrap_or(lo).max(lo);
            let new_low = match before {
                StartAt::Beginning => lo,
                StartAt::End => hi,
                StartAt::LastN(n) => (hi - n).max(lo),
                StartAt::Offset(n) => n.clamp(lo, hi),
                StartAt::Timestamp(_) => match at_time.as_ref().and_then(|m| m.get(key)) {
                    Some(&o) if o >= 0 => o.max(lo),
                    _ => hi,
                },
            };
            TruncateRow {
                partition: key.1,
                low: lo,
                high: hi,
                new_low,
            }
        })
        .collect();
    let plan = DeleteRecordsPlan {
        topic: topic.to_owned(),
        rows,
    };
    if plan.total() == 0 {
        return Err("nothing to delete: no partition has records before that point".to_owned());
    }
    Ok(plan)
}

/// Deletes records before `offsets` and returns how many were removed.
pub(crate) async fn delete_records(
    admin: &Admin,
    consumer: &BaseConsumer,
    topic: &str,
    offsets: &[(i32, i64)],
) -> Result<i64, String> {
    let keys: BTreeSet<(String, i32)> = offsets
        .iter()
        .map(|&(p, _)| (topic.to_owned(), p))
        .collect();
    let before = query_offsets(consumer, &keys, EARLIEST).map_err(|e| e.to_string())?;
    let mut tpl = TopicPartitionList::new();
    for &(partition, offset) in offsets {
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| e.to_string())?;
    }
    let result = admin
        .delete_records(&tpl, &options())
        .await
        .map_err(|e| format!("deleting records from {topic}: {e}"))?;
    let mut deleted = 0;
    for element in result.elements() {
        element
            .error()
            .map_err(|e| format!("{topic} partition {}: {e}", element.partition()))?;
        if let Offset::Offset(new_low) = element.offset() {
            let old = before
                .get(&(topic.to_owned(), element.partition()))
                .copied()
                .unwrap_or(new_low);
            deleted += (new_low - old).max(0);
        }
    }
    Ok(deleted)
}
