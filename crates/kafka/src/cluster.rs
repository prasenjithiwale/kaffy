//! Cluster health: brokers, the controller and partitions that need attention.

use std::collections::BTreeMap;

use crate::TopicInfo;

/// Answer to [`crate::Command::DescribeCluster`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    pub cluster_id: Option<String>,
    /// `None` when the broker didn't say.
    pub controller: Option<i32>,
    /// Brokers in the metadata, by id.
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

/// One broker's share of the partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHealth {
    pub id: i32,
    /// `None` for a broker that holds replicas but is missing from the metadata,
    /// which usually means it is down.
    pub address: Option<String>,
    pub leaders: usize,
    pub replicas: usize,
    /// Replicas on this broker that have fallen out of the in-sync set.
    pub out_of_sync: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProblem {
    pub topic: String,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub kind: ProblemKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProblemKind {
    /// No leader: the partition can't be read or written.
    Offline,
    UnderReplicated,
}

impl ClusterInfo {
    pub fn partition_count(&self) -> usize {
        self.topics.iter().map(|t| t.partitions.len()).sum()
    }

    /// Every broker in the metadata, plus any that replicas point at but the
    /// metadata leaves out. Sorted by id.
    pub fn broker_health(&self) -> Vec<BrokerHealth> {
        let mut health: BTreeMap<i32, BrokerHealth> = self
            .brokers
            .iter()
            .map(|b| {
                let health = BrokerHealth {
                    id: b.id,
                    address: Some(format!("{}:{}", b.host, b.port)),
                    leaders: 0,
                    replicas: 0,
                    out_of_sync: 0,
                };
                (b.id, health)
            })
            .collect();
        for partition in self.topics.iter().flat_map(|t| &t.partitions) {
            for &id in &partition.replicas {
                let broker = health.entry(id).or_insert(BrokerHealth {
                    id,
                    address: None,
                    leaders: 0,
                    replicas: 0,
                    out_of_sync: 0,
                });
                broker.replicas += 1;
                if !partition.isr.contains(&id) {
                    broker.out_of_sync += 1;
                }
            }
            if let Some(leader) = health.get_mut(&partition.leader) {
                leader.leaders += 1;
            }
        }
        health.into_values().collect()
    }

    /// Offline partitions first, then under-replicated ones, each by topic and partition.
    pub fn problems(&self) -> Vec<PartitionProblem> {
        let mut problems: Vec<PartitionProblem> = self
            .topics
            .iter()
            .flat_map(|topic| {
                topic.partitions.iter().filter_map(|p| {
                    let kind = if p.leader < 0 {
                        ProblemKind::Offline
                    } else if p.isr.len() < p.replicas.len() {
                        ProblemKind::UnderReplicated
                    } else {
                        return None;
                    };
                    Some(PartitionProblem {
                        topic: topic.name.clone(),
                        partition: p.id,
                        leader: p.leader,
                        replicas: p.replicas.clone(),
                        isr: p.isr.clone(),
                        kind,
                    })
                })
            })
            .collect();
        problems
            .sort_by(|a, b| (a.kind, &a.topic, a.partition).cmp(&(b.kind, &b.topic, b.partition)));
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionInfo;

    fn partition(id: i32, leader: i32, replicas: &[i32], isr: &[i32]) -> PartitionInfo {
        PartitionInfo {
            id,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
        }
    }

    fn cluster() -> ClusterInfo {
        let broker = |id| BrokerInfo {
            id,
            host: format!("b{id}"),
            port: 9092,
        };
        ClusterInfo {
            cluster_id: Some("abc".into()),
            controller: Some(1),
            // Broker 3 holds replicas but is missing from the metadata.
            brokers: vec![broker(2), broker(1)],
            topics: vec![
                TopicInfo {
                    name: "b".into(),
                    partitions: vec![
                        partition(0, 1, &[1, 2], &[1, 2]),
                        partition(1, 2, &[2, 3], &[2]),
                    ],
                },
                TopicInfo {
                    name: "a".into(),
                    partitions: vec![partition(0, -1, &[3], &[])],
                },
            ],
        }
    }

    #[test]
    fn broker_health_counts_leaders_replicas_and_missing_brokers() {
        let health = cluster().broker_health();
        assert_eq!(
            health,
            vec![
                BrokerHealth {
                    id: 1,
                    address: Some("b1:9092".into()),
                    leaders: 1,
                    replicas: 1,
                    out_of_sync: 0,
                },
                BrokerHealth {
                    id: 2,
                    address: Some("b2:9092".into()),
                    leaders: 1,
                    replicas: 2,
                    out_of_sync: 0,
                },
                BrokerHealth {
                    id: 3,
                    address: None,
                    leaders: 0,
                    replicas: 2,
                    out_of_sync: 2,
                },
            ]
        );
    }

    #[test]
    fn problems_list_offline_partitions_first() {
        let problems = cluster().problems();
        let summary: Vec<_> = problems
            .iter()
            .map(|p| (p.kind, p.topic.as_str(), p.partition))
            .collect();
        assert_eq!(
            summary,
            vec![
                (ProblemKind::Offline, "a", 0),
                (ProblemKind::UnderReplicated, "b", 1),
            ]
        );
        assert_eq!(cluster().partition_count(), 3);
    }
}
