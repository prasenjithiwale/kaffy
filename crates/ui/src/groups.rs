//! State of the consumer group screens.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use kaffy_config::NameFilter;
use kaffy_kafka::{GroupDetail, GroupSummary};
use ratatui::widgets::TableState;

/// How often lag is refreshed while a group screen is open.
pub const REFRESH_EVERY: Duration = Duration::from_secs(5);
/// Lag samples kept per group for the trend charts.
pub const HISTORY_LEN: usize = 60;

#[derive(Debug, Clone, Default)]
pub struct GroupsView {
    pub list: Option<Vec<GroupSummary>>,
    pub table: TableState,
    pub filter: String,
    /// The profile's `groups` patterns; other groups are never shown.
    pub scope: NameFilter,
    /// Total lag samples per group, oldest first.
    pub history: HashMap<String, VecDeque<i64>>,
    /// When the last refresh was requested; `None` sends one right away.
    pub requested: Option<Instant>,
}

impl GroupsView {
    pub fn visible(&self) -> Vec<&GroupSummary> {
        let needle = self.filter.to_lowercase();
        self.list
            .iter()
            .flatten()
            .filter(|g| {
                g.name.to_lowercase().contains(&needle)
                    || g.topics.iter().any(|t| t.to_lowercase().contains(&needle))
            })
            .collect()
    }

    pub fn selected(&self) -> Option<&GroupSummary> {
        self.visible()
            .get(self.table.selected().unwrap_or(0))
            .copied()
    }

    pub fn record_lag(&mut self, group: &str, lag: i64) {
        let samples = self.history.entry(group.to_owned()).or_default();
        if samples.len() == HISTORY_LEN {
            samples.pop_front();
        }
        samples.push_back(lag);
    }

    pub(crate) fn clamp_selection(&mut self) {
        let len = self.visible().len();
        let selected = self.table.selected().unwrap_or(0);
        self.table.select(Some(selected.min(len.saturating_sub(1))));
    }

    /// Whether a refresh is due.
    pub(crate) fn due(&self) -> bool {
        self.requested
            .is_none_or(|at| at.elapsed() >= REFRESH_EVERY)
    }
}

#[derive(Debug, Clone)]
pub struct GroupDetailView {
    pub name: String,
    pub detail: Option<GroupDetail>,
    pub table: TableState,
    pub requested: Option<Instant>,
}

impl GroupDetailView {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            detail: None,
            table: TableState::default().with_selected(0),
            requested: None,
        }
    }

    pub(crate) fn due(&self) -> bool {
        self.requested
            .is_none_or(|at| at.elapsed() >= REFRESH_EVERY)
    }

    /// Topics the group has offsets for, in table order.
    pub fn topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self
            .detail
            .iter()
            .flat_map(|d| d.partitions.iter().map(|p| p.topic.clone()))
            .collect();
        topics.dedup();
        topics
    }
}
