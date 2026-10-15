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

/// Auto-refresh timing for a screen. At most one refresh is in flight, and the
/// next one is due `REFRESH_EVERY` after the last answer (or failure) arrived,
/// so a cluster slower than the interval doesn't pile up requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct Refresh {
    sent: Option<Instant>,
    answered: Option<Instant>,
}

/// A request unanswered this long is assumed lost, so auto-refresh resumes.
const LOST_AFTER: Duration = Duration::from_secs(60);

impl Refresh {
    pub(crate) fn due(&self) -> bool {
        match self.sent {
            Some(at) if at.elapsed() < LOST_AFTER => false,
            _ => self.answered.is_none_or(|at| at.elapsed() >= REFRESH_EVERY),
        }
    }

    /// A request went out: an automatic one, or one the user asked for.
    pub(crate) fn sent(&mut self) {
        self.sent = Some(Instant::now());
    }

    /// Its answer or failure arrived.
    pub(crate) fn answered(&mut self) {
        self.sent = None;
        self.answered = Some(Instant::now());
    }

    /// Refresh on the next tick once nothing is in flight, e.g. after a reset.
    pub(crate) fn expire(&mut self) {
        self.answered = None;
    }

    pub fn in_flight(&self) -> bool {
        self.sent.is_some_and(|at| at.elapsed() < LOST_AFTER)
    }

    /// Tests: the last answer came in longer ago than the interval.
    #[cfg(test)]
    pub(crate) fn answered_long_ago(&mut self) {
        self.sent = None;
        self.answered = Instant::now().checked_sub(REFRESH_EVERY);
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupsView {
    pub list: Option<Vec<GroupSummary>>,
    pub table: TableState,
    pub filter: String,
    /// The profile's `groups` patterns; other groups are never shown.
    pub scope: NameFilter,
    /// Total lag samples per group, oldest first.
    pub history: HashMap<String, VecDeque<i64>>,
    pub refresh: Refresh,
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
}

#[derive(Debug, Clone)]
pub struct GroupDetailView {
    pub name: String,
    pub detail: Option<GroupDetail>,
    pub table: TableState,
    pub refresh: Refresh,
}

impl GroupDetailView {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            detail: None,
            table: TableState::default().with_selected(0),
            refresh: Refresh::default(),
        }
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
