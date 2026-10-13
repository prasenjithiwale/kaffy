//! State of the broker health screen.

use std::time::Instant;

use kaffy_kafka::ClusterInfo;
use ratatui::widgets::TableState;

use crate::groups::REFRESH_EVERY;

#[derive(Debug, Clone, Default)]
pub struct BrokersView {
    pub cluster: Option<ClusterInfo>,
    /// Selection in the partition problems table.
    pub table: TableState,
    /// When the last refresh was requested; `None` sends one right away.
    pub requested: Option<Instant>,
}

impl BrokersView {
    pub(crate) fn clamp_selection(&mut self) {
        let len = self.cluster.as_ref().map_or(0, |c| c.problems().len());
        let selected = self.table.selected().unwrap_or(0);
        self.table.select(Some(selected.min(len.saturating_sub(1))));
    }

    pub(crate) fn due(&self) -> bool {
        self.requested
            .is_none_or(|at| at.elapsed() >= REFRESH_EVERY)
    }
}
