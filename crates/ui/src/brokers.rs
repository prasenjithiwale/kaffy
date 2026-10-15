//! State of the broker health screen.

use kaffy_kafka::ClusterInfo;
use ratatui::widgets::TableState;

use crate::groups::Refresh;

#[derive(Debug, Clone, Default)]
pub struct BrokersView {
    pub cluster: Option<ClusterInfo>,
    /// Selection in the partition problems table.
    pub table: TableState,
    pub refresh: Refresh,
}

impl BrokersView {
    pub(crate) fn clamp_selection(&mut self) {
        let len = self.cluster.as_ref().map_or(0, |c| c.problems().len());
        let selected = self.table.selected().unwrap_or(0);
        self.table.select(Some(selected.min(len.saturating_sub(1))));
    }
}
