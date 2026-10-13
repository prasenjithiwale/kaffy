use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Cell, Row, Table};

use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let visible = state.visible_topics();
    let total = state.topics.as_ref().map_or(0, Vec::len);

    let mut block =
        Block::bordered().title(Line::styled(format!(" Topics ({total}) "), theme::title()));
    if !state.topic_filter.is_empty() {
        block = block.title(
            Line::styled(
                format!(" filter: {} · {} match ", state.topic_filter, visible.len()),
                theme::warning(),
            )
            .right_aligned(),
        );
    }

    if visible.is_empty() {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let text = match &state.topics {
            None => "Loading topics…",
            Some(t) if t.is_empty() => "The cluster has no topics",
            Some(_) => "No topics match the filter",
        };
        super::placeholder(frame, inner, text);
        return;
    }

    let name_width = visible
        .iter()
        .map(|t| t.name.len())
        .max()
        .unwrap_or(0)
        .max(10) as u16;
    let rows: Vec<Row> = visible
        .iter()
        .map(|topic| {
            let name_style = if topic.is_internal() {
                theme::dim()
            } else {
                Default::default()
            };
            let urp = topic.under_replicated();
            let urp_cell = if urp > 0 {
                Cell::from(urp.to_string()).style(theme::error())
            } else {
                Cell::from("0").style(theme::dim())
            };
            Row::new(vec![
                Cell::from(topic.name.clone()).style(name_style),
                Cell::from(topic.partitions.len().to_string()),
                Cell::from(topic.replication_factor().to_string()),
                urp_cell,
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(name_width),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(16),
        ],
    )
    .header(
        Row::new(["Name", "Partitions", "Replication", "Under-replicated"])
            .style(theme::header_row()),
    )
    .column_spacing(3)
    .row_highlight_style(theme::selected())
    .highlight_symbol("▶ ")
    .block(block);

    frame.render_stateful_widget(table, area, &mut state.topic_table);
}
