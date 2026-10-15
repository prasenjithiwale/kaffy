use kaffy_kafka::TopicInfo;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, HighlightSpacing, Row, Table};

use super::kit;
use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let visible = state.visible_topics();
    let total = state.topics.as_ref().map_or(0, Vec::len);

    let mut block = kit::counted("Topics", total);
    if !state.topic_scope.is_empty() {
        block = block.title(kit::scope_note(&state.topic_scope));
    }
    if !state.topic_filter.is_empty() {
        block = block.title(kit::filter_note(&state.topic_filter, visible.len()));
    }

    if visible.is_empty() {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let text = match &state.topics {
            None => "◌ Loading topics…",
            Some(t) if t.is_empty() && !state.topic_scope.is_empty() => {
                "No topics match this profile's topics patterns"
            }
            Some(t) if t.is_empty() => "The cluster has no topics yet · n creates one",
            Some(_) => "No topics match the filter · esc clears it",
        };
        super::placeholder(frame, inner, text);
        return;
    }

    let name_width = visible
        .iter()
        .map(|t| t.name.len() + if t.is_internal() { 10 } else { 0 })
        .max()
        .unwrap_or(0)
        .max(10) as u16;
    let rows: Vec<Row> = visible
        .iter()
        .map(|topic| {
            let name = if topic.is_internal() {
                Line::from(vec![
                    Span::styled(topic.name.clone(), theme::dim()),
                    Span::styled("  internal", theme::faint()),
                ])
            } else {
                Line::styled(topic.name.clone(), theme::text())
            };
            let urp = topic.under_replicated();
            let health = if urp > 0 {
                Line::styled(format!("▲ {urp} under-replicated"), theme::warning())
            } else {
                Line::styled("● healthy", theme::ok())
            };
            Row::new(vec![
                Cell::from(name),
                kit::num(topic.partitions.len().to_string()),
                kit::num(topic.replication_factor().to_string()),
                Cell::from(health),
            ])
        })
        .collect();

    // Below the list, if there is room: the selected topic's partitions.
    let selected = state
        .topic_table
        .selected()
        .unwrap_or(0)
        .min(visible.len() - 1);
    let list_height = visible.len() as u16 + 4;
    let partitions = (area.height >= list_height + 8).then(|| partitions_panel(visible[selected]));
    let (list_area, partitions_area) = match partitions {
        Some(_) => {
            let [top, bottom] =
                Layout::vertical([Constraint::Length(list_height), Constraint::Min(0)]).areas(area);
            (top, Some(bottom))
        }
        None => (area, None),
    };

    let table = kit::table(
        rows,
        [
            Constraint::Length(name_width),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Fill(1),
        ],
    )
    .header(kit::header([
        kit::head("NAME", false),
        kit::head("PARTITIONS", true),
        kit::head("REPLICATION", true),
        kit::head("HEALTH", false),
    ]))
    .column_spacing(3)
    .block(block);

    frame.render_stateful_widget(table, list_area, &mut state.topic_table);
    if let (Some(panel), Some(panel_area)) = (partitions, partitions_area) {
        frame.render_widget(panel, panel_area);
    }
}

/// Leader, replicas and in-sync replicas of each partition.
fn partitions_panel(topic: &TopicInfo) -> Table<'static> {
    let ids = |ids: &[i32]| {
        ids.iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let rows: Vec<Row> = topic
        .partitions
        .iter()
        .map(|p| {
            let missing: Vec<i32> = p
                .replicas
                .iter()
                .copied()
                .filter(|r| !p.isr.contains(r))
                .collect();
            let leader = if p.leader < 0 {
                kit::num_styled("none", theme::error())
            } else {
                kit::num(p.leader.to_string())
            };
            let status = if p.leader < 0 {
                Line::styled("✕ offline", theme::error())
            } else if missing.is_empty() {
                Line::styled("● in sync", theme::ok())
            } else {
                Line::styled(format!("▲ {} behind", ids(&missing)), theme::warning())
            };
            Row::new(vec![
                kit::num_styled(p.id.to_string(), theme::partition(p.id)),
                leader,
                Cell::from(ids(&p.replicas)),
                Cell::from(ids(&p.isr)).style(theme::dim()),
                Cell::from(status),
            ])
        })
        .collect();
    Table::new(
        kit::striped(rows),
        [
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Fill(1),
        ],
    )
    .column_spacing(3)
    // Indented like the list above, which leaves room for its selection marker.
    .highlight_spacing(HighlightSpacing::Always)
    .highlight_symbol("  ")
    .header(kit::header([
        kit::head("PARTITION", true),
        kit::head("LEADER", true),
        kit::head("REPLICAS", false),
        kit::head("IN SYNC", false),
        kit::head("STATUS", false),
    ]))
    .block(kit::panel(format!("{} · partitions", topic.name)))
}
