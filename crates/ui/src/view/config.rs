use kaffy_kafka::ConfigSource;
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Cell, Row, Table};

use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(view) = state.topic_config.as_mut() else {
        return;
    };
    let block = Block::bordered().title(Line::styled(
        format!(" {} · config ", view.topic),
        theme::title(),
    ));
    let Some(entries) = &view.entries else {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::placeholder(frame, inner, "Loading config…");
        return;
    };
    let overrides = entries
        .iter()
        .filter(|e| e.source == ConfigSource::Topic)
        .count();
    let block = block.title(
        Line::styled(
            format!(" {overrides} set on this topic, the rest inherited "),
            theme::dim(),
        )
        .right_aligned(),
    );
    let name_width = entries.iter().map(|e| e.name.len()).max().unwrap_or(10) as u16;
    let rows: Vec<Row> = entries
        .iter()
        .map(|e| {
            let (source, style) = match e.source {
                ConfigSource::Topic => ("topic", theme::warning()),
                ConfigSource::Broker => ("broker", theme::dim()),
                ConfigSource::Default => ("default", theme::dim()),
                ConfigSource::Unknown => ("?", theme::dim()),
            };
            let value = match (&e.value, e.sensitive) {
                (_, true) => "(sensitive)".to_owned(),
                (Some(v), _) => v.clone(),
                (None, _) => "(none)".to_owned(),
            };
            let name = if e.read_only {
                format!("{} (read-only)", e.name)
            } else {
                e.name.clone()
            };
            Row::new(vec![
                Cell::from(name).style(if e.source == ConfigSource::Topic {
                    theme::warning()
                } else {
                    Default::default()
                }),
                Cell::from(value),
                Cell::from(source).style(style),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(name_width + 12),
            Constraint::Fill(1),
            Constraint::Length(8),
        ],
    )
    .header(Row::new(["Setting", "Value", "From"]).style(theme::header_row()))
    .column_spacing(2)
    .row_highlight_style(theme::selected())
    .highlight_symbol("▶ ")
    .block(block);
    frame.render_stateful_widget(table, area, &mut view.table);
}
