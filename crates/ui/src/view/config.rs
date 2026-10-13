use kaffy_kafka::ConfigSource;
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row};

use super::kit;
use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(view) = state.topic_config.as_mut() else {
        return;
    };
    let block = kit::panel(format!("{} · config", view.topic));
    let Some(entries) = &view.entries else {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::placeholder(frame, inner, "◌ Loading config…");
        return;
    };
    let overrides = entries
        .iter()
        .filter(|e| e.source == ConfigSource::Topic)
        .count();
    let block = block.title(kit::note(
        format!("{overrides} set on this topic, the rest inherited"),
        theme::faint(),
    ));
    let name_width = entries.iter().map(|e| e.name.len()).max().unwrap_or(10) as u16;
    let rows: Vec<Row> = entries
        .iter()
        .map(|e| {
            let (source, style) = match e.source {
                ConfigSource::Topic => ("● topic", theme::warning()),
                ConfigSource::Broker => ("broker", theme::dim()),
                ConfigSource::Default => ("default", theme::faint()),
                ConfigSource::Unknown => ("?", theme::faint()),
            };
            let value = match (&e.value, e.sensitive) {
                (_, true) => "(sensitive)".to_owned(),
                (Some(v), _) => v.clone(),
                (None, _) => "(none)".to_owned(),
            };
            let name_style = if e.source == ConfigSource::Topic {
                theme::warning()
            } else {
                theme::text()
            };
            let mut name = vec![Span::styled(e.name.clone(), name_style)];
            if e.read_only {
                name.push(Span::styled("  read-only", theme::faint()));
            }
            let value_style = if e.source == ConfigSource::Topic {
                theme::text()
            } else {
                theme::dim()
            };
            Row::new(vec![
                Cell::from(Line::from(name)),
                Cell::from(value).style(value_style),
                Cell::from(source).style(style),
            ])
        })
        .collect();
    let table = kit::table(
        rows,
        [
            Constraint::Length(name_width + 12),
            Constraint::Fill(1),
            Constraint::Length(8),
        ],
    )
    .header(kit::header([
        kit::head("SETTING", false),
        kit::head("VALUE", false),
        kit::head("FROM", false),
    ]))
    .block(block);
    frame.render_stateful_widget(table, area, &mut view.table);
}
