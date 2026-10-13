use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, TableState};

use super::detail::highlight_json;
use super::kit;

use crate::fmt;
use crate::state::{AppState, BUFFER_CAPACITY};
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(tail) = state.tail.as_mut() else {
        return;
    };
    let visible = tail.visible();

    let status = if tail.paused {
        Span::styled(" ❚❚ PAUSED ", theme::warning().add_modifier(Modifier::BOLD))
    } else if tail.follow {
        Span::styled(" ● LIVE ", theme::ok().add_modifier(Modifier::BOLD))
    } else {
        Span::styled(" ◼ SCROLLED ", theme::dim())
    };
    let partitions = match tail.partitions {
        None => "… partitions".to_owned(),
        Some(1) => "1 partition".to_owned(),
        Some(n) => format!("{n} partitions"),
    };
    let title = format!(" {} ", tail.topic);
    let info = info_title(
        &[
            partitions,
            format!("from {}", fmt::start_at(tail.start)),
            format!("{} buffered (max {BUFFER_CAPACITY})", tail.rows.len()),
            format!("{} received", tail.received),
        ],
        match tail.filter_error() {
            Some(error) => Some((format!("filter: {error}"), theme::error())),
            None if !tail.filter.is_empty() => Some((
                format!("filter: {} · {} match", tail.filter, visible.len()),
                theme::warning(),
            )),
            None => None,
        },
        status,
        // Borders, the topic title and a gap between the two titles.
        (area.width as usize).saturating_sub(title.chars().count() + 3),
    );

    let block = kit::panel(tail.topic.clone()).title(info.right_aligned());

    if visible.is_empty() {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let text = if tail.rows.is_empty() {
            "◌ Waiting for messages…"
        } else {
            "No messages match the filter · esc clears it"
        };
        super::placeholder(frame, inner, text);
        return;
    }

    // Build only the rows that fit: the buffer can hold thousands.
    let height = area.height.saturating_sub(3) as usize; // borders + header row
    let selected = tail.selected_position(&visible).unwrap_or(0);
    if selected < tail.scroll {
        tail.scroll = selected;
    } else if height > 0 && selected >= tail.scroll + height {
        tail.scroll = selected + 1 - height;
    }
    tail.scroll = tail.scroll.min(visible.len().saturating_sub(height.max(1)));
    let window = &visible[tail.scroll..(tail.scroll + height).min(visible.len())];

    let rows_in_window = || window.iter().map(|&i| &tail.rows[i]);
    let offset_width = rows_in_window()
        .map(|r| r.record.offset.to_string().len())
        .max()
        .unwrap_or(0)
        .max(6) as u16;
    let key_width = rows_in_window()
        .map(|r| r.key_preview.chars().count())
        .max()
        .unwrap_or(0)
        .clamp(3, 24) as u16;

    let rows: Vec<Row> = rows_in_window()
        .map(|row| {
            let record = &row.record;
            let key = match &record.key {
                None => Cell::from("∅").style(theme::faint()),
                Some(_) => Cell::from(row.key_preview.as_str()).style(theme::info()),
            };
            let value = match &record.value {
                None => {
                    Cell::from("∅ tombstone").style(theme::faint().add_modifier(Modifier::ITALIC))
                }
                Some(_) if row.value_preview.starts_with(['{', '[']) => {
                    Cell::from(highlight_json(&row.value_preview))
                }
                Some(_) => Cell::from(row.value_preview.as_str()),
            };
            let timestamp = match record.timestamp_ms.map(fmt::timestamp) {
                // Date in a fainter tone than the time, which changes more.
                Some(ts) => match ts.split_once(' ') {
                    Some((date, time)) => Line::from(vec![
                        Span::styled(format!("{date} "), theme::faint()),
                        Span::styled(time.to_owned(), theme::dim()),
                    ]),
                    None => Line::styled(ts, theme::dim()),
                },
                None => Line::raw(""),
            };
            Row::new(vec![
                kit::num_styled(
                    record.partition.to_string(),
                    theme::partition(record.partition),
                ),
                kit::num(record.offset.to_string()),
                Cell::from(timestamp),
                key,
                value,
            ])
        })
        .collect();

    let table = kit::table(
        rows,
        [
            Constraint::Length(3),
            Constraint::Length(offset_width),
            Constraint::Length(23),
            Constraint::Length(key_width),
            Constraint::Fill(1),
        ],
    )
    .header(kit::header([
        kit::head("P", true),
        kit::head("OFFSET", true),
        kit::head("TIMESTAMP", false),
        kit::head("KEY", false),
        kit::head("VALUE", false),
    ]))
    .block(block);

    let mut table_state = TableState::default().with_selected(selected - tail.scroll);
    frame.render_stateful_widget(table, area, &mut table_state);
}

/// The right-hand title: stats, filter and status, shortened to fit `width` so it
/// never covers the topic name. Stats are dropped from the end first, then the
/// filter text is truncated; the status is always shown.
fn info_title(
    stats: &[String],
    filter: Option<(String, Style)>,
    status: Span<'static>,
    width: usize,
) -> Line<'static> {
    let stats_span = |n: usize| match n {
        0 => Span::raw(""),
        n => Span::styled(format!(" {} ", stats[..n].join(" · ")), theme::dim()),
    };
    let filter_width = filter
        .as_ref()
        .map_or(0, |(text, _)| text.chars().count() + 2);
    let mut kept = stats.len();
    while kept > 0 && stats_span(kept).width() + filter_width + status.width() > width {
        kept -= 1;
    }
    let mut spans = vec![stats_span(kept)];
    if let Some((text, style)) = filter {
        let room = width
            .saturating_sub(spans[0].width() + status.width() + 2)
            .max(1);
        let text = if text.chars().count() > room {
            let mut short: String = text.chars().take(room - 1).collect();
            short.push('…');
            short
        } else {
            text
        };
        spans.push(Span::styled(format!(" {text} "), style));
    }
    spans.push(status);
    Line::from(spans)
}
