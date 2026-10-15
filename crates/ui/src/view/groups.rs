use kaffy_kafka::GroupState;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Sparkline};

use super::kit;

use crate::groups::{HISTORY_LEN, REFRESH_EVERY};
use crate::state::AppState;
use crate::theme;

/// Samples shown in the inline trend column.
const TREND_LEN: usize = 24;

pub fn render_list(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let view = &state.groups;
    let visible = view.visible();
    let total = view.list.as_ref().map_or(0, Vec::len);

    let mut block = kit::counted("Consumer groups", total).title(kit::note(
        format!("↻ every {}s", REFRESH_EVERY.as_secs()),
        theme::faint(),
    ));
    if !view.scope.is_empty() {
        block = block.title(kit::scope_note(&view.scope));
    }
    if !view.filter.is_empty() {
        block = block.title(kit::filter_note(&view.filter, visible.len()));
    }
    if visible.is_empty() {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let text = match &view.list {
            None => "◌ Loading consumer groups…",
            Some(l) if l.is_empty() && !view.scope.is_empty() => {
                "No consumer groups match this profile's groups patterns"
            }
            Some(l) if l.is_empty() => "The cluster has no consumer groups yet",
            Some(_) => "No groups match the filter · esc clears it",
        };
        super::placeholder(frame, inner, text);
        return;
    }

    let name_width = visible
        .iter()
        .map(|g| g.name.len())
        .max()
        .unwrap_or(0)
        .max(5) as u16;
    let rows: Vec<Row> = visible
        .iter()
        .map(|group| {
            let samples = view.history.get(&group.name);
            let trend = samples
                .map(|s| inline_sparkline(s.iter().copied(), TREND_LEN))
                .unwrap_or_default();
            let rising = samples.is_some_and(|s| s.len() > 1 && s.back() > s.front());
            let lag = match group.lag {
                Some(lag) => kit::num_styled(thousands(lag), lag_style(lag, rising)),
                None => kit::num_styled("?", theme::faint()),
            };
            Row::new(vec![
                Cell::from(group.name.clone()),
                Cell::from(state_chip(&group.state)),
                kit::num(group.members.to_string()),
                Cell::from(group.topics.join(", ")).style(theme::dim()),
                lag,
                Cell::from(trend).style(if rising {
                    theme::warning()
                } else {
                    theme::accent()
                }),
            ])
        })
        .collect();
    let table = kit::table(
        rows,
        [
            Constraint::Length(name_width),
            Constraint::Length(13),
            Constraint::Length(7),
            Constraint::Fill(1),
            Constraint::Length(10),
            Constraint::Length(TREND_LEN as u16),
        ],
    )
    .header(kit::header([
        kit::head("GROUP", false),
        kit::head("STATE", false),
        kit::head("MEMBERS", true),
        kit::head("TOPICS", false),
        kit::head("LAG", true),
        kit::head("TREND", false),
    ]))
    .block(block);
    frame.render_stateful_widget(table, area, &mut state.groups.table);
}

pub fn render_detail(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(view) = state.group_detail.as_mut() else {
        return;
    };
    let Some(detail) = view.detail.as_ref() else {
        let block = kit::panel(view.name.clone());
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::placeholder(frame, inner, "◌ Loading offsets…");
        return;
    };

    let [top, bottom] = Layout::vertical([Constraint::Length(9), Constraint::Min(0)]).areas(area);
    let [stats_area, chart_area] =
        Layout::horizontal([Constraint::Length(46), Constraint::Min(0)]).areas(top);

    let total = detail.total_lag();
    let samples: Vec<u64> = state
        .groups
        .history
        .get(&view.name)
        .map(|s| s.iter().map(|&l| l.max(0) as u64).collect())
        .unwrap_or_default();
    let topics = view.topics();
    let members = if detail.members.is_empty() {
        vec![Line::from(vec![
            label("Members"),
            Span::styled("none", theme::dim()),
        ])]
    } else {
        detail
            .members
            .iter()
            .take(3)
            .enumerate()
            .map(|(i, m)| {
                let first = if i == 0 {
                    label("Members")
                } else {
                    Span::raw(" ".repeat(10))
                };
                Line::from(vec![
                    first,
                    Span::raw(format!("{} ({})", m.client_id, m.host)),
                ])
            })
            .chain((detail.members.len() > 3).then(|| {
                Line::from(vec![
                    Span::raw(" ".repeat(10)),
                    Span::styled(
                        format!("and {} more", detail.members.len() - 3),
                        theme::dim(),
                    ),
                ])
            }))
            .collect()
    };
    let mut lines = vec![
        Line::from(vec![label("State"), state_chip(&detail.state)]),
        Line::from(vec![
            label("Total lag"),
            Span::styled(thousands(total), lag_style(total, false)),
        ]),
        Line::from(vec![label("Topics"), Span::raw(topics.join(", "))]),
    ];
    lines.extend(members);
    if matches!(detail.state, GroupState::Empty) {
        lines.push(Line::raw(""));
        lines.push(Line::from(vec![
            Span::styled("No members, so offsets can be reset with ", theme::faint()),
            Span::styled("R", theme::key_hint()),
        ]));
    }
    frame.render_widget(
        Paragraph::new(lines).block(kit::panel(view.name.clone())),
        stats_area,
    );

    let chart_title = format!(
        "Total lag, last {} refreshes",
        samples.len().min(HISTORY_LEN)
    );
    let max = samples.iter().copied().max().unwrap_or(0);
    let min = samples.iter().copied().min().unwrap_or(0);
    // Plot above a floor just under the minimum, so changes on a large lag show.
    let floor = min.saturating_sub((max - min).max(max / 20).max(1));
    let plotted: Vec<u64> = samples.iter().map(|&s| s - floor).collect();
    let chart = Sparkline::default()
        .block(kit::panel(chart_title).title(kit::note(
            format!("{} – {}", thousands(min as i64), thousands(max as i64)),
            theme::faint(),
        )))
        .data(&plotted)
        .style(theme::accent());
    frame.render_widget(chart, chart_area);

    let max_lag = detail.partitions.iter().map(|p| p.lag()).max().unwrap_or(0);
    let rows: Vec<Row> = detail
        .partitions
        .iter()
        .map(|p| {
            let lag = p.lag();
            Row::new(vec![
                Cell::from(p.topic.clone()),
                kit::num_styled(p.partition.to_string(), theme::partition(p.partition)),
                kit::num(thousands(p.committed)),
                kit::num(thousands(p.end)),
                kit::num_styled(thousands(lag), lag_style(lag, false)),
                Cell::from(lag_bar(lag, max_lag, LAG_BAR)),
            ])
        })
        .collect();
    let topic_width = detail
        .partitions
        .iter()
        .map(|p| p.topic.len())
        .max()
        .unwrap_or(5)
        .max(5) as u16;
    let table = kit::table(
        rows,
        [
            Constraint::Length(topic_width),
            Constraint::Length(9),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(12),
            Constraint::Length(LAG_BAR as u16),
        ],
    )
    .header(kit::header([
        kit::head("TOPIC", false),
        kit::head("PARTITION", true),
        kit::head("COMMITTED", true),
        kit::head("END OFFSET", true),
        kit::head("LAG", true),
        kit::head("", false),
    ]))
    .block(kit::counted("Partitions", detail.partitions.len()));
    frame.render_stateful_widget(table, bottom, &mut view.table);
}

fn label(name: &str) -> Span<'static> {
    Span::styled(format!("{name:<10}"), theme::label())
}

/// The group's state with a glyph, so it reads without color too.
fn state_chip(state: &GroupState) -> Span<'static> {
    let (glyph, style) = match state {
        GroupState::Stable => ("●", theme::ok()),
        GroupState::Empty => ("○", theme::warning()),
        GroupState::Rebalancing => ("◐", theme::special()),
        GroupState::Dead | GroupState::Other(_) => ("✕", theme::faint()),
    };
    Span::styled(format!("{glyph} {}", state.label()), style)
}

fn lag_style(lag: i64, rising: bool) -> Style {
    if lag == 0 {
        theme::ok()
    } else if rising {
        theme::warning()
    } else {
        theme::text()
    }
}

/// Width of the per-partition lag bars.
const LAG_BAR: usize = 16;

/// `lag` as a bar scaled to `max`, on a faint track.
fn lag_bar(lag: i64, max: i64, width: usize) -> Line<'static> {
    if lag <= 0 || max <= 0 {
        return Line::styled("✓ caught up", theme::ok());
    }
    let filled = ((lag as f64 / max as f64) * width as f64).round().max(1.0) as usize;
    Line::from(vec![
        Span::styled("━".repeat(filled.min(width)), theme::accent()),
        Span::styled("─".repeat(width.saturating_sub(filled)), theme::faint()),
    ])
}

/// `1234567` → `1,234,567`.
pub(super) fn thousands(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

/// The last `width` samples as block characters, scaled between the window's
/// minimum and maximum so movement shows even when lag is large. A flat series
/// draws low when it is zero and mid-height otherwise.
fn inline_sparkline(
    samples: impl DoubleEndedIterator<Item = i64> + ExactSizeIterator,
    width: usize,
) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let skip = samples.len().saturating_sub(width);
    let window: Vec<i64> = samples.skip(skip).map(|v| v.max(0)).collect();
    let (min, max) = match (window.iter().min(), window.iter().max()) {
        (Some(&min), Some(&max)) => (min, max),
        _ => return String::new(),
    };
    window
        .iter()
        .map(|&v| {
            if max == min {
                if max == 0 { BARS[0] } else { BARS[3] }
            } else {
                BARS[(((v - min) as f64 / (max - min) as f64) * 7.0).round() as usize]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_thousands() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(thousands(-4_200), "-4,200");
    }

    #[test]
    fn sparkline_scales_to_window() {
        assert_eq!(inline_sparkline([0, 7, 14].into_iter(), 10), "▁▅█");
        assert_eq!(inline_sparkline([0, 0].into_iter(), 10), "▁▁");
        assert_eq!(
            inline_sparkline([1600, 1616, 1632].into_iter(), 10),
            "▁▅█",
            "small growth on a large lag still shows"
        );
        assert_eq!(inline_sparkline([40, 40].into_iter(), 10), "▄▄");
        assert_eq!(
            inline_sparkline((0..30).collect::<Vec<i64>>().into_iter(), 4),
            "▁▃▆█"
        );
    }
}
