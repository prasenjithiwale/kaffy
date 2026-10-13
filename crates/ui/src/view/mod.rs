mod config;
mod detail;
mod dialog;
mod groups;
mod messages;
mod topics;

use ratatui::Frame;
use ratatui::layout::Flex;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Color;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};

use crate::state::{AppState, Connection, InputKind, Screen};
use crate::theme;

pub fn render(frame: &mut Frame, state: &mut AppState) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    render_header(frame, header, state);
    match state.screen {
        Screen::Topics => topics::render(frame, body, state),
        Screen::Messages => messages::render(frame, body, state),
        Screen::Detail => detail::render(frame, body, state),
        Screen::Groups => groups::render_list(frame, body, state),
        Screen::GroupDetail => groups::render_detail(frame, body, state),
        Screen::TopicConfig => config::render(frame, body, state),
    }
    render_footer(frame, footer, state);
    dialog::render(frame, state);
    if state.show_help {
        render_help(frame, state);
    }
}

/// Keys of each screen: (key, what it does). The footer shows the first few.
fn keys(screen: Screen) -> &'static [(&'static str, &'static str)] {
    match screen {
        Screen::Topics => &[
            ("↑↓", "move"),
            ("⏎", "open"),
            ("/", "filter"),
            ("c", "config"),
            ("n", "new topic"),
            ("D", "delete"),
            ("T", "delete records"),
            ("2", "consumer groups"),
            ("?", "help"),
            ("q", "quit"),
            ("r", "refresh"),
        ],
        Screen::TopicConfig => &[
            ("↑↓", "move"),
            ("⏎", "change value"),
            ("u", "reset to inherited"),
            ("r", "refresh"),
            ("?", "help"),
            ("esc", "back"),
        ],
        Screen::Messages => &[
            ("↑↓", "move"),
            ("⏎", "detail"),
            ("space", "pause"),
            ("/", "filter"),
            ("p", "produce"),
            ("E", "edit & produce"),
            ("X", "replay"),
            ("x", "export"),
            ("?", "help"),
            ("esc", "back"),
            ("b / e", "read from the beginning / only new messages"),
            ("o / t", "jump to an offset / a time"),
            ("y / Y", "copy value / key"),
            ("r", "reload"),
            ("K", "tombstone for this key"),
            ("g / G", "first message / follow newest"),
        ],
        Screen::Detail => &[
            ("↑↓", "scroll"),
            ("y / Y", "copy value / key"),
            ("E", "edit & produce"),
            ("p", "produce"),
            ("K", "tombstone for this key"),
            ("?", "help"),
            ("esc", "back"),
        ],
        Screen::Groups => &[
            ("↑↓", "move"),
            ("⏎", "open"),
            ("R", "reset offsets"),
            ("/", "filter"),
            ("1", "topics"),
            ("r", "refresh"),
            ("?", "help"),
            ("q", "quit"),
        ],
        Screen::GroupDetail => &[
            ("↑↓", "move"),
            ("R", "reset offsets"),
            ("r", "refresh"),
            ("?", "help"),
            ("esc", "back"),
        ],
    }
}

fn render_help(frame: &mut Frame, state: &AppState) {
    let keys = keys(state.screen);
    let mut lines = vec![Line::raw("")];
    lines.extend(keys.iter().map(|(key, label)| {
        Line::from(vec![
            Span::styled(format!("  {key:<8}"), theme::key_hint()),
            Span::raw(*label),
        ])
    }));
    lines.push(Line::raw(""));
    lines.push(Line::from(vec![
        Span::styled("  1 / 2   ", theme::key_hint()),
        Span::raw("topics / consumer groups"),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  ctrl-c  ", theme::key_hint()),
        Span::raw("quit from anywhere"),
    ]));
    lines.push(Line::raw(""));
    lines.push(Line::styled("  Press any key to close", theme::dim()));

    let area = frame.area();
    let width = (lines.iter().map(Line::width).max().unwrap_or(30) as u16 + 6).min(area.width);
    let height = (lines.len() as u16 + 2).min(area.height);
    let [popup] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [popup] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(popup);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_style(theme::label())
                .title(Line::styled(" Keys ", theme::title())),
        ),
        popup,
    );
}

fn render_header(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut spans = vec![
        Span::styled(" kaffy ", theme::badge(theme::ACCENT)),
        Span::raw(" "),
        Span::styled("profile ", theme::dim()),
        Span::raw(state.profile.clone()),
        Span::raw(" "),
    ];
    if state.prod {
        spans.push(Span::styled(" PROD ", theme::badge(Color::Red)));
        spans.push(Span::raw(" "));
    }
    spans.push(if state.read_only {
        Span::styled(" READ-ONLY ", theme::badge(Color::Yellow))
    } else {
        Span::styled(" WRITE ", theme::badge(Color::Magenta))
    });
    spans.push(Span::styled("  │  ", theme::dim()));
    spans.push(match state.connection {
        Connection::Connecting => Span::styled("● connecting…", theme::warning()),
        Connection::Connected { brokers } => Span::styled(
            format!(
                "● connected · {brokers} broker{}",
                if brokers == 1 { "" } else { "s" }
            ),
            theme::ok(),
        ),
    });
    let tabs = Line::from(vec![
        tab(
            "1",
            "Topics",
            matches!(
                state.screen,
                Screen::Topics | Screen::Messages | Screen::Detail | Screen::TopicConfig
            ),
        ),
        Span::raw("  "),
        tab(
            "2",
            "Groups",
            matches!(state.screen, Screen::Groups | Screen::GroupDetail),
        ),
        Span::raw(" "),
    ])
    .right_aligned();
    frame.render_widget(tabs, area);
    if let Some(tail) = &state.tail {
        spans.push(Span::styled("  │  ", theme::dim()));
        spans.push(Span::styled("topic ", theme::dim()));
        spans.push(Span::styled(tail.topic.clone(), theme::title()));
    }
    frame.render_widget(Line::from(spans), area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &AppState) {
    if let Some(input) = &state.input {
        let filtering_messages = input.kind == InputKind::Filter && state.screen != Screen::Topics;
        let right_text: Line = match state.tail.as_ref().and_then(|t| t.filter_error()) {
            Some(error) if filtering_messages => Line::styled(format!("{error} "), theme::error()),
            _ if filtering_messages && input.buffer.is_empty() => Line::styled(
                "word  ~regex  key:  value:  h:  p:0,2  after:-15m  before:  !not ",
                theme::dim(),
            ),
            _ => Line::from(hints(&[("⏎", "apply"), ("esc", "cancel")])),
        };
        let [left, right] = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(right_text.width() as u16),
        ])
        .areas(area);
        let line = Line::from(vec![
            Span::styled(input.kind.prompt(), theme::key_hint()),
            Span::raw(input.buffer.clone()),
            Span::styled("▏", theme::key_hint()),
        ]);
        frame.render_widget(line, left);
        frame.render_widget(right_text.right_aligned(), right);
        return;
    }
    if let Some(flash) = &state.flash {
        let style = if flash.error {
            theme::error()
        } else {
            theme::ok()
        };
        frame.render_widget(
            Paragraph::new(Span::styled(flash.text.clone(), style)),
            area,
        );
        return;
    }
    // The footer shows the common keys; ? lists them all.
    let keys = &keys(state.screen)[..keys(state.screen).len().min(10)];
    frame.render_widget(Line::from(hints(keys)), area);
}

fn tab(key: &str, label: &str, active: bool) -> Span<'static> {
    if active {
        Span::styled(format!(" {key} {label} "), theme::selected())
    } else {
        Span::styled(format!(" {key} {label} "), theme::dim())
    }
}

fn hints(keys: &[(&str, &str)]) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (key, label) in keys {
        spans.push(Span::styled(format!(" {key}"), theme::key_hint()));
        spans.push(Span::styled(format!(" {label} "), theme::dim()));
    }
    spans
}

/// A dimmed, centered message inside `area`.
fn placeholder(frame: &mut Frame, area: Rect, text: &str) {
    let [middle] = Layout::vertical([Constraint::Length(1)])
        .flex(ratatui::layout::Flex::Center)
        .areas(area);
    frame.render_widget(
        Line::styled(text.to_owned(), theme::dim()).centered(),
        middle,
    );
}
