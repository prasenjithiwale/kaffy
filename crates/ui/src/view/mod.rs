mod detail;
mod messages;
mod topics;

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Color;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::state::{AppState, Connection, Screen};
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
    }
    render_footer(frame, footer, state);
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
        Connection::Connected { brokers } => {
            Span::styled(format!("● connected · {brokers} brokers"), theme::ok())
        }
    });
    if let Some(tail) = &state.tail {
        spans.push(Span::styled("  │  ", theme::dim()));
        spans.push(Span::styled("topic ", theme::dim()));
        spans.push(Span::styled(tail.topic.clone(), theme::title()));
    }
    frame.render_widget(Line::from(spans), area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &AppState) {
    if let Some(input) = &state.input {
        let [left, right] =
            Layout::horizontal([Constraint::Min(0), Constraint::Length(22)]).areas(area);
        let line = Line::from(vec![
            Span::styled(input.kind.prompt(), theme::key_hint()),
            Span::raw(input.buffer.clone()),
            Span::styled("▏", theme::key_hint()),
        ]);
        frame.render_widget(line, left);
        frame.render_widget(
            Line::from(hints(&[("⏎", "apply"), ("esc", "cancel")])).right_aligned(),
            right,
        );
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
    let keys: &[(&str, &str)] = match state.screen {
        Screen::Topics => &[
            ("↑↓", "move"),
            ("⏎", "open"),
            ("/", "filter"),
            ("r", "refresh"),
            ("q", "quit"),
        ],
        Screen::Messages => &[
            ("↑↓", "move"),
            ("⏎", "detail"),
            ("space", "pause"),
            ("/", "filter"),
            ("b/e", "beginning/end"),
            ("o", "offset"),
            ("t", "time"),
            ("y/Y", "copy value/key"),
            ("esc", "back"),
        ],
        Screen::Detail => &[
            ("↑↓", "scroll"),
            ("y", "copy value"),
            ("Y", "copy key"),
            ("esc", "back"),
            ("q", "quit"),
        ],
    };
    frame.render_widget(Line::from(hints(keys)), area);
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
