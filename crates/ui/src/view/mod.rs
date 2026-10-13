mod brokers;
mod config;
mod detail;
mod dialog;
mod groups;
mod kit;
mod messages;
mod schemas;
mod topics;

use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Padding, Paragraph};

use crate::state::{AppState, Connection, InputKind, Screen};
use crate::theme;

pub fn render(frame: &mut Frame, state: &mut AppState) {
    frame.render_widget(Block::new().style(theme::base()), frame.area());
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
        Screen::Schemas => schemas::render_list(frame, body, state),
        Screen::SchemaDetail => schemas::render_subject(frame, body, state),
        Screen::Brokers => brokers::render(frame, body, state),
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
            ("?", "help"),
            ("q", "quit"),
            ("r", "refresh"),
        ],
        Screen::Schemas => &[
            ("↑↓", "move"),
            ("⏎", "open"),
            ("n", "new subject"),
            ("/", "filter"),
            ("r", "refresh"),
            ("?", "help"),
            ("q", "quit"),
        ],
        Screen::SchemaDetail => &[
            ("↑↓", "version"),
            ("d", "diff with previous"),
            ("e", "edit & register"),
            ("PgUp/PgDn", "scroll"),
            ("r", "refresh"),
            ("?", "help"),
            ("esc", "back"),
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
            ("r", "refresh"),
            ("?", "help"),
            ("q", "quit"),
        ],
        Screen::Brokers => &[
            ("↑↓", "move"),
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

/// The tabs, with the screens each one covers.
const TABS: [(&str, &str); 4] = [
    ("1", "Topics"),
    ("2", "Groups"),
    ("3", "Schemas"),
    ("4", "Brokers"),
];

fn active_tab(screen: Screen) -> &'static str {
    match screen {
        Screen::Topics | Screen::Messages | Screen::Detail | Screen::TopicConfig => "1",
        Screen::Groups | Screen::GroupDetail => "2",
        Screen::Schemas | Screen::SchemaDetail => "3",
        Screen::Brokers => "4",
    }
}

fn render_help(frame: &mut Frame, state: &AppState) {
    const EVERYWHERE: [(&str, &str); 4] = [
        ("1-4", "topics, consumer groups, schemas, brokers"),
        ("g / G", "first / last row"),
        ("PgUp/PgDn", "page"),
        ("ctrl-c", "quit from anywhere"),
    ];
    let keys = keys(state.screen);
    let key_width = keys
        .iter()
        .chain(&EVERYWHERE)
        .map(|(k, _)| k.chars().count())
        .max()
        .unwrap_or(4)
        + 2;
    let row = |(key, label): &(&str, &str)| {
        Line::from(vec![
            Span::styled(format!(" {key:<key_width$}"), theme::key_hint()),
            Span::styled(label.to_string(), theme::text()),
        ])
    };
    let section = |name: &str| Line::styled(name.to_uppercase(), theme::header_row());
    let mut lines = vec![section("This screen")];
    lines.extend(keys.iter().map(row));
    lines.push(Line::raw(""));
    lines.push(section("Everywhere"));
    lines.extend(EVERYWHERE.iter().map(row));
    lines.push(Line::raw(""));
    lines.push(Line::styled("Press any key to close", theme::faint()).centered());

    let area = frame.area();
    let width = (lines.iter().map(Line::width).max().unwrap_or(30) as u16 + 6).min(area.width);
    let height = (lines.len() as u16 + 2).min(area.height);
    let popup = centered(area, width, height);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).style(theme::raised()).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(theme::border_focus())
                .padding(Padding::horizontal(1))
                .title(Line::styled(" Keys ", theme::title())),
        ),
        popup,
    );
}

pub(super) fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let [popup] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [popup] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(popup);
    popup
}

fn render_header(frame: &mut Frame, area: Rect, state: &AppState) {
    frame.render_widget(Block::new().style(theme::bar()), area);
    let sep = || Span::styled("  ", theme::faint());
    let mut spans = vec![
        Span::styled(" ◆ kaffy ", theme::badge(theme::accent_color())),
        Span::raw(" "),
        Span::styled(state.profile.clone(), theme::title()),
        sep(),
    ];
    if state.prod {
        spans.push(Span::styled(" PROD ", theme::badge(theme::err_color())));
        spans.push(Span::raw(" "));
    }
    spans.push(if state.read_only {
        Span::styled("◌ READ-ONLY", theme::warning())
    } else {
        Span::styled("✎ WRITE", theme::special())
    });
    spans.push(sep());
    spans.push(match state.connection {
        Connection::Connecting => Span::styled("◌ connecting…", theme::warning()),
        Connection::Connected { brokers } => Span::styled(
            format!(
                "● connected · {brokers} broker{}",
                if brokers == 1 { "" } else { "s" }
            ),
            theme::ok(),
        ),
    });
    if let Some(tail) = &state.tail {
        spans.push(sep());
        spans.push(Span::styled("topic ", theme::dim()));
        spans.push(Span::styled(tail.topic.clone(), theme::accent()));
    }
    let left = Line::from(spans);

    let active = active_tab(state.screen);
    let tab_line = |compact: bool| {
        let mut spans = Vec::new();
        for (key, label) in TABS {
            let is_active = key == active;
            // Short of room, only the active tab keeps its name.
            let text = if compact && !is_active {
                format!(" {key} ")
            } else {
                format!(" {key} {label} ")
            };
            spans.push(if is_active {
                Span::styled(text, theme::badge(theme::accent_color()))
            } else {
                Span::styled(text, theme::dim())
            });
            spans.push(Span::raw(" "));
        }
        Line::from(spans).right_aligned()
    };
    let mut right = tab_line(false);
    if left.width() + right.width() + 2 > area.width as usize {
        right = tab_line(true);
    }
    frame.render_widget(right, area);
    frame.render_widget(left, area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &AppState) {
    frame.render_widget(Block::new().style(theme::bar()), area);
    if let Some(input) = &state.input {
        let filtering_messages = input.kind == InputKind::Filter && state.screen != Screen::Topics;
        let right_text: Line = match state.tail.as_ref().and_then(|t| t.filter_error()) {
            Some(error) if filtering_messages => Line::styled(format!("{error} "), theme::error()),
            _ if filtering_messages && input.buffer.is_empty() => Line::styled(
                "word  ~regex  key:  value:  h:  p:0,2  after:-15m  before:  !not ",
                theme::faint(),
            ),
            _ => Line::from(kit::key_hints(&[("⏎", "apply"), ("esc", "cancel")])),
        };
        let [left, right] = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(right_text.width() as u16),
        ])
        .areas(area);
        let line = Line::from(vec![
            Span::styled(
                format!(" {} ", input.kind.prompt().trim_end()),
                theme::key_chip(),
            ),
            Span::raw(" "),
            Span::styled(input.buffer.clone(), theme::text()),
            Span::styled("▏", theme::key_hint()),
        ]);
        frame.render_widget(line, left);
        frame.render_widget(right_text.right_aligned(), right);
        return;
    }

    // A message, if any, takes the right; keys fill what is left.
    let toast = state.flash.as_ref().map(|flash| {
        let (icon, style) = if flash.error {
            ("✗", theme::error())
        } else {
            ("✓", theme::ok())
        };
        Line::styled(format!("{icon} {} ", flash.text), style).right_aligned()
    });
    let toast_width = toast.as_ref().map_or(0, Line::width) as u16;
    let [left, right] =
        Layout::horizontal([Constraint::Min(0), Constraint::Length(toast_width)]).areas(area);
    if let Some(toast) = toast {
        frame.render_widget(toast, right);
    }
    // The common keys, as many as fit; ? lists them all.
    let help = kit::key_hints(&[("?", "keys")]);
    let help_width: usize = help.iter().map(Span::width).sum();
    let mut spans = Vec::new();
    let mut used = 0;
    for (key, label) in keys(state.screen).iter().filter(|(k, _)| *k != "?") {
        let hint = kit::key_hints(&[(key, label)]);
        let width: usize = hint.iter().map(Span::width).sum();
        if used + width + help_width > left.width as usize {
            break;
        }
        used += width;
        spans.extend(hint);
    }
    spans.extend(help);
    frame.render_widget(Line::from(spans), left);
}

/// A centered message inside `area`, in a muted style.
fn placeholder(frame: &mut Frame, area: Rect, text: &str) {
    let [middle] = Layout::vertical([Constraint::Length(1)])
        .flex(Flex::Center)
        .areas(area);
    frame.render_widget(
        Line::styled(text.to_owned(), theme::dim()).centered(),
        middle,
    );
}
