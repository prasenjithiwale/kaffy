mod brokers;
mod config;
mod detail;
mod dialog;
mod groups;
mod kit;
mod messages;
mod palette;
mod schemas;
mod topics;

use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Padding, Paragraph};

use crate::commands;
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
    palette::render(frame, state);
}

/// The keys bound to a command, as shown to the user: `↑↓` for movement,
/// otherwise the keys joined with ` / `.
fn shown_keys(state: &AppState, id: &str, all: bool) -> Option<String> {
    let map = &state.keymap;
    if id == "up" {
        if let (Some(up), Some(down)) = (map.hint("up"), map.hint("down")) {
            return Some(format!("{up}{down}"));
        }
    }
    let keys = map.keys(id);
    if keys.is_empty() {
        return None;
    }
    let shown: Vec<String> = if all {
        keys.iter().map(ToString::to_string).collect()
    } else {
        vec![keys[0].to_string()]
    };
    Some(shown.join(" / "))
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
    let screen = state.screen;
    let rows = |everywhere: bool| -> Vec<(String, &'static str)> {
        commands::on(screen)
            .filter(|c| (c.scope == commands::Scope::Everywhere) == everywhere && c.id != "down")
            .filter_map(|c| {
                let label = if c.id == "up" { "Move" } else { c.title };
                shown_keys(state, c.id, true).map(|keys| (keys, label))
            })
            .collect()
    };
    let (this_screen, everywhere) = (rows(false), rows(true));
    // Pairs and the tab keys share a row, so the overlay fits small terminals.
    const MERGED: [(&[&str], &str); 3] = [
        (&["page-up", "page-down"], "Page up / down"),
        (&["top", "bottom"], "First / last row (follow newest)"),
        (
            &["topics", "groups", "schemas", "brokers"],
            "Topics, groups, schemas, brokers",
        ),
    ];
    let first_key = |id: &str| state.keymap.hint(id);
    let mut everywhere: Vec<(String, &str)> = everywhere
        .into_iter()
        .filter(|(_, label)| {
            !MERGED.iter().any(|(ids, _)| {
                ids.iter()
                    .any(|id| commands::find(id).is_some_and(|c| c.title == *label))
            })
        })
        .collect();
    for (ids, label) in MERGED {
        let keys: Vec<String> = ids.iter().filter_map(|id| first_key(id)).collect();
        if !keys.is_empty() {
            everywhere.push((keys.join(" "), label));
        }
    }
    everywhere.push(("ctrl-c".to_owned(), "Quit from anywhere"));
    let column = |title: &str, rows: &[(String, &str)]| -> Vec<Line<'static>> {
        let key_width = rows
            .iter()
            .map(|(k, _)| k.chars().count())
            .max()
            .unwrap_or(4)
            + 2;
        let mut lines = vec![Line::styled(title.to_uppercase(), theme::header_row())];
        lines.extend(rows.iter().map(|(key, label)| {
            Line::from(vec![
                Span::styled(format!(" {key:<key_width$}"), theme::key_hint()),
                Span::styled(label.to_string(), theme::text()),
            ])
        }));
        lines
    };
    let left = column("This screen", &this_screen);
    let right = column("Everywhere", &everywhere);
    let footer = Line::styled(
        "Press any key to close · rebind keys in the config's [keys] table",
        theme::faint(),
    )
    .centered();

    let area = frame.area();
    let width_of = |lines: &[Line]| lines.iter().map(Line::width).max().unwrap_or(0) as u16;
    let (left_width, right_width) = (width_of(&left), width_of(&right));
    // Side by side when they fit, else one under the other.
    let side_by_side = left_width + right_width + 10 <= area.width;
    let (width, body_height) = if side_by_side {
        (
            (left_width + right_width + 8).max(footer.width() as u16 + 4),
            left.len().max(right.len()) as u16,
        )
    } else {
        (
            left_width.max(right_width).max(footer.width() as u16) + 4,
            (left.len() + right.len() + 1) as u16,
        )
    };
    let popup = centered(
        area,
        width.min(area.width),
        (body_height + 4).min(area.height),
    );
    frame.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme::border_focus())
        .padding(Padding::horizontal(1))
        .style(theme::raised())
        .title(Line::styled(" Keys ", theme::title()));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    let [body, _, foot] = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(inner);
    if side_by_side {
        let [l, _, r] = Layout::horizontal([
            Constraint::Length(left_width),
            Constraint::Length(4),
            Constraint::Min(0),
        ])
        .areas(body);
        frame.render_widget(Paragraph::new(left), l);
        frame.render_widget(Paragraph::new(right), r);
    } else {
        let mut lines = left;
        lines.push(Line::raw(""));
        lines.extend(right);
        frame.render_widget(Paragraph::new(lines), body);
    }
    frame.render_widget(footer, foot);
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
    // The common keys, as many as fit; the help key always shows.
    // The palette and help keys always show, at the end.
    let mut help = Vec::new();
    for (id, label) in [("palette", "commands"), ("help", "keys")] {
        if let Some(key) = shown_keys(state, id, false) {
            help.extend(kit::key_hints(&[(&key, label)]));
        }
    }
    let help_width: usize = help.iter().map(Span::width).sum();
    let mut spans = Vec::new();
    let mut used = 0;
    // Top-level screens have nowhere to go back to.
    let top_level = matches!(
        state.screen,
        Screen::Topics | Screen::Groups | Screen::Schemas | Screen::Brokers
    );
    let footer = commands::on(state.screen).filter(|c| {
        c.footer && !matches!(c.id, "help" | "palette") && !(top_level && c.id == "back")
    });
    for command in footer {
        let Some(key) = shown_keys(state, command.id, false) else {
            continue;
        };
        let hint = kit::key_hints(&[(&key, command.label)]);
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
