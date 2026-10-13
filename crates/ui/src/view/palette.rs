use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, Padding, Paragraph};

use crate::palette::{self, MAX_RESULTS, Match};
use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, state: &AppState) {
    let Some(palette) = &state.palette else {
        return;
    };
    let results = palette::matches(state, &palette.query);
    let shown = &results[..results.len().min(MAX_RESULTS)];

    let area = frame.area();
    let width = area.width.saturating_sub(4).min(76);
    let height = (shown.len().max(1) as u16 + 5).min(area.height.saturating_sub(2));
    // Near the top, like an editor's palette, so results grow downwards.
    let [_, popup] =
        Layout::vertical([Constraint::Length(2), Constraint::Length(height)]).areas(area);
    let [popup] = Layout::horizontal([Constraint::Length(width)])
        .flex(ratatui::layout::Flex::Center)
        .areas(popup);

    frame.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme::border_focus())
        .padding(Padding::horizontal(1))
        .style(theme::raised())
        .title(Line::styled(" Go to ", theme::title()))
        .title(Line::styled(" ⏎ run · ↑↓ choose · esc close ", theme::faint()).right_aligned());
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let [input, rule, list] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(inner);
    let prompt = if palette.query.is_empty() {
        vec![
            Span::styled("› ", theme::key_hint()),
            Span::styled("▏", theme::key_hint()),
            Span::styled(
                "a command, or a topic, group or subject name",
                theme::faint(),
            ),
        ]
    } else {
        vec![
            Span::styled("› ", theme::key_hint()),
            Span::styled(palette.query.clone(), theme::title()),
            Span::styled("▏", theme::key_hint()),
        ]
    };
    frame.render_widget(Line::from(prompt), input);
    frame.render_widget(
        Line::styled("─".repeat(rule.width as usize), theme::border()),
        rule,
    );

    if shown.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled("Nothing matches", theme::dim())),
            list,
        );
        return;
    }
    for (i, result) in shown.iter().enumerate() {
        let row = Rect {
            y: list.y + i as u16,
            height: 1,
            ..list
        };
        if row.y >= list.bottom() {
            break;
        }
        render_row(frame, row, state, result, i == palette.selected);
    }
}

fn render_row(frame: &mut Frame, area: Rect, state: &AppState, result: &Match, selected: bool) {
    if selected {
        frame.render_widget(Block::new().style(theme::selected()), area);
    }
    let marker = if selected {
        Span::styled("▌ ", theme::accent())
    } else {
        Span::raw("  ")
    };
    let mut spans = vec![marker];
    // The matched characters stand out.
    for (i, c) in result.entry.text().chars().enumerate() {
        let style = if result.positions.contains(&i) {
            theme::accent().add_modifier(Modifier::BOLD)
        } else {
            theme::text()
        };
        spans.push(Span::styled(c.to_string(), style));
    }
    frame.render_widget(Line::from(spans), area);

    let key = result
        .entry
        .command()
        .and_then(|c| state.keymap.hint(c.id))
        .map(|k| Span::styled(format!(" {k} "), theme::key_chip()));
    let mut right = vec![Span::styled(
        format!("{}  ", result.entry.kind()),
        theme::faint(),
    )];
    right.extend(key);
    frame.render_widget(Line::from(right).right_aligned(), area);
}
