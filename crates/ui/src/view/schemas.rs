use kaffy_kafka::SchemaType;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Paragraph, Row};

use super::kit;

use super::detail::highlight_json;
use crate::schemas::{DiffLine, diff};
use crate::state::AppState;
use crate::theme;

pub fn render_list(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let view = &state.schemas;
    let visible = view.visible();
    let total = view.list.as_ref().map_or(0, Vec::len);
    let mut block = kit::counted("Schema Registry subjects", total);
    if !view.filter.is_empty() {
        block = block.title(kit::filter_note(&view.filter, visible.len()));
    }
    if visible.is_empty() {
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let text = match &view.list {
            None => "◌ Loading subjects…",
            Some(l) if l.is_empty() => "The registry has no subjects yet · n registers one",
            Some(_) => "No subjects match the filter · esc clears it",
        };
        super::placeholder(frame, inner, text);
        return;
    }
    let width = visible
        .iter()
        .map(|s| s.subject.len())
        .max()
        .unwrap_or(7)
        .max(7) as u16;
    let rows: Vec<Row> = visible
        .iter()
        .map(|s| {
            Row::new(vec![
                Cell::from(s.subject.clone()),
                Cell::from(s.schema_type.label()).style(type_style(s.schema_type)),
                kit::num(format!("v{}", s.version)),
                kit::num_styled(s.id.to_string(), theme::dim()),
            ])
        })
        .collect();
    let table = kit::table(
        rows,
        [
            Constraint::Length(width),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(10),
        ],
    )
    .header(kit::header([
        kit::head("SUBJECT", false),
        kit::head("TYPE", false),
        kit::head("LATEST VERSION", true),
        kit::head("SCHEMA ID", true),
    ]))
    .column_spacing(3)
    .block(block);
    frame.render_stateful_widget(table, area, &mut state.schemas.table);
}

pub fn render_subject(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(view) = state.subject.as_mut() else {
        return;
    };
    let [left, right] =
        Layout::horizontal([Constraint::Length(22), Constraint::Min(0)]).areas(area);

    let compat = view.compatibility.clone().unwrap_or_else(|| "…".to_owned());
    let versions = view.versions.clone().unwrap_or_default();
    let rows: Vec<Row> = versions
        .iter()
        .map(|v| {
            let id = view
                .schemas
                .get(v)
                .map(|s| format!("id {}", s.id))
                .unwrap_or_default();
            Row::new(vec![
                Cell::from(format!("v{v}")),
                Cell::from(id).style(theme::dim()),
            ])
        })
        .collect();
    let list = kit::table(rows, [Constraint::Length(4), Constraint::Fill(1)]).block(
        kit::panel("Versions").title_bottom(Line::from(vec![
            Span::styled(" compat ", theme::faint()),
            Span::styled(format!("{compat} "), theme::accent()),
        ])),
    );
    frame.render_stateful_widget(list, left, &mut view.table);

    let selected = view.selected_version();
    let schema = selected.and_then(|v| view.schemas.get(&v));
    let (title, lines): (String, Vec<Line>) = match (schema, view.diff) {
        (None, _) => (
            format!(" {} ", view.subject),
            vec![Line::styled("◌ Loading…", theme::dim())],
        ),
        (Some(s), false) => (
            format!(
                " {} · v{} · {} ",
                s.subject,
                s.version,
                s.schema_type.label()
            ),
            schema_lines(s.schema_type, &s.schema),
        ),
        (Some(s), true) => match view.previous_version().map(|p| view.schemas.get(&p)) {
            None => (
                format!(" {} · v{} is the first version ", s.subject, s.version),
                schema_lines(s.schema_type, &s.schema),
            ),
            Some(None) => (
                format!(" {} · v{} ", s.subject, s.version),
                vec![Line::styled("Loading the previous version…", theme::dim())],
            ),
            Some(Some(prev)) => (
                format!(
                    " {} · changes from v{} to v{} ",
                    s.subject, prev.version, s.version
                ),
                diff_lines(&diff(s.schema_type, &prev.schema, &s.schema), None),
            ),
        },
    };
    let max = lines.len().saturating_sub(1) as u16;
    view.scroll = view.scroll.min(max);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((view.scroll, 0))
            .block(kit::panel(title.trim().to_owned())),
        right,
    );
}

fn type_style(schema_type: SchemaType) -> Style {
    match schema_type {
        SchemaType::Avro => theme::special(),
        SchemaType::Json => theme::info(),
        SchemaType::Protobuf => theme::warning(),
    }
}

fn schema_lines(schema_type: SchemaType, schema: &str) -> Vec<Line<'static>> {
    let pretty = kaffy_schema::pretty_schema(schema_type, schema);
    match schema_type {
        SchemaType::Avro | SchemaType::Json => pretty.lines().map(highlight_json).collect(),
        SchemaType::Protobuf => pretty.lines().map(|l| Line::raw(l.to_owned())).collect(),
    }
}

/// Diff lines styled by sign. With `context`, unchanged lines further than
/// that from a change are folded away.
pub(super) fn diff_lines(diff: &[DiffLine], context: Option<usize>) -> Vec<Line<'static>> {
    let changed: Vec<usize> = diff
        .iter()
        .enumerate()
        .filter(|(_, l)| l.sign != ' ')
        .map(|(i, _)| i)
        .collect();
    let keep = |i: usize| match context {
        None => true,
        Some(n) => changed.iter().any(|&c| c.abs_diff(i) <= n),
    };
    let mut out = Vec::new();
    let mut folded = false;
    for (i, line) in diff.iter().enumerate() {
        if !keep(i) {
            if !folded {
                out.push(Line::styled("  …", theme::dim()));
                folded = true;
            }
            continue;
        }
        folded = false;
        let style: Style = match line.sign {
            '+' => theme::ok(),
            '-' => theme::error(),
            _ => theme::faint(),
        };
        out.push(Line::from(vec![Span::styled(
            format!("{} {}", line.sign, line.text),
            style,
        )]));
    }
    if changed.is_empty() {
        out = vec![Line::styled("No changes.", theme::dim())];
    }
    out
}
