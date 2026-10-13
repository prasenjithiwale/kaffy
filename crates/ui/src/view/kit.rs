//! Building blocks every screen draws with, so they look alike.

use ratatui::layout::{Alignment, Constraint};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Cell, Padding, Row, Table};

use crate::theme;

/// A rounded panel with a bold title.
pub fn panel(title: impl Into<String>) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme::border())
        .padding(Padding::horizontal(1))
        .title(Line::from(vec![
            Span::raw(" "),
            Span::styled(title.into(), theme::title()),
            Span::raw(" "),
        ]))
}

/// A panel titled `title · count`.
pub fn counted(title: impl Into<String>, count: usize) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(theme::border())
        .padding(Padding::horizontal(1))
        .title(Line::from(vec![
            Span::raw(" "),
            Span::styled(title.into(), theme::title()),
            Span::styled(format!(" {count} "), theme::dim()),
        ]))
}

/// Text for the right-hand side of a panel's top border.
pub fn note(text: impl Into<String>, style: Style) -> Line<'static> {
    Line::styled(format!(" {} ", text.into()), style).right_aligned()
}

/// The `filter: … · n match` note shown while a list is filtered.
pub fn filter_note(filter: &str, matches: usize) -> Line<'static> {
    Line::from(vec![
        Span::styled(" filter ", theme::dim()),
        Span::styled(filter.to_owned(), theme::warning()),
        Span::styled(format!(" · {matches} match "), theme::dim()),
    ])
    .right_aligned()
}

/// Column headings.
pub fn header<'a>(cells: impl IntoIterator<Item = Cell<'a>>) -> Row<'a> {
    Row::new(cells).style(theme::header_row())
}

/// A heading cell; `right` aligns it with a numeric column.
pub fn head(text: &str, right: bool) -> Cell<'static> {
    let line = Line::from(text.to_owned());
    Cell::from(if right { line.right_aligned() } else { line })
}

/// A right-aligned number or other short value.
pub fn num(text: impl Into<String>) -> Cell<'static> {
    Cell::from(Line::from(text.into()).alignment(Alignment::Right))
}

pub fn num_styled(text: impl Into<String>, style: Style) -> Cell<'static> {
    Cell::from(Line::styled(text.into(), style).alignment(Alignment::Right))
}

/// Alternate rows get a faint background.
pub fn striped(rows: Vec<Row<'_>>) -> Vec<Row<'_>> {
    rows.into_iter()
        .enumerate()
        .map(|(i, row)| {
            if i % 2 == 1 {
                row.style(theme::zebra())
            } else {
                row
            }
        })
        .collect()
}

/// A selectable table with the shared look.
pub fn table<'a>(rows: Vec<Row<'a>>, widths: impl IntoIterator<Item = Constraint>) -> Table<'a> {
    Table::new(striped(rows), widths)
        .column_spacing(2)
        .row_highlight_style(theme::selected())
        .highlight_symbol(Span::styled("▌ ", theme::accent()))
}

/// Keys and labels as chips: `⏎ open  / filter`.
pub fn key_hints(keys: &[(&str, &str)]) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (key, label) in keys {
        spans.push(Span::styled(format!(" {key} "), theme::key_chip()));
        spans.push(Span::styled(format!(" {label}  "), theme::dim()));
    }
    spans
}
