use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};

use super::kit;

use crate::fmt;
use crate::state::AppState;
use crate::theme;

const LABEL_WIDTH: usize = 11;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(detail) = state.detail.as_mut() else {
        return;
    };
    let record = &detail.record;

    let mut lines: Vec<Line> = vec![
        field("Topic", record.topic.clone()),
        field("Partition", record.partition.to_string()),
        field("Offset", record.offset.to_string()),
        field(
            "Timestamp",
            record
                .timestamp_ms
                .map(|ms| format!("{}  ({ms})", fmt::timestamp(ms)))
                .unwrap_or_else(|| "none".to_owned()),
        ),
    ];

    match &detail.key {
        Some(key) => {
            let mut key_lines = key.text.lines();
            let first = key_lines.next().unwrap_or_default().to_owned();
            lines.push(Line::from(vec![
                label("Key"),
                Span::styled(first, theme::info()),
                Span::styled(format!("  ({})", key.label), theme::faint()),
            ]));
            lines.extend(key_lines.map(|l| indented(l.to_owned())));
        }
        None => lines.push(Line::from(vec![
            label("Key"),
            Span::styled("∅ null", theme::dim()),
        ])),
    }

    if record.headers.is_empty() {
        lines.push(Line::from(vec![
            label("Headers"),
            Span::styled("none", theme::dim()),
        ]));
    } else {
        for (i, header) in record.headers.iter().enumerate() {
            let value = header
                .value
                .as_deref()
                .map(|v| kaffy_schema::preview(v, None, 200))
                .unwrap_or_else(|| "∅".to_owned());
            let first = if i == 0 {
                label("Headers")
            } else {
                Span::raw(" ".repeat(LABEL_WIDTH))
            };
            lines.push(Line::from(vec![
                first,
                Span::styled(header.key.clone(), theme::json::key()),
                Span::styled(" = ", theme::dim()),
                Span::raw(value),
            ]));
        }
    }

    lines.push(Line::raw(""));
    match &detail.value {
        Some(value) => {
            let size = record.value.as_ref().map_or(0, Vec::len);
            lines.push(Line::from(vec![
                Span::styled("VALUE", theme::header_row()),
                Span::styled(format!("  ({}, {size} bytes)", value.label), theme::faint()),
            ]));
            lines.push(Line::raw(""));
            if value.json {
                lines.extend(value.text.lines().map(highlight_json));
            } else {
                lines.extend(value.text.lines().map(|l| Line::raw(l.to_owned())));
            }
        }
        None => lines.push(Line::from(vec![
            Span::styled("VALUE", theme::header_row()),
            Span::styled("  ∅ null (tombstone)", theme::faint()),
        ])),
    }

    // Keep the last line reachable but stop scrolling into empty space.
    let max_scroll = lines.len().saturating_sub(1) as u16;
    detail.scroll = detail.scroll.min(max_scroll);

    let block = kit::panel(format!(
        "{} · p{} · offset {}",
        record.topic, record.partition, record.offset
    ));
    let paragraph = Paragraph::new(Text::from(lines))
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((detail.scroll, 0));
    frame.render_widget(paragraph, area);
}

fn label(name: &str) -> Span<'static> {
    Span::styled(format!("{name:<LABEL_WIDTH$}"), theme::label())
}

fn field(name: &str, value: String) -> Line<'static> {
    Line::from(vec![label(name), Span::raw(value)])
}

fn indented(text: String) -> Line<'static> {
    Line::from(vec![Span::raw(" ".repeat(LABEL_WIDTH)), Span::raw(text)])
}

/// Colors one line of pretty-printed JSON: keys, strings, numbers, literals.
pub(super) fn highlight_json(line: &str) -> Line<'static> {
    let mut spans = Vec::new();
    let mut plain = String::new();
    let mut chars = line.char_indices().peekable();

    let flush = |plain: &mut String, spans: &mut Vec<Span<'static>>| {
        if !plain.is_empty() {
            spans.push(Span::raw(std::mem::take(plain)));
        }
    };

    while let Some((start, c)) = chars.next() {
        match c {
            '"' => {
                let mut end = line.len();
                let mut escaped = false;
                for (i, c) in chars.by_ref() {
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if c == '"' {
                        end = i + 1;
                        break;
                    }
                }
                let is_key = line[end..].trim_start().starts_with(':');
                let style: Style = if is_key {
                    theme::json::key()
                } else {
                    theme::json::string()
                };
                flush(&mut plain, &mut spans);
                spans.push(Span::styled(line[start..end].to_owned(), style));
            }
            '-' | '0'..='9' | 't' | 'f' | 'n' => {
                let mut end = start + c.len_utf8();
                while let Some(&(i, c)) = chars.peek() {
                    if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+') {
                        end = i + c.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                let style = if c.is_ascii_alphabetic() {
                    theme::json::literal()
                } else {
                    theme::json::number()
                };
                flush(&mut plain, &mut spans);
                spans.push(Span::styled(line[start..end].to_owned(), style));
            }
            _ => plain.push(c),
        }
    }
    flush(&mut plain, &mut spans);
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn styled(line: &Line) -> Vec<(String, Style)> {
        line.spans
            .iter()
            .map(|s| (s.content.to_string(), s.style))
            .collect()
    }

    #[test]
    fn highlights_json_tokens() {
        let spans = styled(&highlight_json(
            r#"  "a\"b": "x:y", "n": -1.5e3, "t": true"#,
        ));
        assert!(spans.contains(&(r#""a\"b""#.to_owned(), theme::json::key())));
        assert!(spans.contains(&(r#""x:y""#.to_owned(), theme::json::string())));
        assert!(spans.contains(&("-1.5e3".to_owned(), theme::json::number())));
        assert!(spans.contains(&("true".to_owned(), theme::json::literal())));
        let text: String = spans.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(text, r#"  "a\"b": "x:y", "n": -1.5e3, "t": true"#);
    }
}
