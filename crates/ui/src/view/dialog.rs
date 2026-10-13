use kaffy_kafka::{Payload, ResetTarget};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Clear, Gauge, Paragraph, Wrap};

use super::groups::thousands;
use crate::dialog::{Confirm, Dialog};
use crate::fmt;
use crate::state::AppState;
use crate::theme;

/// Rows of a plan shown before the rest are summarized.
const MAX_ROWS: usize = 12;

pub fn render(frame: &mut Frame, state: &AppState) {
    let Some(dialog) = &state.dialog else {
        return;
    };
    let (title, mut lines, footer) = match dialog {
        Dialog::Choice { title, options, .. } => {
            let mut lines = vec![Line::raw("")];
            lines.extend(options.iter().map(|(key, label)| {
                Line::from(vec![
                    Span::styled(format!("  {key}  "), theme::key_hint()),
                    Span::raw(label.clone()),
                ])
            }));
            (title.clone(), lines, hint(&[("esc", "cancel")]))
        }
        Dialog::Pending { title, .. } => (
            title.clone(),
            vec![
                Line::raw(""),
                Line::styled("Waiting for the cluster…", theme::dim()),
            ],
            Line::raw(""),
        ),
        Dialog::ResetPlan { plan, confirm } => {
            let target = match plan.target {
                ResetTarget::Earliest => "the earliest offset".to_owned(),
                ResetTarget::Latest => "the latest offset".to_owned(),
                ResetTarget::Timestamp(ms) => {
                    format!("the first message at or after {}", fmt::timestamp(ms))
                }
                ResetTarget::Offset(n) => format!("offset {n} (clamped to each partition)"),
            };
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("Move to ", theme::dim()),
                    Span::raw(target),
                ]),
                Line::raw(""),
                Line::styled(
                    format!(
                        "{:<24} {:>4} {:>12} {:>12} {:>12}",
                        "Topic", "P", "Current", "New", "Lag after"
                    ),
                    theme::header_row(),
                ),
            ];
            let mut reprocess = 0i64;
            let mut skip = 0i64;
            for (i, row) in plan.rows.iter().enumerate() {
                let current = row.current.unwrap_or(row.new);
                if row.new < current {
                    reprocess += current - row.new;
                } else {
                    skip += row.new - current;
                }
                if i < MAX_ROWS {
                    lines.push(Line::raw(format!(
                        "{:<24} {:>4} {:>12} {:>12} {:>12}",
                        truncate(&row.topic, 24),
                        row.partition,
                        row.current.map_or_else(|| "none".to_owned(), thousands),
                        thousands(row.new),
                        thousands((row.end - row.new).max(0)),
                    )));
                }
            }
            if plan.rows.len() > MAX_ROWS {
                lines.push(Line::styled(
                    format!("… and {} more partitions", plan.rows.len() - MAX_ROWS),
                    theme::dim(),
                ));
            }
            lines.push(Line::raw(""));
            let effect = match (reprocess, skip) {
                (0, 0) => "No change: the group is already there.".to_owned(),
                (r, 0) => format!("Consumers will process {} message(s) again.", thousands(r)),
                (0, s) => format!("Consumers will skip {} message(s).", thousands(s)),
                (r, s) => format!(
                    "Consumers will process {} again and skip {}.",
                    thousands(r),
                    thousands(s)
                ),
            };
            lines.push(Line::styled(effect, theme::warning()));
            (
                format!("Reset offsets of {}", plan.group),
                lines,
                confirm_line(confirm, "reset"),
            )
        }
        Dialog::ReplayPlan {
            plan,
            filter,
            confirm,
        } => {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("From ", theme::dim()),
                    Span::raw(plan.source.clone()),
                    Span::styled("  to ", theme::dim()),
                    Span::raw(plan.target.clone()),
                ]),
                Line::raw(""),
                Line::styled(
                    format!(
                        "{:>9} {:>14} {:>14} {:>12}",
                        "Partition", "From offset", "To offset", "Messages"
                    ),
                    theme::header_row(),
                ),
            ];
            for range in plan.ranges.iter().filter(|r| r.to > r.from).take(MAX_ROWS) {
                lines.push(Line::raw(format!(
                    "{:>9} {:>14} {:>14} {:>12}",
                    range.partition,
                    thousands(range.from),
                    thousands(range.to),
                    thousands(range.to - range.from)
                )));
            }
            lines.push(Line::raw(""));
            lines.push(Line::from(vec![
                Span::styled("Up to ", theme::dim()),
                Span::raw(format!("{} message(s)", thousands(plan.total()))),
                Span::styled(
                    match filter {
                        Some(_) => ", only those matching the filter",
                        None => ", all of them",
                    },
                    theme::dim(),
                ),
            ]));
            if let Some(filter) = filter {
                lines.push(Line::from(vec![
                    Span::styled("Filter  ", theme::dim()),
                    Span::styled(filter.clone(), theme::warning()),
                ]));
            }
            lines.push(Line::styled(
                if plan.keep_partitions {
                    "Each message keeps its partition. Keys, values and headers are copied; timestamps are new."
                } else {
                    "The topics have different partition counts, so the key decides the partition (like Java producers)."
                },
                theme::dim(),
            ));
            if plan.source == plan.target {
                lines.push(Line::styled(
                    "Replaying into the same topic: its consumers will see these messages twice.",
                    theme::warning(),
                ));
            }
            (
                format!("Replay into {}", plan.target),
                lines,
                confirm_line(confirm, "replay"),
            )
        }
        Dialog::ProduceReview { request, confirm } => {
            let field = |name: &str, value: String| {
                Line::from(vec![
                    Span::styled(format!("{name:<11}"), theme::label()),
                    Span::raw(value),
                ])
            };
            let mut lines = vec![
                field("Topic", request.topic.clone()),
                field(
                    "Partition",
                    request
                        .partition
                        .map_or_else(|| "chosen from the key".to_owned(), |p| p.to_string()),
                ),
                field(
                    "Key",
                    request.key.as_deref().map_or_else(
                        || "none".to_owned(),
                        |k| String::from_utf8_lossy(k).into_owned(),
                    ),
                ),
            ];
            for h in &request.headers {
                lines.push(field(
                    "Header",
                    format!(
                        "{} = {}",
                        h.key,
                        h.value
                            .as_deref()
                            .map(String::from_utf8_lossy)
                            .unwrap_or_default()
                    ),
                ));
            }
            let (format, body) = match &request.value {
                None => ("tombstone (no value)".to_owned(), String::new()),
                Some(Payload::Bytes(bytes)) => (
                    format!("{} bytes", bytes.len()),
                    kaffy_schema::render(bytes, None).text,
                ),
                Some(Payload::Avro { schema_id, json }) => (
                    format!("JSON encoded as Avro with schema {schema_id}"),
                    serde_json::to_string_pretty(json).expect("a Value always serializes"),
                ),
            };
            lines.push(field("Value", format));
            for line in body.lines().take(MAX_ROWS) {
                lines.push(Line::raw(format!("           {line}")));
            }
            if body.lines().count() > MAX_ROWS {
                lines.push(Line::styled("           …", theme::dim()));
            }
            (
                format!("Produce to {}", request.topic),
                lines,
                confirm_line(confirm, "produce"),
            )
        }
        Dialog::CreateTopic { spec, confirm } => {
            let field = |name: &str, value: String| {
                Line::from(vec![
                    Span::styled(format!("{name:<20}"), theme::label()),
                    Span::raw(value),
                ])
            };
            let lines = vec![
                field("Name", spec.name.clone()),
                field("Partitions", spec.partitions.to_string()),
                field("Replication factor", spec.replication.to_string()),
                Line::raw(""),
                Line::styled(
                    "Other settings use the broker defaults; change them later with c on the topic.",
                    theme::dim(),
                ),
            ];
            (
                format!("Create topic {}", spec.name),
                lines,
                confirm_line(confirm, "create"),
            )
        }
        Dialog::DeleteTopic {
            topic,
            partitions,
            confirm,
        } => {
            let lines = vec![
                Line::raw(""),
                Line::styled(
                    format!(
                        "This permanently deletes {topic} and all of its {partitions} partition(s)."
                    ),
                    theme::error(),
                ),
                Line::raw(
                    "Consumers and producers of the topic will start failing. This cannot be undone.",
                ),
            ];
            (
                format!("Delete topic {topic}"),
                lines,
                confirm_line(confirm, "delete it"),
            )
        }
        Dialog::DeleteRecords { plan, confirm } => {
            let mut lines = vec![Line::styled(
                format!(
                    "{:>9} {:>14} {:>14} {:>14}",
                    "Partition", "Starts at", "Will start at", "Deleted"
                ),
                theme::header_row(),
            )];
            for row in plan.rows.iter().take(MAX_ROWS) {
                lines.push(Line::raw(format!(
                    "{:>9} {:>14} {:>14} {:>14}",
                    row.partition,
                    thousands(row.low),
                    thousands(row.new_low),
                    thousands((row.new_low - row.low).max(0)),
                )));
            }
            if plan.rows.len() > MAX_ROWS {
                lines.push(Line::styled(
                    format!("… and {} more partitions", plan.rows.len() - MAX_ROWS),
                    theme::dim(),
                ));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                format!(
                    "{} record(s) will be permanently deleted. This cannot be undone.",
                    thousands(plan.total())
                ),
                theme::error(),
            ));
            (
                format!("Delete records from {}", plan.topic),
                lines,
                confirm_line(confirm, "delete them"),
            )
        }
        Dialog::ConfigChange {
            topic,
            name,
            old,
            new,
            confirm,
        } => {
            let shown = |v: &Option<String>| v.clone().unwrap_or_else(|| "(hidden)".to_owned());
            let lines = vec![
                Line::from(vec![
                    Span::styled(format!("{:<10}", "Setting"), theme::label()),
                    Span::raw(name.clone()),
                ]),
                Line::from(vec![
                    Span::styled(format!("{:<10}", "Now"), theme::label()),
                    Span::raw(shown(old)),
                ]),
                Line::from(vec![
                    Span::styled(format!("{:<10}", "After"), theme::label()),
                    match new {
                        Some(v) => Span::styled(v.clone(), theme::warning()),
                        None => Span::styled(
                            "the inherited value (broker or default)",
                            theme::warning(),
                        ),
                    },
                ]),
                Line::raw(""),
                Line::styled("The topic's other settings stay as they are.", theme::dim()),
            ];
            (
                format!("Change {topic}"),
                lines,
                confirm_line(confirm, "apply"),
            )
        }
        Dialog::ReplayProgress { plan, progress } => {
            let status = match (&progress.error, progress.done) {
                (Some(e), _) => Line::styled(format!("Stopped: {e}"), theme::error()),
                (None, true) => Line::styled("Finished.", theme::ok()),
                (None, false) => Line::styled("Copying…", theme::dim()),
            };
            let lines = vec![
                Line::raw(""),
                Line::raw(""),
                Line::from(vec![
                    Span::styled("Read ", theme::dim()),
                    Span::raw(format!(
                        "{} of {}",
                        thousands(progress.scanned as i64),
                        thousands(plan.total())
                    )),
                    Span::styled("   produced ", theme::dim()),
                    Span::raw(thousands(progress.produced as i64)),
                ]),
                status,
            ];
            let footer = if progress.done {
                hint(&[("esc", "close")])
            } else {
                hint(&[("c", "cancel"), ("esc", "hide (keeps running)")])
            };
            (
                format!("Replaying {} into {}", plan.source, plan.target),
                lines,
                footer,
            )
        }
    };

    let area = frame.area();
    let width = (lines
        .iter()
        .map(Line::width)
        .max()
        .unwrap_or(40)
        .max(title.len() + 6)
        .max(footer.width())
        + 4)
    .min(area.width.saturating_sub(4) as usize) as u16;
    lines.push(Line::raw(""));
    lines.push(footer);
    let height = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
    let [popup] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [popup] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(popup);

    frame.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_style(theme::label())
        .title(Line::styled(format!(" {title} "), theme::title()));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        inner,
    );

    if let Dialog::ReplayProgress { plan, progress } = dialog {
        let ratio = (progress.scanned as f64 / plan.total().max(1) as f64).min(1.0);
        let gauge = Gauge::default()
            .ratio(ratio)
            .gauge_style(if progress.error.is_some() {
                theme::error()
            } else {
                theme::label()
            })
            .label(format!("{:.0}%", ratio * 100.0));
        frame.render_widget(
            gauge,
            Rect {
                y: inner.y,
                height: 1,
                ..inner
            },
        );
    }
}

fn confirm_line(confirm: &Confirm, verb: &str) -> Line<'static> {
    if let Some(reason) = &confirm.blocked {
        return Line::from(vec![
            Span::styled(reason.clone(), theme::error()),
            Span::styled("   esc close", theme::dim()),
        ]);
    }
    match &confirm.expected {
        None => Line::from(vec![
            Span::styled(" y ", theme::key_hint()),
            Span::raw(verb.to_owned()),
            Span::styled("   esc ", theme::key_hint()),
            Span::raw("cancel"),
        ]),
        Some(expected) => Line::from(vec![
            Span::raw(format!(
                "Production profile: type {expected} and press enter to {verb}: "
            )),
            Span::styled(confirm.typed.clone(), theme::warning()),
            Span::styled("▏", theme::key_hint()),
        ]),
    }
}

fn hint(keys: &[(&str, &str)]) -> Line<'static> {
    Line::from(
        keys.iter()
            .flat_map(|(k, l)| {
                [
                    Span::styled(format!(" {k} "), theme::key_hint()),
                    Span::styled(format!("{l}  "), theme::dim()),
                ]
            })
            .collect::<Vec<_>>(),
    )
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_owned()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}
