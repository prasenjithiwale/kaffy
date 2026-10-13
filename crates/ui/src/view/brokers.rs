use kaffy_kafka::{ClusterInfo, ProblemKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use super::kit;
use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(cluster) = state.brokers.cluster.as_ref() else {
        let block = kit::panel("Brokers");
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::placeholder(frame, inner, "◌ Loading cluster…");
        return;
    };
    let health = cluster.broker_health();
    let problems = cluster.problems();

    let [summary_area, brokers_area, problems_area] = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(health.len() as u16 + 3),
        Constraint::Min(4),
    ])
    .areas(area);

    render_summary(frame, summary_area, cluster, problems.len());

    let rows: Vec<Row> = health
        .iter()
        .map(|b| {
            let role = if cluster.controller == Some(b.id) {
                Cell::from("◆ controller").style(theme::accent())
            } else {
                Cell::from("")
            };
            let address = match &b.address {
                Some(address) => Cell::from(Line::from(vec![
                    Span::styled("● ", theme::ok()),
                    Span::raw(address.clone()),
                ])),
                None => Cell::from("✕ not in metadata, likely down").style(theme::error()),
            };
            let out_of_sync = if b.out_of_sync > 0 {
                kit::num_styled(b.out_of_sync.to_string(), theme::error())
            } else {
                kit::num_styled("0", theme::faint())
            };
            Row::new(vec![
                kit::num(b.id.to_string()),
                address,
                role,
                kit::num(b.leaders.to_string()),
                kit::num(b.replicas.to_string()),
                out_of_sync,
            ])
        })
        .collect();
    let address_width = health
        .iter()
        .map(|b| b.address.as_ref().map_or(30, |a| a.len() + 2))
        .max()
        .unwrap_or(10)
        .max(7) as u16;
    let brokers = Table::new(
        kit::striped(rows),
        [
            Constraint::Length(2),
            Constraint::Length(address_width),
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(11),
        ],
    )
    .header(kit::header([
        kit::head("ID", true),
        kit::head("ADDRESS", false),
        kit::head("ROLE", false),
        kit::head("LEADERS", true),
        kit::head("REPLICAS", true),
        kit::head("OUT OF SYNC", true),
    ]))
    .column_spacing(3)
    .block(kit::counted("Brokers", cluster.brokers.len()));
    frame.render_widget(brokers, brokers_area);

    let block = kit::counted("Partitions needing attention", problems.len());
    if problems.is_empty() {
        let inner = block.inner(problems_area);
        frame.render_widget(block, problems_area);
        super::placeholder(
            frame,
            inner,
            "✓ Every partition has a leader and all its replicas in sync",
        );
        return;
    }
    let topic_width = problems
        .iter()
        .map(|p| p.topic.len())
        .max()
        .unwrap_or(5)
        .max(5) as u16;
    let ids = |ids: &[i32]| ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",");
    let rows: Vec<Row> = problems
        .iter()
        .map(|p| {
            let (problem, leader) = match p.kind {
                ProblemKind::Offline => ("✕ offline", "none".to_owned()),
                ProblemKind::UnderReplicated => ("▲ under-replicated", p.leader.to_string()),
            };
            let missing: Vec<i32> = p
                .replicas
                .iter()
                .copied()
                .filter(|r| !p.isr.contains(r))
                .collect();
            Row::new(vec![
                Cell::from(problem).style(match p.kind {
                    ProblemKind::Offline => theme::error(),
                    ProblemKind::UnderReplicated => theme::warning(),
                }),
                Cell::from(p.topic.clone()),
                kit::num_styled(p.partition.to_string(), theme::partition(p.partition)),
                kit::num(leader),
                Cell::from(ids(&p.replicas)),
                Cell::from(ids(&p.isr)),
                Cell::from(ids(&missing)).style(theme::error()),
            ])
        })
        .collect();
    let table = kit::table(
        rows,
        [
            Constraint::Length(18),
            Constraint::Length(topic_width),
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(kit::header([
        kit::head("PROBLEM", false),
        kit::head("TOPIC", false),
        kit::head("PARTITION", true),
        kit::head("LEADER", true),
        kit::head("REPLICAS", false),
        kit::head("IN SYNC", false),
        kit::head("MISSING", false),
    ]))
    .column_spacing(3)
    .block(block);
    frame.render_stateful_widget(table, problems_area, &mut state.brokers.table);
}

fn render_summary(frame: &mut Frame, area: Rect, cluster: &ClusterInfo, problems: usize) {
    let field = |label: &str, value: Vec<Span<'static>>| {
        let mut spans = vec![Span::styled(format!("{label:<12}"), theme::label())];
        spans.extend(value);
        Line::from(spans)
    };
    let mut controller = match cluster.controller {
        Some(id) => vec![Span::raw(format!("broker {id}"))],
        None => vec![Span::styled("unknown", theme::warning())],
    };
    // KRaft brokers name one of themselves, which forwards to the controller quorum.
    controller.push(Span::styled(
        "   as reported to clients; on KRaft, the broker that forwards to the controller quorum",
        theme::dim(),
    ));
    let offline = cluster
        .problems()
        .iter()
        .any(|p| p.kind == ProblemKind::Offline);
    let health = if problems == 0 {
        Span::styled("● healthy", theme::ok())
    } else {
        Span::styled(
            format!(
                "▲ {problems} of {} partition(s) need attention",
                cluster.partition_count()
            ),
            if offline {
                theme::error()
            } else {
                theme::warning().add_modifier(Modifier::BOLD)
            },
        )
    };
    let cluster_id = cluster
        .cluster_id
        .clone()
        .unwrap_or_else(|| "unknown".into());
    let lines = vec![
        field("Cluster id", vec![Span::raw(cluster_id)]),
        field("Controller", controller),
        field("Health", vec![health]),
    ];
    let block = kit::panel("Cluster");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(Paragraph::new(lines), inner);
}
