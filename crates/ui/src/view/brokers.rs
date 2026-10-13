use kaffy_kafka::{ClusterInfo, ProblemKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table};

use crate::state::AppState;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, state: &mut AppState) {
    let Some(cluster) = state.brokers.cluster.as_ref() else {
        let block = Block::bordered().title(Line::styled(" Brokers ", theme::title()));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::placeholder(frame, inner, "Loading cluster…");
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
                Cell::from("controller").style(theme::title())
            } else {
                Cell::from("")
            };
            let address = match &b.address {
                Some(address) => Cell::from(address.clone()),
                None => Cell::from("not in metadata, likely down").style(theme::error()),
            };
            let out_of_sync = if b.out_of_sync > 0 {
                Cell::from(b.out_of_sync.to_string()).style(theme::error())
            } else {
                Cell::from("0").style(theme::dim())
            };
            Row::new(vec![
                Cell::from(b.id.to_string()),
                address,
                role,
                Cell::from(b.leaders.to_string()),
                Cell::from(b.replicas.to_string()),
                out_of_sync,
            ])
        })
        .collect();
    let address_width = health
        .iter()
        .map(|b| b.address.as_ref().map_or(28, String::len))
        .max()
        .unwrap_or(10)
        .max(7) as u16;
    let brokers = Table::new(
        rows,
        [
            Constraint::Length(4),
            Constraint::Length(address_width),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(11),
        ],
    )
    .header(
        Row::new([
            "ID",
            "Address",
            "Role",
            "Leaders",
            "Replicas",
            "Out of sync",
        ])
        .style(theme::header_row()),
    )
    .column_spacing(3)
    .block(Block::bordered().title(Line::styled(
        format!(" Brokers ({}) ", cluster.brokers.len()),
        theme::title(),
    )));
    frame.render_widget(brokers, brokers_area);

    let block = Block::bordered().title(Line::styled(
        format!(" Partitions needing attention ({}) ", problems.len()),
        theme::title(),
    ));
    if problems.is_empty() {
        let inner = block.inner(problems_area);
        frame.render_widget(block, problems_area);
        super::placeholder(
            frame,
            inner,
            "Every partition has a leader and all its replicas in sync",
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
                ProblemKind::Offline => ("offline", "none".to_owned()),
                ProblemKind::UnderReplicated => ("under-replicated", p.leader.to_string()),
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
                Cell::from(p.partition.to_string()),
                Cell::from(leader),
                Cell::from(ids(&p.replicas)),
                Cell::from(ids(&p.isr)),
                Cell::from(ids(&missing)).style(theme::error()),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Length(topic_width),
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new([
            "Problem",
            "Topic",
            "Partition",
            "Leader",
            "Replicas",
            "In sync",
            "Missing",
        ])
        .style(theme::header_row()),
    )
    .column_spacing(3)
    .row_highlight_style(theme::selected())
    .highlight_symbol("▶ ")
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
    let health = if problems == 0 {
        Span::styled("healthy", theme::ok())
    } else {
        Span::styled(
            format!(
                "{problems} of {} partition(s) need attention",
                cluster.partition_count()
            ),
            theme::error(),
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
    let block = Block::bordered().title(Line::styled(" Cluster ", theme::title()));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(Paragraph::new(lines), inner);
}
