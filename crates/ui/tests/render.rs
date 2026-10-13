//! Renders each screen into an in-memory terminal and checks what is on it.
//! Run with `--nocapture` to see the screens.

use kaffy_kafka::{
    BrokerInfo, ClusterInfo, Command, Event, Header, PartitionInfo, Record, TopicInfo,
};
use kaffy_ui::{Action, AppState, Effect, InputKind, Screen};
use ratatui::Terminal;
use ratatui::backend::TestBackend;

fn topic(name: &str, isr: Vec<i32>) -> TopicInfo {
    TopicInfo {
        name: name.to_owned(),
        partitions: vec![PartitionInfo {
            id: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr,
        }],
    }
}

fn record(offset: i64, value: Option<&str>) -> Record {
    Record {
        topic: "orders".to_owned(),
        partition: 0,
        offset,
        timestamp_ms: Some(1_700_000_000_000 + offset * 1000),
        key: Some(format!("cust-{offset:03}").into_bytes()),
        value: value.map(|v| v.as_bytes().to_vec()),
        headers: vec![Header {
            key: "trace-id".to_owned(),
            value: Some(b"abc123".to_vec()),
        }],
        ..Default::default()
    }
}

fn draw(state: &mut AppState) -> String {
    draw_sized(state, 110, 20)
}

fn draw_sized(state: &mut AppState, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| kaffy_ui::render(frame, state))
        .unwrap();
    let buffer = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    println!("{out}");
    out
}

fn state_with_messages() -> AppState {
    let mut state = AppState::new("local", true, false);
    state.on_event(Event::Metadata {
        brokers: 3,
        topics: vec![
            topic("__consumer_offsets", vec![1, 2, 3]),
            topic("orders", vec![1, 2, 3]),
            topic("payments", vec![1, 3]),
        ],
    });
    state.update(Action::Down);
    let Some(Effect::Kafka(Command::Tail { session, .. })) = state.update(Action::Open) else {
        panic!("open should start a tail");
    };
    state.on_event(Event::TailStarted {
        session,
        partitions: 1,
    });
    for i in 0..30 {
        let value =
            (i % 10 != 9).then(|| format!(r#"{{"order":{i},"status":"PAID","amount":12.5}}"#));
        state.on_event(Event::Record {
            session,
            record: Box::new(record(i, value.as_deref())),
        });
    }
    state
}

#[test]
fn topics_screen() {
    let mut state = AppState::new("local", true, false);
    assert!(draw(&mut state).contains("Loading topics…"));

    state.on_event(Event::Metadata {
        brokers: 3,
        topics: vec![
            topic("orders", vec![1, 2, 3]),
            topic("payments", vec![1, 3]),
        ],
    });
    let screen = draw(&mut state);
    assert!(screen.contains("connected · 3 brokers"));
    assert!(screen.contains("READ-ONLY"));
    assert!(screen.contains("Topics 2"));
    assert!(screen.contains("▌ orders"));
    assert!(
        screen.contains("▲ 1 under-replicated"),
        "payments lacks broker 2"
    );
    assert!(
        screen.contains("orders · partitions"),
        "the selected topic's partitions"
    );

    state.update(Action::BeginInput(InputKind::Filter));
    state.update(Action::InputChar('p'));
    let screen = draw(&mut state);
    assert!(screen.contains("filter p · 1 match"));
    assert!(!screen.contains("orders"));
}

#[test]
fn messages_screen_follows_newest() {
    let mut state = state_with_messages();
    assert_eq!(state.screen, Screen::Messages);
    let screen = draw(&mut state);
    assert!(screen.contains("● LIVE"));
    assert!(screen.contains("30 buffered"));
    // Newest record visible, oldest scrolled out of the 20-line terminal.
    assert!(screen.contains(r#"{"order":28,"status":"PAID","amount":12.5}"#));
    assert!(screen.contains("∅ tombstone"));
    assert!(!screen.contains(r#"{"order":0,"#));

    state.update(Action::Top);
    let screen = draw(&mut state);
    assert!(screen.contains("◼ SCROLLED"));
    assert!(screen.contains(r#"{"order":0,"#));
}

#[test]
fn detail_screen() {
    let mut state = state_with_messages();
    state.update(Action::Up);
    state.update(Action::Open);
    assert_eq!(state.screen, Screen::Detail);
    let screen = draw(&mut state);
    assert!(screen.contains("orders · p0 · offset 28"));
    assert!(screen.contains("cust-028  (text)"));
    assert!(screen.contains("trace-id = abc123"));
    assert!(screen.contains("(json, 42 bytes)"));
    assert!(screen.contains(r#""status": "PAID","#));
}

#[test]
fn long_filter_never_hides_the_topic_name() {
    let mut state = state_with_messages();
    state.update(Action::BeginInput(InputKind::Filter));
    for c in r#"~"status":"(PAID|SHIPPED|CANCELLED|CREATED)" p:0,1,2 after:-1d"#.chars() {
        state.update(Action::InputChar(c));
    }
    state.update(Action::InputSubmit);
    for width in [60, 80, 110, 160] {
        let screen = draw_sized(&mut state, width, 12);
        let title = screen.lines().nth(1).unwrap();
        assert!(title.starts_with("╭ orders "), "width {width}: {title}");
        assert!(
            title.contains("LIVE"),
            "width {width}: status is always shown: {title}"
        );
        assert!(title.contains("filter: "), "width {width}: {title}");
    }
    // Wide enough for everything.
    let wide = draw_sized(&mut state, 200, 12);
    assert!(wide.lines().nth(1).unwrap().contains("30 received"));
}

#[test]
fn brokers_screen_flags_missing_brokers_and_problem_partitions() {
    let mut state = AppState::new("local", true, false);
    assert_eq!(
        state.update(Action::ShowBrokers),
        Some(Effect::Kafka(Command::DescribeCluster))
    );
    let loading = draw(&mut state);
    assert!(loading.contains("Loading cluster"));

    let mut offline = topic("payments", vec![]);
    offline.partitions[0].leader = -1;
    offline.partitions[0].replicas = vec![3];
    state.on_event(Event::Cluster(ClusterInfo {
        cluster_id: Some("abc-123".to_owned()),
        controller: Some(2),
        brokers: [1, 2]
            .map(|id| BrokerInfo {
                id,
                host: format!("kafka-{id}"),
                port: 9092,
            })
            .to_vec(),
        topics: vec![topic("orders", vec![1, 2]), offline],
    }));
    assert_eq!(state.screen, Screen::Brokers);
    let screen = draw_sized(&mut state, 110, 24);
    assert!(screen.contains("abc-123"));
    assert!(screen.contains("broker 2"), "controller");
    assert!(screen.contains("2 of 2 partition(s) need attention"));
    assert!(screen.contains("kafka-1:9092"));
    assert!(
        screen.contains("not in metadata"),
        "broker 3 is only in replica lists"
    );
    assert!(screen.contains("offline"));
    assert!(screen.contains("under-replicated"));
    assert!(screen.contains("4 Brokers"));
}

#[test]
fn healthy_cluster_says_so() {
    let mut state = AppState::new("local", true, false);
    state.update(Action::ShowBrokers);
    state.on_event(Event::Cluster(ClusterInfo {
        cluster_id: None,
        controller: None,
        brokers: vec![BrokerInfo {
            id: 1,
            host: "localhost".to_owned(),
            port: 9092,
        }],
        topics: vec![TopicInfo {
            name: "orders".to_owned(),
            partitions: vec![PartitionInfo {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        }],
    }));
    let screen = draw_sized(&mut state, 110, 24);
    assert!(screen.contains("healthy"));
    assert!(screen.contains("Every partition has a leader"));
}
