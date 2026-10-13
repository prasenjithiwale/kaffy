//! Renders each screen into an in-memory terminal and checks what is on it.
//! Run with `--nocapture` to see the screens.

use kaffy_kafka::{Command, Event, Header, PartitionInfo, Record, TopicInfo};
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
    }
}

fn draw(state: &mut AppState) -> String {
    let mut terminal = Terminal::new(TestBackend::new(110, 20)).unwrap();
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
            record: record(i, value.as_deref()),
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
    assert!(screen.contains("Topics (2)"));
    assert!(screen.contains("▶ orders"));

    state.update(Action::BeginInput(InputKind::Filter));
    state.update(Action::InputChar('p'));
    let screen = draw(&mut state);
    assert!(screen.contains("filter: p · 1 match"));
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
