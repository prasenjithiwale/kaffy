//! Renders real kaffy screens, fed by the demo cluster, as HTML for the website.
//!
//! ```sh
//! cargo run -p kaffy-ui --example snapshot > screens.html
//! ```
//!
//! Each screen becomes a `<pre class="tui">` of styled spans. Colors are emitted as
//! classes (`fg-cyan`, `rev`, `dim`, ...) so the page's CSS decides the palette.

use std::fmt::Write as _;
use std::time::Duration;

use kaffy_kafka::{Command, Worker};
use kaffy_ui::{Action, AppState, Effect, InputKind};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Cell;
use ratatui::style::{Color, Modifier};

const WIDTH: u16 = 112;
const HEIGHT: u16 = 26;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut worker = kaffy_kafka::spawn_demo();
    let mut state = AppState::new("demo", false, false);
    let mut screens = Vec::new();

    send(&worker, Command::ListTopics);
    settle(&mut worker, &mut state, 200).await;
    act(&mut worker, &mut state, &[Action::Down]).await;
    screens.push(("topics", draw(&mut state)));

    act(&mut worker, &mut state, &[Action::Open]).await;
    settle(&mut worker, &mut state, 1500).await;
    screens.push(("messages", draw(&mut state)));

    act(
        &mut worker,
        &mut state,
        &[Action::BeginInput(InputKind::Filter)],
    )
    .await;
    for c in r#"~"status":"(PAID|SHIPPED)" p:0,1"#.chars() {
        state.update(Action::InputChar(c));
    }
    act(
        &mut worker,
        &mut state,
        &[Action::InputSubmit, Action::TogglePause],
    )
    .await;
    settle(&mut worker, &mut state, 300).await;
    screens.push(("filter", draw(&mut state)));

    // Back to topics (clear filter, leave), then open sensor-readings' newest record.
    act(
        &mut worker,
        &mut state,
        &[Action::Back, Action::Back, Action::Down, Action::Down],
    )
    .await;
    act(&mut worker, &mut state, &[Action::Open]).await;
    settle(&mut worker, &mut state, 1500).await;
    act(
        &mut worker,
        &mut state,
        &[Action::TogglePause, Action::Up, Action::Open],
    )
    .await;
    screens.push(("detail", draw(&mut state)));

    // Consumer groups: sample lag for a few seconds so the trend lines have shape.
    act(&mut worker, &mut state, &[Action::ShowGroups]).await;
    for _ in 0..14 {
        send(&worker, Command::ListGroups);
        settle(&mut worker, &mut state, 450).await;
    }
    screens.push(("groups", draw(&mut state)));

    act(&mut worker, &mut state, &[Action::Open]).await;
    for _ in 0..6 {
        send(&worker, Command::DescribeGroup("analytics-backfill".into()));
        settle(&mut worker, &mut state, 300).await;
    }
    screens.push(("group", draw(&mut state)));

    act(
        &mut worker,
        &mut state,
        &[
            Action::ResetOffsets,
            Action::DialogKey('a'),
            Action::DialogKey('e'),
        ],
    )
    .await;
    settle(&mut worker, &mut state, 300).await;
    screens.push(("reset", draw(&mut state)));

    for (name, html) in screens {
        println!("<pre class=\"tui\" data-screen=\"{name}\">{html}</pre>");
    }
}

fn send(worker: &Worker, command: Command) {
    worker
        .commands
        .send(command)
        .expect("demo worker is running");
}

/// Applies actions, forwarding any Kafka commands they produce.
async fn act(worker: &mut Worker, state: &mut AppState, actions: &[Action]) {
    for action in actions {
        if let Some(Effect::Kafka(command)) = state.update(action.clone()) {
            send(worker, command);
        }
    }
    settle(worker, state, 50).await;
}

/// Feeds demo events into the state for `ms` milliseconds.
async fn settle(worker: &mut Worker, state: &mut AppState, ms: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, worker.events.recv()).await {
        state.on_event(event);
    }
}

fn draw(state: &mut AppState) -> String {
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
    terminal
        .draw(|frame| kaffy_ui::render(frame, state))
        .unwrap();
    let buffer = terminal.backend().buffer().clone();

    let mut html = String::new();
    for y in 0..buffer.area.height {
        let mut run_classes = String::new();
        let mut run_text = String::new();
        for x in 0..buffer.area.width {
            let cell = &buffer[(x, y)];
            if cell.symbol().is_empty() {
                continue; // second half of a wide character
            }
            let classes = classes(cell);
            if classes != run_classes {
                flush(&mut html, &run_classes, &run_text);
                run_classes = classes;
                run_text.clear();
            }
            escape_into(&mut run_text, cell.symbol());
        }
        flush(&mut html, &run_classes, run_text.trim_end());
        html.push('\n');
    }
    html
}

fn flush(html: &mut String, classes: &str, text: &str) {
    if text.is_empty() {
        return;
    }
    if classes.is_empty() {
        html.push_str(text);
    } else {
        let _ = write!(html, "<span class=\"{classes}\">{text}</span>");
    }
}

fn classes(cell: &Cell) -> String {
    let mut classes = Vec::new();
    let (mut fg, mut bg) = (cell.fg, cell.bg);
    if cell.modifier.contains(Modifier::REVERSED) {
        std::mem::swap(&mut fg, &mut bg);
        classes.push("rev".to_owned());
    }
    if let Some(name) = color_name(fg) {
        classes.push(format!("fg-{name}"));
    }
    if let Some(name) = color_name(bg) {
        classes.push(format!("bg-{name}"));
    }
    for (modifier, class) in [
        (Modifier::BOLD, "b"),
        (Modifier::DIM, "dim"),
        (Modifier::UNDERLINED, "u"),
    ] {
        if cell.modifier.contains(modifier) {
            classes.push(class.to_owned());
        }
    }
    classes.join(" ")
}

fn color_name(color: Color) -> Option<&'static str> {
    Some(match color {
        Color::Reset => return None,
        Color::Black => "black",
        Color::Red => "red",
        Color::Green => "green",
        Color::Yellow => "yellow",
        Color::Blue => "blue",
        Color::Magenta => "magenta",
        Color::Cyan => "cyan",
        Color::Gray | Color::White => "white",
        _ => "white",
    })
}

fn escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
}
