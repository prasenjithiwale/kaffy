//! Application state and the reducer that updates it.
//!
//! Key presses and Kafka events both become [`Action`]s. [`AppState::update`]
//! applies an action and may return an [`Effect`] for the app to carry out
//! (send a Kafka command, copy to the clipboard). Rendering only reads state.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use kaffy_kafka::{Command, Event, Record, StartAt, TopicInfo};
use ratatui::widgets::TableState;

use crate::filter::{Filter, Subject};
use crate::fmt;

/// Records kept per tail; older ones are dropped.
pub const BUFFER_CAPACITY: usize = 5_000;
/// Where a tail starts when a topic is opened.
pub const DEFAULT_START: StartAt = StartAt::LastN(100);
/// Characters kept for the table previews.
const PREVIEW_CHARS: usize = 400;
const FLASH_TTL: Duration = Duration::from_secs(6);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
    Detail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Filter,
    SeekOffset,
    SeekTimestamp,
}

impl InputKind {
    pub fn prompt(self) -> &'static str {
        match self {
            Self::Filter => "/",
            Self::SeekOffset => "seek to offset: ",
            Self::SeekTimestamp => "seek to time (-15m, 2026-10-13 09:30, RFC 3339, epoch ms): ",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Input {
    pub kind: InputKind,
    pub buffer: String,
    /// Filter value to restore if the input is cancelled.
    previous: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Quit,
    Up,
    Down,
    PageUp,
    PageDown,
    Top,
    Bottom,
    Open,
    Back,
    Refresh,
    TogglePause,
    Seek(StartAt),
    BeginInput(InputKind),
    InputChar(char),
    InputBackspace,
    InputSubmit,
    InputCancel,
    CopyKey,
    CopyValue,
    Tick,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Kafka(Command),
    Copy { what: &'static str, text: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Connection {
    Connecting,
    Connected { brokers: usize },
}

#[derive(Debug, Clone)]
pub struct Flash {
    pub text: String,
    pub error: bool,
    at: Instant,
}

/// A record plus the strings the table needs, computed once on arrival.
#[derive(Debug, Clone)]
pub struct Row {
    pub record: Record,
    pub key_preview: String,
    pub value_preview: String,
    /// Text the filter searches; binary payloads are converted lossily.
    key_text: String,
    value_text: String,
    /// One `name=value` line per header.
    headers_text: String,
}

impl Row {
    fn new(record: Record) -> Self {
        let key_preview = record
            .key
            .as_deref()
            .map(|k| kaffy_schema::preview(k, PREVIEW_CHARS))
            .unwrap_or_default();
        let value_preview = record
            .value
            .as_deref()
            .map(|v| kaffy_schema::preview(v, PREVIEW_CHARS))
            .unwrap_or_default();
        let lossy = |bytes: Option<&[u8]>| {
            bytes
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default()
        };
        let headers_text = record
            .headers
            .iter()
            .map(|h| format!("{}={}", h.key, lossy(h.value.as_deref())))
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            key_text: lossy(record.key.as_deref()),
            value_text: lossy(record.value.as_deref()),
            headers_text,
            record,
            key_preview,
            value_preview,
        }
    }

    fn subject(&self) -> Subject<'_> {
        Subject {
            key: &self.key_text,
            value: &self.value_text,
            headers: &self.headers_text,
            partition: self.record.partition,
            timestamp_ms: self.record.timestamp_ms,
        }
    }

    fn id(&self) -> (i32, i64) {
        (self.record.partition, self.record.offset)
    }
}

#[derive(Debug, Clone)]
pub struct TailView {
    pub topic: String,
    pub session: u64,
    pub start: StartAt,
    pub partitions: Option<usize>,
    pub rows: VecDeque<Row>,
    pub received: u64,
    pub paused: bool,
    /// The filter as typed; see [`crate::filter`] for the syntax.
    pub filter: String,
    compiled: Result<Filter, String>,
    /// Keep the newest record selected as records arrive.
    pub follow: bool,
    /// (partition, offset) of the selected record when not following.
    selected: Option<(i32, i64)>,
    /// First visible row, maintained by the view.
    pub scroll: usize,
}

impl TailView {
    fn new(topic: String, session: u64, start: StartAt) -> Self {
        Self {
            topic,
            session,
            start,
            partitions: None,
            rows: VecDeque::new(),
            received: 0,
            paused: false,
            filter: String::new(),
            compiled: Ok(Filter::default()),
            follow: true,
            selected: None,
            scroll: 0,
        }
    }

    /// Indices into `rows` that match the filter. An invalid filter shows everything.
    pub fn visible(&self) -> Vec<usize> {
        match &self.compiled {
            Ok(filter) if !filter.is_empty() => self
                .rows
                .iter()
                .enumerate()
                .filter(|(_, row)| filter.matches(&row.subject()))
                .map(|(i, _)| i)
                .collect(),
            _ => (0..self.rows.len()).collect(),
        }
    }

    pub fn filter_error(&self) -> Option<&str> {
        self.compiled.as_ref().err().map(String::as_str)
    }

    fn set_filter(&mut self, text: String) {
        self.compiled = Filter::parse(&text, fmt::now_ms());
        self.filter = text;
    }

    /// Position of the selected row within `visible`.
    pub fn selected_position(&self, visible: &[usize]) -> Option<usize> {
        if visible.is_empty() {
            return None;
        }
        if self.follow {
            return Some(visible.len() - 1);
        }
        let found = self
            .selected
            .and_then(|id| visible.iter().position(|&i| self.rows[i].id() == id));
        // The selected record was evicted or filtered out: fall back to the top.
        Some(found.unwrap_or(0))
    }

    pub fn selected_row(&self) -> Option<&Row> {
        let visible = self.visible();
        self.selected_position(&visible)
            .map(|pos| &self.rows[visible[pos]])
    }

    fn move_selection(&mut self, delta: isize) {
        let visible = self.visible();
        let Some(current) = self.selected_position(&visible) else {
            return;
        };
        let last = visible.len() - 1;
        let target = current.saturating_add_signed(delta).min(last);
        self.select_position(&visible, target);
    }

    fn select_position(&mut self, visible: &[usize], position: usize) {
        if visible.is_empty() {
            return;
        }
        self.follow = position == visible.len() - 1;
        self.selected = Some(self.rows[visible[position]].id());
    }

    fn push(&mut self, record: Record) {
        if self.rows.len() == BUFFER_CAPACITY {
            self.rows.pop_front();
            self.scroll = self.scroll.saturating_sub(1);
        }
        self.rows.push_back(Row::new(record));
        self.received += 1;
    }
}

#[derive(Debug, Clone)]
pub struct DetailView {
    pub record: Record,
    pub key_format: Option<kaffy_schema::Format>,
    pub key_text: Option<String>,
    pub value_format: Option<kaffy_schema::Format>,
    pub value_text: Option<String>,
    pub scroll: u16,
}

impl DetailView {
    fn new(record: Record) -> Self {
        let key = record.key.as_deref().map(kaffy_schema::render);
        let value = record.value.as_deref().map(kaffy_schema::render);
        Self {
            key_format: key.as_ref().map(|(f, _)| *f),
            key_text: key.map(|(_, t)| t),
            value_format: value.as_ref().map(|(f, _)| *f),
            value_text: value.map(|(_, t)| t),
            record,
            scroll: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub profile: String,
    pub read_only: bool,
    pub prod: bool,
    pub screen: Screen,
    pub connection: Connection,
    pub topics: Option<Vec<TopicInfo>>,
    pub topic_filter: String,
    pub topic_table: TableState,
    pub tail: Option<TailView>,
    pub detail: Option<DetailView>,
    pub input: Option<Input>,
    pub flash: Option<Flash>,
    pub should_quit: bool,
    next_session: u64,
}

impl AppState {
    pub fn new(profile: impl Into<String>, read_only: bool, prod: bool) -> Self {
        Self {
            profile: profile.into(),
            read_only,
            prod,
            screen: Screen::Topics,
            connection: Connection::Connecting,
            topics: None,
            topic_filter: String::new(),
            topic_table: TableState::default().with_selected(0),
            tail: None,
            detail: None,
            input: None,
            flash: None,
            should_quit: false,
            next_session: 0,
        }
    }

    pub fn visible_topics(&self) -> Vec<&TopicInfo> {
        let needle = self.topic_filter.to_lowercase();
        self.topics
            .iter()
            .flatten()
            .filter(|t| t.name.to_lowercase().contains(&needle))
            .collect()
    }

    pub fn flash_info(&mut self, text: impl Into<String>) {
        self.flash = Some(Flash {
            text: text.into(),
            error: false,
            at: Instant::now(),
        });
    }

    pub fn flash_error(&mut self, text: impl Into<String>) {
        self.flash = Some(Flash {
            text: text.into(),
            error: true,
            at: Instant::now(),
        });
    }

    pub fn on_event(&mut self, event: Event) {
        match event {
            Event::Metadata { brokers, topics } => {
                self.connection = Connection::Connected { brokers };
                self.topics = Some(topics);
                self.clamp_topic_selection();
            }
            Event::TailStarted {
                session,
                partitions,
            } => {
                if let Some(tail) = self.tail.as_mut().filter(|t| t.session == session) {
                    tail.partitions = Some(partitions);
                }
            }
            Event::Record { session, record } => {
                if let Some(tail) = self.tail.as_mut().filter(|t| t.session == session) {
                    tail.push(record);
                }
            }
            Event::Error(message) => self.flash_error(message),
        }
    }

    pub fn update(&mut self, action: Action) -> Option<Effect> {
        match action {
            Action::Quit => {
                self.should_quit = true;
                None
            }
            Action::Tick => {
                if self
                    .flash
                    .as_ref()
                    .is_some_and(|f| f.at.elapsed() > FLASH_TTL)
                {
                    self.flash = None;
                }
                None
            }
            Action::BeginInput(kind) => {
                let current = match kind {
                    InputKind::Filter => self.filter().to_owned(),
                    InputKind::SeekOffset | InputKind::SeekTimestamp => String::new(),
                };
                self.input = Some(Input {
                    kind,
                    buffer: current.clone(),
                    previous: current,
                });
                None
            }
            Action::InputChar(c) => {
                let input = self.input.as_mut()?;
                input.buffer.push(c);
                self.sync_live_filter();
                None
            }
            Action::InputBackspace => {
                let input = self.input.as_mut()?;
                input.buffer.pop();
                self.sync_live_filter();
                None
            }
            Action::InputCancel => {
                let input = self.input.take()?;
                if input.kind == InputKind::Filter {
                    self.set_filter(input.previous);
                }
                None
            }
            Action::InputSubmit => self.submit_input(),
            _ => match self.screen {
                Screen::Topics => self.update_topics(action),
                Screen::Messages => self.update_messages(action),
                Screen::Detail => self.update_detail(action),
            },
        }
    }

    fn update_topics(&mut self, action: Action) -> Option<Effect> {
        let len = self.visible_topics().len();
        let selected = self.topic_table.selected().unwrap_or(0);
        let select =
            |table: &mut TableState, i: usize| table.select(Some(i.min(len.saturating_sub(1))));
        match action {
            Action::Up => select(&mut self.topic_table, selected.saturating_sub(1)),
            Action::Down => select(&mut self.topic_table, selected + 1),
            Action::PageUp => select(&mut self.topic_table, selected.saturating_sub(10)),
            Action::PageDown => select(&mut self.topic_table, selected + 10),
            Action::Top => select(&mut self.topic_table, 0),
            Action::Bottom => select(&mut self.topic_table, len),
            Action::Refresh => return Some(Effect::Kafka(Command::ListTopics)),
            Action::Back if !self.topic_filter.is_empty() => self.set_filter(String::new()),
            Action::Open => {
                let topic = self.visible_topics().get(selected)?.name.clone();
                self.screen = Screen::Messages;
                self.tail = None;
                return Some(self.start_tail(topic, DEFAULT_START));
            }
            _ => {}
        }
        None
    }

    fn update_messages(&mut self, action: Action) -> Option<Effect> {
        let tail = self.tail.as_mut()?;
        match action {
            Action::Up => tail.move_selection(-1),
            Action::Down => tail.move_selection(1),
            Action::PageUp => tail.move_selection(-20),
            Action::PageDown => tail.move_selection(20),
            Action::Top => {
                let visible = tail.visible();
                tail.select_position(&visible, 0);
            }
            Action::Bottom => tail.follow = true,
            Action::Open => {
                let record = tail.selected_row()?.record.clone();
                self.detail = Some(DetailView::new(record));
                self.screen = Screen::Detail;
            }
            Action::Back if !tail.filter.is_empty() => tail.set_filter(String::new()),
            Action::Back => {
                self.tail = None;
                self.screen = Screen::Topics;
                return Some(Effect::Kafka(Command::StopTail));
            }
            Action::TogglePause => {
                tail.paused = !tail.paused;
                let command = if tail.paused {
                    Command::Pause
                } else {
                    Command::Resume
                };
                return Some(Effect::Kafka(command));
            }
            Action::Seek(start) => {
                let topic = tail.topic.clone();
                return Some(self.restart_tail(topic, start));
            }
            Action::Refresh => {
                let (topic, start) = (tail.topic.clone(), tail.start);
                return Some(self.restart_tail(topic, start));
            }
            Action::CopyKey | Action::CopyValue => {
                let record = tail.selected_row()?.record.clone();
                return copy(&record, action == Action::CopyKey);
            }
            _ => {}
        }
        None
    }

    fn update_detail(&mut self, action: Action) -> Option<Effect> {
        let detail = self.detail.as_mut()?;
        match action {
            Action::Up => detail.scroll = detail.scroll.saturating_sub(1),
            Action::Down => detail.scroll = detail.scroll.saturating_add(1),
            Action::PageUp => detail.scroll = detail.scroll.saturating_sub(20),
            Action::PageDown => detail.scroll = detail.scroll.saturating_add(20),
            Action::Top => detail.scroll = 0,
            Action::Back => {
                self.detail = None;
                self.screen = Screen::Messages;
            }
            Action::CopyKey | Action::CopyValue => {
                return copy(&detail.record, action == Action::CopyKey);
            }
            _ => {}
        }
        None
    }

    fn submit_input(&mut self) -> Option<Effect> {
        let input = self.input.take()?;
        let text = input.buffer.trim();
        let start = match input.kind {
            InputKind::Filter => return None,
            InputKind::SeekOffset => match text.parse::<i64>() {
                Ok(offset) if offset >= 0 => StartAt::Offset(offset),
                _ => {
                    self.flash_error(format!("not an offset: {text:?}"));
                    return None;
                }
            },
            InputKind::SeekTimestamp => match fmt::parse_timestamp(text, fmt::now_ms()) {
                Some(ms) => StartAt::Timestamp(ms),
                None => {
                    self.flash_error(format!("not a time: {text:?}"));
                    return None;
                }
            },
        };
        let topic = self.tail.as_ref()?.topic.clone();
        Some(self.restart_tail(topic, start))
    }

    fn start_tail(&mut self, topic: String, start: StartAt) -> Effect {
        self.next_session += 1;
        let session = self.next_session;
        self.tail = Some(TailView::new(topic.clone(), session, start));
        Effect::Kafka(Command::Tail {
            session,
            topic,
            start,
        })
    }

    /// Like `start_tail` but keeps the filter.
    fn restart_tail(&mut self, topic: String, start: StartAt) -> Effect {
        let filter = self.tail.take().map(|t| t.filter).unwrap_or_default();
        let effect = self.start_tail(topic, start);
        if let Some(tail) = self.tail.as_mut() {
            tail.set_filter(filter);
        }
        self.flash_info(format!("reading from {}", fmt::start_at(start)));
        effect
    }

    /// The filter of the current screen.
    fn filter(&self) -> &str {
        match (self.screen, &self.tail) {
            (Screen::Messages | Screen::Detail, Some(tail)) => &tail.filter,
            _ => &self.topic_filter,
        }
    }

    fn set_filter(&mut self, value: String) {
        match (self.screen, self.tail.as_mut()) {
            (Screen::Messages | Screen::Detail, Some(tail)) => tail.set_filter(value),
            _ => {
                self.topic_filter = value;
                self.clamp_topic_selection();
            }
        }
    }

    fn sync_live_filter(&mut self) {
        if let Some(input) = &self.input {
            if input.kind == InputKind::Filter {
                self.set_filter(input.buffer.clone());
            }
        }
    }

    fn clamp_topic_selection(&mut self) {
        let len = self.visible_topics().len();
        let selected = self.topic_table.selected().unwrap_or(0);
        self.topic_table
            .select(Some(selected.min(len.saturating_sub(1))));
    }
}

fn copy(record: &Record, key: bool) -> Option<Effect> {
    let (what, bytes) = if key {
        ("key", record.key.as_deref())
    } else {
        ("value", record.value.as_deref())
    };
    let bytes = bytes?;
    let text = match kaffy_schema::detect(bytes) {
        kaffy_schema::Format::Json | kaffy_schema::Format::Text => {
            String::from_utf8_lossy(bytes).into_owned()
        }
        _ => kaffy_schema::hex_dump(bytes),
    };
    Some(Effect::Copy { what, text })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaffy_kafka::{PartitionInfo, TopicInfo};

    fn topic(name: &str) -> TopicInfo {
        TopicInfo {
            name: name.to_owned(),
            partitions: vec![PartitionInfo {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        }
    }

    fn record(offset: i64, value: &str) -> Record {
        Record {
            topic: "orders".to_owned(),
            partition: 0,
            offset,
            timestamp_ms: Some(0),
            key: Some(b"k".to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: Vec::new(),
        }
    }

    fn with_topics() -> AppState {
        let mut state = AppState::new("test", true, false);
        state.on_event(Event::Metadata {
            brokers: 1,
            topics: vec![topic("orders"), topic("payments")],
        });
        state
    }

    fn open_orders(state: &mut AppState) -> u64 {
        match state.update(Action::Open) {
            Some(Effect::Kafka(Command::Tail { session, topic, .. })) => {
                assert_eq!(topic, "orders");
                session
            }
            other => panic!("expected a tail command, got {other:?}"),
        }
    }

    #[test]
    fn topic_filter_is_live_and_cancel_restores() {
        let mut state = with_topics();
        state.update(Action::BeginInput(InputKind::Filter));
        state.update(Action::InputChar('p'));
        state.update(Action::InputChar('a'));
        assert_eq!(state.visible_topics().len(), 1);
        state.update(Action::InputCancel);
        assert_eq!(state.visible_topics().len(), 2);
    }

    #[test]
    fn stale_sessions_are_ignored() {
        let mut state = with_topics();
        let first = open_orders(&mut state);
        let Some(Effect::Kafka(Command::Tail {
            session: second, ..
        })) = state.update(Action::Seek(StartAt::Beginning))
        else {
            panic!("seek should restart the tail");
        };
        state.on_event(Event::Record {
            session: first,
            record: record(1, "old"),
        });
        state.on_event(Event::Record {
            session: second,
            record: record(0, "new"),
        });
        let tail = state.tail.as_ref().unwrap();
        assert_eq!(tail.rows.len(), 1);
        assert_eq!(tail.rows[0].value_preview, "new");
    }

    #[test]
    fn selection_follows_until_moved() {
        let mut state = with_topics();
        let session = open_orders(&mut state);
        for i in 0..5 {
            state.on_event(Event::Record {
                session,
                record: record(i, "x"),
            });
        }
        state.update(Action::Up);
        let tail = state.tail.as_ref().unwrap();
        assert!(!tail.follow);
        assert_eq!(tail.selected_row().unwrap().record.offset, 3);

        // New records don't move a pinned selection.
        state.on_event(Event::Record {
            session,
            record: record(5, "x"),
        });
        assert_eq!(
            state
                .tail
                .as_ref()
                .unwrap()
                .selected_row()
                .unwrap()
                .record
                .offset,
            3
        );

        state.update(Action::Bottom);
        assert_eq!(
            state
                .tail
                .as_ref()
                .unwrap()
                .selected_row()
                .unwrap()
                .record
                .offset,
            5
        );
    }

    #[test]
    fn ring_buffer_is_bounded() {
        let mut state = with_topics();
        let session = open_orders(&mut state);
        for i in 0..(BUFFER_CAPACITY as i64 + 10) {
            state.on_event(Event::Record {
                session,
                record: record(i, "x"),
            });
        }
        let tail = state.tail.as_ref().unwrap();
        assert_eq!(tail.rows.len(), BUFFER_CAPACITY);
        assert_eq!(tail.rows[0].record.offset, 10);
        assert_eq!(tail.received, BUFFER_CAPACITY as u64 + 10);
    }

    #[test]
    fn message_filter_matches_value() {
        let mut state = with_topics();
        let session = open_orders(&mut state);
        state.on_event(Event::Record {
            session,
            record: record(0, r#"{"status":"PAID"}"#),
        });
        state.on_event(Event::Record {
            session,
            record: record(1, r#"{"status":"SHIPPED"}"#),
        });
        state.update(Action::BeginInput(InputKind::Filter));
        for c in "paid".chars() {
            state.update(Action::InputChar(c));
        }
        state.update(Action::InputSubmit);
        assert_eq!(state.tail.as_ref().unwrap().visible(), vec![0]);
        // Esc clears the filter before leaving the screen.
        assert_eq!(state.update(Action::Back), None);
        assert_eq!(state.screen, Screen::Messages);
        assert_eq!(
            state.update(Action::Back),
            Some(Effect::Kafka(Command::StopTail))
        );
    }

    #[test]
    fn bad_seek_input_flashes_error() {
        let mut state = with_topics();
        open_orders(&mut state);
        state.update(Action::BeginInput(InputKind::SeekOffset));
        state.update(Action::InputChar('x'));
        assert_eq!(state.update(Action::InputSubmit), None);
        assert!(state.flash.as_ref().unwrap().error);
    }
}
