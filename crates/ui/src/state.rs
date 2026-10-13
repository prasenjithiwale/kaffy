//! Application state and the reducer that updates it.
//!
//! Key presses and Kafka events both become [`Action`]s. [`AppState::update`]
//! applies an action and may return an [`Effect`] for the app to carry out
//! (send a Kafka command, copy to the clipboard). Rendering only reads state.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use kaffy_kafka::{
    Command, Event, Operation, Record, RecordFilter, ReplayEnd, ResetTarget, StartAt, TopicInfo,
};
use kaffy_schema::Rendered;
use ratatui::widgets::TableState;

use crate::dialog::{ChoicePurpose, Confirm, Dialog};
use crate::export::{self, ExportFormat};
use crate::filter::{Filter, Subject};
use crate::groups::{GroupDetailView, GroupsView};
use crate::{fmt, template};

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
    Groups,
    GroupDetail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Filter,
    SeekOffset,
    SeekTimestamp,
    ResetTime,
    ResetOffset,
    ReplayTarget,
    ReplayStart,
    ReplayEnd,
}

impl InputKind {
    pub fn prompt(self) -> &'static str {
        match self {
            Self::Filter => "/",
            Self::SeekOffset => "seek to offset: ",
            Self::SeekTimestamp => "seek to time (-15m, 2026-10-13 09:30, RFC 3339, epoch ms): ",
            Self::ResetTime => {
                "reset to the first message at or after (-1h, 2026-10-13 09:30, ...): "
            }
            Self::ResetOffset => "reset every partition to offset: ",
            Self::ReplayTarget => "replay into topic: ",
            Self::ReplayStart => "replay from (beginning, an offset, or a time like -1h): ",
            Self::ReplayEnd => "replay until (now, an offset, or a time): ",
        }
    }
}

/// Answers collected by multi-step prompts.
#[derive(Debug, Clone)]
enum Wizard {
    Reset {
        group: String,
        topic: Option<String>,
    },
    Replay {
        source: String,
        target: Option<String>,
        start: Option<StartAt>,
    },
}

/// The template handed to `$EDITOR`, to tell an untouched new message from an edit.
#[derive(Debug, Clone)]
struct Editing {
    template: String,
    fresh: bool,
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
    ShowTopics,
    ShowGroups,
    /// Write a new message in `$EDITOR`.
    Produce,
    /// Edit the selected message in `$EDITOR` and produce the result.
    EditProduce,
    Replay,
    Export,
    ResetOffsets,
    /// Keys while a dialog is open.
    DialogKey(char),
    DialogEnter,
    DialogBackspace,
    DialogCancel,
    /// The text saved in `$EDITOR`.
    Edited(String),
    ToggleHelp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Kafka(Command),
    Copy {
        what: &'static str,
        text: String,
    },
    /// Several effects, in order.
    Many(Vec<Effect>),
    /// Suspend the UI, open `template` in `$EDITOR`, then dispatch [`Action::Edited`].
    Edit {
        template: String,
    },
    /// Write `contents` to `path` in the working directory.
    WriteFile {
        path: String,
        contents: String,
        summary: String,
    },
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
            .map(|k| kaffy_schema::preview(k, record.key_decoded.as_ref(), PREVIEW_CHARS))
            .unwrap_or_default();
        let value_preview = record
            .value
            .as_deref()
            .map(|v| kaffy_schema::preview(v, record.value_decoded.as_ref(), PREVIEW_CHARS))
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
            key_text: record
                .key
                .as_deref()
                .map(|k| search_text(k, record.key_decoded.as_ref()))
                .unwrap_or_default(),
            value_text: record
                .value
                .as_deref()
                .map(|v| search_text(v, record.value_decoded.as_ref()))
                .unwrap_or_default(),
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
    pub key: Option<Rendered>,
    pub value: Option<Rendered>,
    pub scroll: u16,
}

impl DetailView {
    fn new(record: Record) -> Self {
        let key = record
            .key
            .as_deref()
            .map(|k| kaffy_schema::render(k, record.key_decoded.as_ref()));
        let value = record
            .value
            .as_deref()
            .map(|v| kaffy_schema::render(v, record.value_decoded.as_ref()));
        Self {
            key,
            value,
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
    pub groups: GroupsView,
    pub group_detail: Option<GroupDetailView>,
    pub dialog: Option<Dialog>,
    pub show_help: bool,
    wizard: Option<Wizard>,
    editing: Option<Editing>,
    next_session: u64,
    next_request: u64,
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
            groups: GroupsView::default(),
            group_detail: None,
            dialog: None,
            show_help: false,
            wizard: None,
            editing: None,
            next_session: 0,
            next_request: 0,
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
                    tail.push(*record);
                }
            }
            Event::Groups(list) => {
                for group in &list {
                    if let Some(lag) = group.lag {
                        self.groups.record_lag(&group.name, lag);
                    }
                }
                self.groups.list = Some(list);
                self.groups.clamp_selection();
            }
            Event::GroupDetail(detail) => {
                if let Some(view) = self.group_detail.as_mut().filter(|v| v.name == detail.name) {
                    let lag = detail.total_lag();
                    view.detail = Some(detail);
                    let name = view.name.clone();
                    self.groups.record_lag(&name, lag);
                }
            }
            Event::ResetPlan(plan) => {
                if self.pending(Operation::PlanReset) {
                    let mut confirm = Confirm::new(self.prod, &plan.group);
                    if !matches!(
                        plan.state,
                        kaffy_kafka::GroupState::Empty | kaffy_kafka::GroupState::Dead
                    ) {
                        confirm = confirm.blocked(format!(
                            "{} is {}: stop its consumers before resetting offsets",
                            plan.group,
                            plan.state.label()
                        ));
                    }
                    self.dialog = Some(Dialog::ResetPlan { plan, confirm });
                }
            }
            Event::ResetApplied { group, partitions } => {
                if self.pending(Operation::ApplyReset) {
                    self.dialog = None;
                }
                self.flash_info(format!("reset {partitions} partition(s) of {group}"));
                if let Some(view) = self.group_detail.as_mut() {
                    view.requested = None;
                }
            }
            Event::Produced {
                topic,
                partition,
                offset,
                ..
            } => {
                if self.pending(Operation::Produce) {
                    self.dialog = None;
                }
                self.flash_info(format!(
                    "produced to {topic} partition {partition} at offset {offset}"
                ));
            }
            Event::ReplayPlan(plan) => {
                if self.pending(Operation::PlanReplay) {
                    let filter = self
                        .tail
                        .as_ref()
                        .filter(|t| t.filter_error().is_none() && !t.filter.trim().is_empty())
                        .map(|t| t.filter.clone());
                    let confirm = Confirm::new(self.prod, &plan.target);
                    self.dialog = Some(Dialog::ReplayPlan {
                        plan,
                        filter,
                        confirm,
                    });
                }
            }
            Event::ReplayProgress(update) => {
                if let Some(Dialog::ReplayProgress { progress, .. }) = self.dialog.as_mut() {
                    if progress.id == update.id {
                        *progress = update;
                        return;
                    }
                }
                if update.done {
                    match &update.error {
                        Some(e) => self.flash_error(format!("replay stopped: {e}")),
                        None => self.flash_info(format!(
                            "replay finished: {} message(s) produced",
                            update.produced
                        )),
                    }
                }
            }
            Event::Failed { operation, message } => {
                if self.pending(operation) {
                    self.dialog = None;
                }
                if let Some(Dialog::ReplayProgress { progress, .. }) = self.dialog.as_mut() {
                    if operation == Operation::Replay {
                        progress.done = true;
                        progress.error = Some(message.clone());
                    }
                }
                self.flash_error(message);
            }
            Event::Error(message) => self.flash_error(message),
        }
    }

    /// Whether a dialog is waiting for `operation`.
    fn pending(&self, operation: Operation) -> bool {
        matches!(&self.dialog, Some(Dialog::Pending { operation: op, .. }) if *op == operation)
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
                self.refresh_groups_if_due()
            }
            Action::DialogKey(_)
            | Action::DialogEnter
            | Action::DialogBackspace
            | Action::DialogCancel => self.update_dialog(action),
            Action::Edited(text) => self.edited(text),
            Action::ToggleHelp => {
                self.show_help = !self.show_help;
                None
            }
            Action::ShowTopics => self.show_topics(),
            Action::ShowGroups => self.show_groups(),
            Action::BeginInput(kind) => {
                let current = match kind {
                    InputKind::Filter => self.filter().to_owned(),
                    InputKind::ReplayTarget => match &self.wizard {
                        Some(Wizard::Replay { source, .. }) => source.clone(),
                        _ => String::new(),
                    },
                    InputKind::ReplayStart => "beginning".to_owned(),
                    InputKind::ReplayEnd => "now".to_owned(),
                    _ => String::new(),
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
                self.wizard = None;
                None
            }
            Action::InputSubmit => self.submit_input(),
            _ => match self.screen {
                Screen::Topics => self.update_topics(action),
                Screen::Messages => self.update_messages(action),
                Screen::Detail => self.update_detail(action),
                Screen::Groups => self.update_groups(action),
                Screen::GroupDetail => self.update_group_detail(action),
            },
        }
    }

    fn show_topics(&mut self) -> Option<Effect> {
        match self.screen {
            Screen::Topics => None,
            Screen::Messages | Screen::Detail => self.leave_tail(Screen::Topics),
            Screen::Groups | Screen::GroupDetail => {
                self.group_detail = None;
                self.screen = Screen::Topics;
                Some(Effect::Kafka(Command::ListTopics))
            }
        }
    }

    fn show_groups(&mut self) -> Option<Effect> {
        let leave = match self.screen {
            Screen::Groups => return None,
            Screen::Messages | Screen::Detail => self.leave_tail(Screen::Groups),
            Screen::Topics | Screen::GroupDetail => None,
        };
        self.screen = Screen::Groups;
        self.group_detail = None;
        self.groups.requested = Some(Instant::now());
        let list = Effect::Kafka(Command::ListGroups);
        Some(match leave {
            Some(stop) => Effect::Many(vec![stop, list]),
            None => list,
        })
    }

    fn leave_tail(&mut self, to: Screen) -> Option<Effect> {
        self.tail = None;
        self.detail = None;
        self.screen = to;
        Some(Effect::Kafka(Command::StopTail))
    }

    fn refresh_groups_if_due(&mut self) -> Option<Effect> {
        if self.dialog.is_some() {
            return None;
        }
        match self.screen {
            Screen::Groups if self.groups.due() => {
                self.groups.requested = Some(Instant::now());
                Some(Effect::Kafka(Command::ListGroups))
            }
            Screen::GroupDetail => {
                let view = self.group_detail.as_mut()?;
                if !view.due() {
                    return None;
                }
                view.requested = Some(Instant::now());
                Some(Effect::Kafka(Command::DescribeGroup(view.name.clone())))
            }
            _ => None,
        }
    }

    fn update_groups(&mut self, action: Action) -> Option<Effect> {
        let len = self.groups.visible().len();
        let selected = self.groups.table.selected().unwrap_or(0);
        let table = &mut self.groups.table;
        let mut select = |i: usize| table.select(Some(i.min(len.saturating_sub(1))));
        match action {
            Action::Up => select(selected.saturating_sub(1)),
            Action::Down => select(selected + 1),
            Action::PageUp => select(selected.saturating_sub(10)),
            Action::PageDown => select(selected + 10),
            Action::Top => select(0),
            Action::Bottom => select(len),
            Action::Refresh => {
                self.groups.requested = Some(Instant::now());
                return Some(Effect::Kafka(Command::ListGroups));
            }
            Action::Back if !self.groups.filter.is_empty() => self.set_filter(String::new()),
            Action::Back => return self.show_topics(),
            Action::Open => {
                let name = self.groups.selected()?.name.clone();
                return Some(self.open_group(name));
            }
            Action::ResetOffsets => {
                let group = self.groups.selected()?.clone();
                return self.begin_reset(group.name, group.topics);
            }
            _ => {}
        }
        None
    }

    fn open_group(&mut self, name: String) -> Effect {
        let mut view = GroupDetailView::new(name.clone());
        view.requested = Some(Instant::now());
        self.group_detail = Some(view);
        self.screen = Screen::GroupDetail;
        Effect::Kafka(Command::DescribeGroup(name))
    }

    fn update_group_detail(&mut self, action: Action) -> Option<Effect> {
        let view = self.group_detail.as_mut()?;
        let len = view.detail.as_ref().map_or(0, |d| d.partitions.len());
        let selected = view.table.selected().unwrap_or(0);
        let mut select = |i: usize| view.table.select(Some(i.min(len.saturating_sub(1))));
        match action {
            Action::Up => select(selected.saturating_sub(1)),
            Action::Down => select(selected + 1),
            Action::PageUp => select(selected.saturating_sub(10)),
            Action::PageDown => select(selected + 10),
            Action::Top => select(0),
            Action::Bottom => select(len),
            Action::Refresh => {
                view.requested = Some(Instant::now());
                return Some(Effect::Kafka(Command::DescribeGroup(view.name.clone())));
            }
            Action::Back => {
                self.group_detail = None;
                self.screen = Screen::Groups;
                self.groups.requested = Some(Instant::now());
                return Some(Effect::Kafka(Command::ListGroups));
            }
            Action::ResetOffsets => {
                let (name, topics) = (view.name.clone(), view.topics());
                return self.begin_reset(name, topics);
            }
            _ => {}
        }
        None
    }

    /// Writes need a writable profile; says why not otherwise.
    fn check_writable(&mut self, what: &str) -> bool {
        if self.read_only {
            self.flash_error(format!(
                "{what} is disabled: profile {} is read-only (set read_only = false to allow changes)",
                self.profile
            ));
        }
        !self.read_only
    }

    fn begin_reset(&mut self, group: String, topics: Vec<String>) -> Option<Effect> {
        if !self.check_writable("resetting offsets") {
            return None;
        }
        if topics.len() > 1 {
            let mut options = vec![('a', format!("all {} topics", topics.len()))];
            options.extend(
                topics
                    .iter()
                    .take(9)
                    .enumerate()
                    .map(|(i, t)| (char::from(b'1' + i as u8), t.clone())),
            );
            self.dialog = Some(Dialog::Choice {
                title: format!("Reset offsets of {group}: which topics?"),
                options,
                purpose: ChoicePurpose::ResetScope { group, topics },
            });
        } else {
            self.choose_reset_target(group, topics.into_iter().next());
        }
        None
    }

    fn choose_reset_target(&mut self, group: String, topic: Option<String>) {
        let scope = topic
            .as_deref()
            .map_or_else(|| "all topics".to_owned(), str::to_owned);
        self.dialog = Some(Dialog::Choice {
            title: format!("Reset {group} on {scope} to:"),
            options: vec![
                (
                    'e',
                    "earliest: reprocess everything still retained".to_owned(),
                ),
                ('l', "latest: skip everything not yet consumed".to_owned()),
                ('t', "a time…".to_owned()),
                ('o', "an offset…".to_owned()),
            ],
            purpose: ChoicePurpose::ResetTarget { group, topic },
        });
    }

    fn plan_reset(
        &mut self,
        group: String,
        topic: Option<String>,
        target: ResetTarget,
    ) -> Option<Effect> {
        self.dialog = Some(Dialog::Pending {
            title: format!("Working out new offsets for {group}…"),
            operation: Operation::PlanReset,
        });
        Some(Effect::Kafka(Command::PlanReset {
            group,
            topic,
            target,
        }))
    }

    fn update_dialog(&mut self, action: Action) -> Option<Effect> {
        let dialog = self.dialog.as_mut()?;
        match (dialog, action) {
            (Dialog::Pending { .. }, _) => None,
            (Dialog::ReplayProgress { progress, .. }, action) => {
                if progress.done {
                    if matches!(action, Action::DialogCancel | Action::DialogEnter) {
                        self.dialog = None;
                    }
                    return None;
                }
                match action {
                    Action::DialogKey('c') => Some(Effect::Kafka(Command::CancelReplay)),
                    // Esc hides the dialog; the replay keeps running and reports when done.
                    Action::DialogCancel => {
                        self.dialog = None;
                        self.flash_info("replay continues in the background");
                        None
                    }
                    _ => None,
                }
            }
            (_, Action::DialogCancel) => {
                if let Some(Dialog::Choice {
                    purpose: ChoicePurpose::TemplateError { .. },
                    ..
                }) = &self.dialog
                {
                    self.flash_info("discarded the edited message");
                }
                self.dialog = None;
                None
            }
            (
                Dialog::Choice {
                    options, purpose, ..
                },
                Action::DialogKey(c),
            ) => {
                if !options.iter().any(|(k, _)| *k == c) {
                    return None;
                }
                let purpose = purpose.clone();
                self.dialog = None;
                self.choose(purpose, c)
            }
            (
                Dialog::ResetPlan { confirm, .. }
                | Dialog::ReplayPlan { confirm, .. }
                | Dialog::ProduceReview { confirm, .. },
                action,
            ) => {
                let confirmed = match action {
                    Action::DialogKey(c) => confirm.key(c),
                    Action::DialogEnter => confirm.enter(),
                    Action::DialogBackspace => {
                        confirm.backspace();
                        false
                    }
                    _ => false,
                };
                if confirmed { self.confirmed() } else { None }
            }
            _ => None,
        }
    }

    fn choose(&mut self, purpose: ChoicePurpose, c: char) -> Option<Effect> {
        match purpose {
            ChoicePurpose::ResetScope { group, topics } => {
                let topic = match c {
                    'a' => None,
                    n => topics.get((n as u8 - b'1') as usize).cloned(),
                };
                self.choose_reset_target(group, topic);
                None
            }
            ChoicePurpose::ResetTarget { group, topic } => match c {
                'e' => self.plan_reset(group, topic, ResetTarget::Earliest),
                'l' => self.plan_reset(group, topic, ResetTarget::Latest),
                't' | 'o' => {
                    self.wizard = Some(Wizard::Reset { group, topic });
                    let kind = if c == 't' {
                        InputKind::ResetTime
                    } else {
                        InputKind::ResetOffset
                    };
                    self.update(Action::BeginInput(kind))
                }
                _ => None,
            },
            ChoicePurpose::ExportFormat => {
                let format = match c {
                    'j' => ExportFormat::Json,
                    'n' => ExportFormat::Ndjson,
                    _ => ExportFormat::Csv,
                };
                self.export(format)
            }
            ChoicePurpose::TemplateError { text } => {
                self.editing = Some(Editing {
                    template: text.clone(),
                    fresh: false,
                });
                Some(Effect::Edit { template: text })
            }
        }
    }

    /// The open plan or review was confirmed: send it.
    fn confirmed(&mut self) -> Option<Effect> {
        match self.dialog.take()? {
            Dialog::ResetPlan { plan, .. } => {
                self.dialog = Some(Dialog::Pending {
                    title: format!("Committing new offsets for {}…", plan.group),
                    operation: Operation::ApplyReset,
                });
                Some(Effect::Kafka(Command::ApplyReset {
                    offsets: plan.offsets(),
                    group: plan.group,
                }))
            }
            Dialog::ReplayPlan { plan, filter, .. } => {
                self.next_request += 1;
                let id = self.next_request;
                let filter = filter
                    .and_then(|text| Filter::parse(&text, fmt::now_ms()).ok())
                    .map(|filter| {
                        RecordFilter::new(move |record: &Record| {
                            filter.matches(&Row::new(record.clone()).subject())
                        })
                    });
                self.dialog = Some(Dialog::ReplayProgress {
                    plan: plan.clone(),
                    progress: kaffy_kafka::ReplayProgress {
                        id,
                        scanned: 0,
                        produced: 0,
                        done: false,
                        error: None,
                    },
                });
                Some(Effect::Kafka(Command::Replay { id, plan, filter }))
            }
            Dialog::ProduceReview { request, .. } => {
                self.dialog = Some(Dialog::Pending {
                    title: format!("Producing to {}…", request.topic),
                    operation: Operation::Produce,
                });
                Some(Effect::Kafka(Command::Produce(request)))
            }
            other => {
                self.dialog = Some(other);
                None
            }
        }
    }

    fn begin_produce(&mut self, record: Option<Record>) -> Option<Effect> {
        if !self.check_writable("producing") {
            return None;
        }
        let (template, fresh) = match &record {
            Some(record) => (template::from_record(record), false),
            None => (template::new_message(&self.tail.as_ref()?.topic), true),
        };
        self.editing = Some(Editing {
            template: template.clone(),
            fresh,
        });
        Some(Effect::Edit { template })
    }

    fn edited(&mut self, text: String) -> Option<Effect> {
        let editing = self.editing.take()?;
        if editing.fresh && text == editing.template {
            self.flash_info("nothing to produce: the template was not changed");
            return None;
        }
        self.next_request += 1;
        match template::parse(&text, self.next_request) {
            Ok(None) => {
                self.flash_info("cancelled: the message was left empty");
                None
            }
            Ok(Some(request)) => {
                let confirm = Confirm::new(self.prod, &request.topic);
                self.dialog = Some(Dialog::ProduceReview { request, confirm });
                None
            }
            Err(error) => {
                self.dialog = Some(Dialog::Choice {
                    title: format!("The message can't be sent: {error}"),
                    options: vec![('e', "edit it again".to_owned())],
                    purpose: ChoicePurpose::TemplateError { text },
                });
                None
            }
        }
    }

    fn begin_replay(&mut self) -> Option<Effect> {
        if !self.check_writable("replaying") {
            return None;
        }
        let source = self.tail.as_ref()?.topic.clone();
        self.wizard = Some(Wizard::Replay {
            source,
            target: None,
            start: None,
        });
        self.update(Action::BeginInput(InputKind::ReplayTarget))
    }

    fn export(&mut self, format: ExportFormat) -> Option<Effect> {
        let tail = self.tail.as_ref()?;
        let records: Vec<&Record> = tail
            .visible()
            .into_iter()
            .map(|i| &tail.rows[i].record)
            .collect();
        if records.is_empty() {
            self.flash_error("nothing to export: no messages are shown");
            return None;
        }
        let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
        let path = format!(
            "kaffy-{}-{stamp}.{}",
            sanitize(&tail.topic),
            format.extension()
        );
        let summary = format!("{} message(s)", records.len());
        Some(Effect::WriteFile {
            contents: export::export(&records, format),
            path,
            summary,
        })
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
            Action::Produce => return self.begin_produce(None),
            Action::EditProduce => {
                let record = tail.selected_row()?.record.clone();
                return self.begin_produce(Some(record));
            }
            Action::Replay => return self.begin_replay(),
            Action::Export => {
                self.dialog = Some(Dialog::Choice {
                    title: format!("Export the {} shown message(s) as:", tail.visible().len()),
                    options: vec![
                        ('j', "JSON (one array)".to_owned()),
                        ('n', "NDJSON (one message per line)".to_owned()),
                        ('c', "CSV".to_owned()),
                    ],
                    purpose: ChoicePurpose::ExportFormat,
                });
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
            Action::EditProduce => {
                let record = detail.record.clone();
                return self.begin_produce(Some(record));
            }
            Action::Produce => return self.begin_produce(None),
            _ => {}
        }
        None
    }

    fn submit_input(&mut self) -> Option<Effect> {
        let input = self.input.take()?;
        let text = input.buffer.trim();
        let now = fmt::now_ms();
        match (input.kind, self.wizard.take()) {
            (InputKind::ResetTime, Some(Wizard::Reset { group, topic })) => {
                return match fmt::parse_timestamp(text, now) {
                    Some(ms) => self.plan_reset(group, topic, ResetTarget::Timestamp(ms)),
                    None => {
                        self.flash_error(format!("not a time: {text:?}"));
                        None
                    }
                };
            }
            (InputKind::ResetOffset, Some(Wizard::Reset { group, topic })) => {
                return match text.parse::<i64>() {
                    Ok(n) if n >= 0 => self.plan_reset(group, topic, ResetTarget::Offset(n)),
                    _ => {
                        self.flash_error(format!("not an offset: {text:?}"));
                        None
                    }
                };
            }
            (InputKind::ReplayTarget, Some(Wizard::Replay { source, .. })) => {
                if text.is_empty() {
                    self.flash_error("replay needs a target topic");
                    return None;
                }
                self.wizard = Some(Wizard::Replay {
                    source,
                    target: Some(text.to_owned()),
                    start: None,
                });
                return self.update(Action::BeginInput(InputKind::ReplayStart));
            }
            (InputKind::ReplayStart, Some(Wizard::Replay { source, target, .. })) => {
                let start = match text {
                    "beginning" | "earliest" | "" => Some(StartAt::Beginning),
                    "end" | "now" | "latest" => Some(StartAt::End),
                    _ => match text.parse::<i64>() {
                        Ok(n) if n >= 0 => Some(StartAt::Offset(n)),
                        _ => fmt::parse_timestamp(text, now).map(StartAt::Timestamp),
                    },
                };
                let Some(start) = start else {
                    self.flash_error(format!(
                        "not a start: {text:?} (beginning, an offset or a time)"
                    ));
                    return None;
                };
                self.wizard = Some(Wizard::Replay {
                    source,
                    target,
                    start: Some(start),
                });
                return self.update(Action::BeginInput(InputKind::ReplayEnd));
            }
            (
                InputKind::ReplayEnd,
                Some(Wizard::Replay {
                    source,
                    target: Some(target),
                    start: Some(start),
                }),
            ) => {
                let end = match text {
                    "now" | "end" | "latest" | "" => Some(ReplayEnd::Now),
                    _ => match text.parse::<i64>() {
                        Ok(n) if n >= 0 => Some(ReplayEnd::Offset(n)),
                        _ => fmt::parse_timestamp(text, now).map(ReplayEnd::Timestamp),
                    },
                };
                let Some(end) = end else {
                    self.flash_error(format!("not an end: {text:?} (now, an offset or a time)"));
                    return None;
                };
                self.dialog = Some(Dialog::Pending {
                    title: format!("Counting messages to replay from {source}…"),
                    operation: Operation::PlanReplay,
                });
                return Some(Effect::Kafka(Command::PlanReplay {
                    source,
                    target,
                    start,
                    end,
                }));
            }
            (_, wizard) => self.wizard = wizard,
        }
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
            _ => return None,
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
            (Screen::Groups | Screen::GroupDetail, _) => &self.groups.filter,
            _ => &self.topic_filter,
        }
    }

    fn set_filter(&mut self, value: String) {
        match (self.screen, self.tail.as_mut()) {
            (Screen::Messages | Screen::Detail, Some(tail)) => tail.set_filter(value),
            (Screen::Groups | Screen::GroupDetail, _) => {
                self.groups.filter = value;
                self.groups.clamp_selection();
            }
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

/// A file-name-safe version of a topic name.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// What the filter searches: decoded JSON, or for payloads that failed to
/// decode, the same "schema N not decoded" wording the table shows plus the error.
fn search_text(bytes: &[u8], decoded: Option<&kaffy_schema::Decoded>) -> String {
    match decoded {
        Some(kaffy_schema::Decoded {
            schema_id,
            result: Err(error),
            ..
        }) => format!("schema {schema_id} not decoded: {error}"),
        _ => kaffy_schema::plain_text(bytes, decoded),
    }
}

fn copy(record: &Record, key: bool) -> Option<Effect> {
    let (what, bytes, decoded) = if key {
        ("key", record.key.as_deref(), record.key_decoded.as_ref())
    } else {
        (
            "value",
            record.value.as_deref(),
            record.value_decoded.as_ref(),
        )
    };
    let bytes = bytes?;
    let text = match (decoded, kaffy_schema::detect(bytes)) {
        (
            Some(kaffy_schema::Decoded {
                result: Ok(value), ..
            }),
            _,
        ) => serde_json::to_string_pretty(value).expect("a Value always serializes"),
        (None, kaffy_schema::Format::Json | kaffy_schema::Format::Text) => {
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
            ..Default::default()
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
            record: Box::new(record(1, "old")),
        });
        state.on_event(Event::Record {
            session: second,
            record: Box::new(record(0, "new")),
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
                record: Box::new(record(i, "x")),
            });
        }
        state.update(Action::Up);
        let tail = state.tail.as_ref().unwrap();
        assert!(!tail.follow);
        assert_eq!(tail.selected_row().unwrap().record.offset, 3);

        // New records don't move a pinned selection.
        state.on_event(Event::Record {
            session,
            record: Box::new(record(5, "x")),
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
                record: Box::new(record(i, "x")),
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
            record: Box::new(record(0, r#"{"status":"PAID"}"#)),
        });
        state.on_event(Event::Record {
            session,
            record: Box::new(record(1, r#"{"status":"SHIPPED"}"#)),
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
    fn filter_searches_decoded_payloads() {
        let mut state = with_topics();
        let session = open_orders(&mut state);
        let decoded = |result| kaffy_schema::Decoded {
            schema_id: 7,
            schema_type: Some(kaffy_schema::SchemaType::Avro),
            result,
        };
        let avro = |offset, result| Record {
            topic: "orders".to_owned(),
            offset,
            value: Some(vec![0, 0, 0, 0, 7, 2]),
            value_decoded: Some(decoded(result)),
            ..Default::default()
        };
        state.on_event(Event::Record {
            session,
            record: Box::new(avro(0, Ok(serde_json::json!({"status": "PAID"})))),
        });
        state.on_event(Event::Record {
            session,
            record: Box::new(avro(1, Err("schema registry: 404 Not Found".to_owned()))),
        });
        let visible_for = |state: &mut AppState, filter: &str| {
            state.update(Action::BeginInput(InputKind::Filter));
            for _ in 0..40 {
                state.update(Action::InputBackspace);
            }
            for c in filter.chars() {
                state.update(Action::InputChar(c));
            }
            state.update(Action::InputSubmit);
            state.tail.as_ref().unwrap().visible()
        };
        assert_eq!(visible_for(&mut state, r#"~"status":"PAID""#), vec![0]);
        assert_eq!(visible_for(&mut state, "not decoded"), vec![1]);
        assert_eq!(visible_for(&mut state, "404"), vec![1]);
    }

    fn writable(prod: bool) -> AppState {
        let mut state = AppState::new("test", false, prod);
        state.on_event(Event::Metadata {
            brokers: 1,
            topics: vec![topic("orders"), topic("payments")],
        });
        state
    }

    fn group_list(state: &mut AppState, group_state: kaffy_kafka::GroupState) {
        state.on_event(Event::Groups(vec![kaffy_kafka::GroupSummary {
            name: "billing".into(),
            state: group_state,
            members: 0,
            topics: vec!["orders".into()],
            lag: Some(10),
        }]));
    }

    fn reset_plan(group_state: kaffy_kafka::GroupState) -> kaffy_kafka::ResetPlan {
        kaffy_kafka::ResetPlan {
            group: "billing".into(),
            state: group_state,
            target: ResetTarget::Earliest,
            rows: vec![kaffy_kafka::ResetRow {
                topic: "orders".into(),
                partition: 0,
                current: Some(40),
                new: 0,
                end: 50,
            }],
        }
    }

    #[test]
    fn reset_is_previewed_then_applied() {
        let mut state = writable(false);
        assert!(matches!(
            state.update(Action::ShowGroups),
            Some(Effect::Kafka(Command::ListGroups))
        ));
        group_list(&mut state, kaffy_kafka::GroupState::Empty);
        // One topic: straight to the target menu.
        assert_eq!(state.update(Action::ResetOffsets), None);
        assert!(matches!(
            &state.dialog,
            Some(Dialog::Choice {
                purpose: ChoicePurpose::ResetTarget { .. },
                ..
            })
        ));
        assert_eq!(
            state.update(Action::DialogKey('z')),
            None,
            "unknown keys are ignored"
        );
        assert_eq!(
            state.update(Action::DialogKey('e')),
            Some(Effect::Kafka(Command::PlanReset {
                group: "billing".into(),
                topic: Some("orders".into()),
                target: ResetTarget::Earliest,
            }))
        );
        state.on_event(Event::ResetPlan(reset_plan(kaffy_kafka::GroupState::Empty)));
        assert!(matches!(&state.dialog, Some(Dialog::ResetPlan { .. })));
        let Some(Effect::Kafka(Command::ApplyReset { group, offsets })) =
            state.update(Action::DialogKey('y'))
        else {
            panic!("y should apply the plan");
        };
        assert_eq!((group.as_str(), offsets[0].offset), ("billing", 0));
        state.on_event(Event::ResetApplied {
            group,
            partitions: 1,
        });
        assert!(state.dialog.is_none());
        assert!(!state.flash.as_ref().unwrap().error);
    }

    #[test]
    fn reset_of_active_group_is_blocked() {
        let mut state = writable(false);
        state.update(Action::ShowGroups);
        group_list(&mut state, kaffy_kafka::GroupState::Stable);
        state.update(Action::ResetOffsets);
        state.update(Action::DialogKey('l'));
        state.on_event(Event::ResetPlan(reset_plan(
            kaffy_kafka::GroupState::Stable,
        )));
        let Some(Dialog::ResetPlan { confirm, .. }) = &state.dialog else {
            panic!("plan shown")
        };
        assert!(
            confirm
                .blocked
                .as_deref()
                .unwrap()
                .contains("stop its consumers")
        );
        assert_eq!(state.update(Action::DialogKey('y')), None);
        assert_eq!(state.update(Action::DialogCancel), None);
        assert!(state.dialog.is_none());
    }

    #[test]
    fn production_needs_the_name_typed() {
        let mut state = writable(true);
        state.update(Action::ShowGroups);
        group_list(&mut state, kaffy_kafka::GroupState::Empty);
        state.update(Action::ResetOffsets);
        state.update(Action::DialogKey('e'));
        state.on_event(Event::ResetPlan(reset_plan(kaffy_kafka::GroupState::Empty)));
        assert_eq!(
            state.update(Action::DialogKey('y')),
            None,
            "y alone is not enough"
        );
        state.update(Action::DialogBackspace);
        for c in "billin".chars() {
            state.update(Action::DialogKey(c));
        }
        assert_eq!(state.update(Action::DialogEnter), None, "partial name");
        state.update(Action::DialogKey('g'));
        assert!(matches!(
            state.update(Action::DialogEnter),
            Some(Effect::Kafka(Command::ApplyReset { .. }))
        ));
    }

    #[test]
    fn read_only_profiles_refuse_writes() {
        let mut state = with_topics();
        open_orders(&mut state);
        for action in [Action::Produce, Action::Replay] {
            assert_eq!(state.update(action), None);
            assert!(state.flash.as_ref().unwrap().text.contains("read-only"));
        }
        state.update(Action::ShowGroups);
        group_list(&mut state, kaffy_kafka::GroupState::Empty);
        assert_eq!(state.update(Action::ResetOffsets), None);
        assert!(state.dialog.is_none());
        // Export does not write to Kafka, so it stays available.
        state.update(Action::ShowTopics);
        open_orders(&mut state);
        state.update(Action::Export);
        assert!(state.dialog.is_some());
    }

    #[test]
    fn produce_goes_through_the_editor_and_a_review() {
        let mut state = writable(false);
        state.update(Action::Open);
        let Some(Effect::Edit { template }) = state.update(Action::Produce) else {
            panic!("p opens the editor");
        };
        assert_eq!(state.update(Action::Edited(template.clone())), None);
        assert!(state.flash.as_ref().unwrap().text.contains("not changed"));

        let Some(Effect::Edit { template }) = state.update(Action::Produce) else {
            panic!()
        };
        // A broken edit can be reopened with the text kept.
        assert_eq!(state.update(Action::Edited("no separator".into())), None);
        assert_eq!(
            state.update(Action::DialogKey('e')),
            Some(Effect::Edit {
                template: "no separator".into()
            })
        );
        let edited = template.replace("key: \n", "key: cust-9\n") + "{\"id\": 1}";
        assert_eq!(state.update(Action::Edited(edited)), None);
        let Some(Dialog::ProduceReview { request, .. }) = &state.dialog else {
            panic!("review shown")
        };
        assert_eq!(request.key.as_deref(), Some(&b"cust-9"[..]));
        let Some(Effect::Kafka(Command::Produce(request))) = state.update(Action::DialogKey('y'))
        else {
            panic!("y sends it");
        };
        state.on_event(Event::Produced {
            id: request.id,
            topic: "orders".into(),
            partition: 1,
            offset: 77,
        });
        assert!(state.dialog.is_none());
        assert!(state.flash.as_ref().unwrap().text.contains("offset 77"));
    }

    #[test]
    fn replay_wizard_carries_the_filter() {
        let mut state = writable(false);
        let session = open_orders(&mut state);
        for (i, value) in ["PAID", "SHIPPED", "PAID"].iter().enumerate() {
            state.on_event(Event::Record {
                session,
                record: Box::new(record(i as i64, value)),
            });
        }
        state.update(Action::BeginInput(InputKind::Filter));
        for c in "paid".chars() {
            state.update(Action::InputChar(c));
        }
        state.update(Action::InputSubmit);

        state.update(Action::Replay);
        assert_eq!(
            state.input.as_ref().unwrap().buffer,
            "orders",
            "target defaults to the source"
        );
        state.update(Action::InputSubmit);
        assert_eq!(state.input.as_ref().unwrap().buffer, "beginning");
        state.update(Action::InputSubmit);
        assert_eq!(state.input.as_ref().unwrap().buffer, "now");
        assert_eq!(
            state.update(Action::InputSubmit),
            Some(Effect::Kafka(Command::PlanReplay {
                source: "orders".into(),
                target: "orders".into(),
                start: StartAt::Beginning,
                end: ReplayEnd::Now,
            }))
        );
        let plan = kaffy_kafka::ReplayPlan {
            source: "orders".into(),
            target: "orders".into(),
            ranges: vec![kaffy_kafka::PartitionRange {
                partition: 0,
                from: 0,
                to: 3,
            }],
            target_partitions: 1,
            keep_partitions: true,
        };
        state.on_event(Event::ReplayPlan(plan));
        let Some(Dialog::ReplayPlan { filter, .. }) = &state.dialog else {
            panic!("plan shown")
        };
        assert_eq!(filter.as_deref(), Some("paid"));
        let Some(Effect::Kafka(Command::Replay {
            id,
            filter: Some(filter),
            ..
        })) = state.update(Action::DialogKey('y'))
        else {
            panic!("y starts the replay with the filter");
        };
        assert!(filter.matches(&record(0, "PAID")));
        assert!(!filter.matches(&record(1, "SHIPPED")));

        state.on_event(Event::ReplayProgress(kaffy_kafka::ReplayProgress {
            id,
            scanned: 3,
            produced: 2,
            done: true,
            error: None,
        }));
        let Some(Dialog::ReplayProgress { progress, .. }) = &state.dialog else {
            panic!("progress shown")
        };
        assert_eq!(progress.produced, 2);
        state.update(Action::DialogCancel);
        assert!(state.dialog.is_none());
    }

    #[test]
    fn export_writes_the_shown_messages() {
        let mut state = with_topics();
        let session = open_orders(&mut state);
        for i in 0..3 {
            state.on_event(Event::Record {
                session,
                record: Box::new(record(i, "x")),
            });
        }
        state.update(Action::Export);
        let Some(Effect::WriteFile {
            path,
            contents,
            summary,
        }) = state.update(Action::DialogKey('n'))
        else {
            panic!("n exports NDJSON");
        };
        assert!(
            path.starts_with("kaffy-orders-") && path.ends_with(".ndjson"),
            "{path}"
        );
        assert_eq!(contents.lines().count(), 3);
        assert_eq!(summary, "3 message(s)");
    }

    #[test]
    fn groups_refresh_while_open() {
        let mut state = with_topics();
        state.update(Action::ShowGroups);
        assert_eq!(state.update(Action::Tick), None, "just refreshed");
        state.groups.requested = None;
        assert_eq!(
            state.update(Action::Tick),
            Some(Effect::Kafka(Command::ListGroups))
        );
        group_list(&mut state, kaffy_kafka::GroupState::Stable);
        assert_eq!(
            state.groups.history["billing"]
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert_eq!(
            state.update(Action::Open),
            Some(Effect::Kafka(Command::DescribeGroup("billing".into())))
        );
        state.group_detail.as_mut().unwrap().requested = None;
        assert_eq!(
            state.update(Action::Tick),
            Some(Effect::Kafka(Command::DescribeGroup("billing".into())))
        );
        assert!(matches!(
            state.update(Action::Back),
            Some(Effect::Kafka(Command::ListGroups))
        ));
        assert_eq!(state.screen, Screen::Groups);
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
