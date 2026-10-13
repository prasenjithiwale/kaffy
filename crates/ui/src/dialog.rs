//! Dialogs for write operations: menus, waiting, previews and progress.
//!
//! Every change follows the same path: the worker returns a plan, the dialog
//! shows exactly what will happen, and nothing is sent until it is confirmed.

use kaffy_kafka::{
    DeleteRecordsPlan, NewTopic, Operation, ProduceRequest, ReplayPlan, ReplayProgress, ResetPlan,
};

#[derive(Debug, Clone)]
pub enum Dialog {
    /// A one-key menu.
    Choice {
        title: String,
        options: Vec<(char, String)>,
        purpose: ChoicePurpose,
    },
    /// Waiting for the worker.
    Pending {
        title: String,
        operation: Operation,
    },
    ResetPlan {
        plan: ResetPlan,
        confirm: Confirm,
    },
    ReplayPlan {
        plan: ReplayPlan,
        /// The filter the replay applies, as typed.
        filter: Option<String>,
        confirm: Confirm,
    },
    ProduceReview {
        request: ProduceRequest,
        confirm: Confirm,
    },
    ReplayProgress {
        plan: ReplayPlan,
        progress: ReplayProgress,
    },
    CreateTopic {
        spec: NewTopic,
        confirm: Confirm,
    },
    /// Permanent: always confirmed by typing the topic name.
    DeleteTopic {
        topic: String,
        partitions: usize,
        confirm: Confirm,
    },
    /// Permanent: always confirmed by typing the topic name.
    DeleteRecords {
        plan: DeleteRecordsPlan,
        confirm: Confirm,
    },
    ConfigChange {
        topic: String,
        name: String,
        old: Option<String>,
        /// `None` resets to the inherited value.
        new: Option<String>,
        confirm: Confirm,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChoicePurpose {
    /// Which of the group's topics to reset.
    ResetScope {
        group: String,
        topics: Vec<String>,
    },
    ResetTarget {
        group: String,
        topic: Option<String>,
    },
    ExportFormat,
    /// The edited template did not parse.
    TemplateError {
        text: String,
    },
}

/// How a change is confirmed: `y`, or typing a name on production profiles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirm {
    /// Name to type; `None` means a single `y`.
    pub expected: Option<String>,
    pub typed: String,
    /// Shown instead of the prompt when the change cannot be made.
    pub blocked: Option<String>,
}

impl Confirm {
    pub fn new(prod: bool, name: &str) -> Self {
        Self {
            expected: prod.then(|| name.to_owned()),
            typed: String::new(),
            blocked: None,
        }
    }

    /// Always requires typing `name`, for changes that cannot be undone.
    pub fn typed(name: &str) -> Self {
        Self::new(true, name)
    }

    pub fn blocked(mut self, reason: impl Into<String>) -> Self {
        self.blocked = Some(reason.into());
        self
    }

    /// Handles a key; returns true when the change is confirmed.
    pub(crate) fn key(&mut self, c: char) -> bool {
        if self.blocked.is_some() {
            return false;
        }
        match &self.expected {
            None => c == 'y' || c == 'Y',
            Some(_) => {
                self.typed.push(c);
                false
            }
        }
    }

    pub(crate) fn enter(&self) -> bool {
        self.blocked.is_none() && self.expected.as_ref().is_some_and(|e| *e == self.typed)
    }

    pub(crate) fn backspace(&mut self) {
        self.typed.pop();
    }
}
