//! Every command a key can trigger: its name (used in the config's `[keys]`
//! table), where it applies, what it is called in the help and the palette,
//! and its default keys.

use kaffy_kafka::StartAt;

use crate::state::{Action, InputKind, Screen};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Everywhere,
    On(&'static [Screen]),
}

impl Scope {
    pub fn includes(self, screen: Screen) -> bool {
        match self {
            Scope::Everywhere => true,
            Scope::On(screens) => screens.contains(&screen),
        }
    }
}

#[derive(Debug)]
pub struct CommandDef {
    /// Name in the config, such as `pause` or `seek-beginning`.
    pub id: &'static str,
    /// Short label for the footer and help.
    pub label: &'static str,
    /// Longer name for the palette.
    pub title: &'static str,
    pub scope: Scope,
    pub keys: &'static [&'static str],
    /// Shown in the footer while there is room.
    pub footer: bool,
    /// Offered in the palette. Movement keys are not.
    pub palette: bool,
    pub action: fn() -> Action,
}

use Screen::*;

const LISTS: &[Screen] = &[Topics, Messages, Groups, Schemas];
const OPENABLE: &[Screen] = &[Topics, Messages, Groups, Schemas];
const REFRESHABLE: &[Screen] = &[
    Topics,
    Messages,
    Groups,
    GroupDetail,
    Schemas,
    SchemaDetail,
    TopicConfig,
    Brokers,
];
const TAIL: &[Screen] = &[Messages, Detail];

macro_rules! cmd {
    ($id:literal, $label:literal, $title:literal, $scope:expr, [$($key:literal),*], $footer:literal, $palette:literal, $action:expr) => {
        CommandDef {
            id: $id,
            label: $label,
            title: $title,
            scope: $scope,
            keys: &[$($key),*],
            footer: $footer,
            palette: $palette,
            action: || $action,
        }
    };
}

/// In footer order: screen commands first, then the ones that work everywhere.
pub static COMMANDS: &[CommandDef] = &[
    cmd!(
        "up",
        "move",
        "Move up",
        Scope::Everywhere,
        ["up", "k"],
        true,
        false,
        Action::Up
    ),
    cmd!(
        "down",
        "move",
        "Move down",
        Scope::Everywhere,
        ["down", "j"],
        false,
        false,
        Action::Down
    ),
    cmd!(
        "open",
        "open",
        "Open the selection",
        Scope::On(OPENABLE),
        ["enter", "right", "l"],
        true,
        false,
        Action::Open
    ),
    cmd!(
        "pause",
        "pause",
        "Pause or resume the live tail",
        Scope::On(&[Messages]),
        ["space"],
        true,
        true,
        Action::TogglePause
    ),
    cmd!(
        "filter",
        "filter",
        "Filter the list",
        Scope::On(LISTS),
        ["/"],
        true,
        true,
        Action::BeginInput(InputKind::Filter)
    ),
    cmd!(
        "config",
        "config",
        "Show the topic's config",
        Scope::On(&[Topics]),
        ["c"],
        true,
        true,
        Action::ShowConfig
    ),
    cmd!(
        "new-topic",
        "new topic",
        "Create a topic",
        Scope::On(&[Topics]),
        ["n"],
        true,
        true,
        Action::CreateTopic
    ),
    cmd!(
        "delete-topic",
        "delete",
        "Delete the topic",
        Scope::On(&[Topics]),
        ["D"],
        true,
        true,
        Action::DeleteTopic
    ),
    cmd!(
        "delete-records",
        "delete records",
        "Delete records before an offset or time",
        Scope::On(&[Topics]),
        ["T"],
        true,
        true,
        Action::Truncate
    ),
    cmd!(
        "produce",
        "produce",
        "Produce a message",
        Scope::On(TAIL),
        ["p"],
        true,
        true,
        Action::Produce
    ),
    cmd!(
        "edit-produce",
        "edit & produce",
        "Edit this message and produce it",
        Scope::On(TAIL),
        ["E"],
        true,
        true,
        Action::EditProduce
    ),
    cmd!(
        "replay",
        "replay",
        "Replay messages into a topic",
        Scope::On(&[Messages]),
        ["X"],
        true,
        true,
        Action::Replay
    ),
    cmd!(
        "export",
        "export",
        "Export the shown messages",
        Scope::On(&[Messages]),
        ["x"],
        true,
        true,
        Action::Export
    ),
    cmd!(
        "seek-beginning",
        "from start",
        "Read from the beginning",
        Scope::On(&[Messages]),
        ["b"],
        false,
        true,
        Action::Seek(StartAt::Beginning)
    ),
    cmd!(
        "seek-end",
        "only new",
        "Read only new messages",
        Scope::On(&[Messages]),
        ["e"],
        false,
        true,
        Action::Seek(StartAt::End)
    ),
    cmd!(
        "seek-offset",
        "to offset",
        "Jump to an offset",
        Scope::On(&[Messages]),
        ["o"],
        false,
        true,
        Action::BeginInput(InputKind::SeekOffset)
    ),
    cmd!(
        "seek-time",
        "to time",
        "Jump to a time",
        Scope::On(&[Messages]),
        ["t"],
        false,
        true,
        Action::BeginInput(InputKind::SeekTimestamp)
    ),
    cmd!(
        "copy-value",
        "copy value",
        "Copy the value",
        Scope::On(TAIL),
        ["y"],
        false,
        true,
        Action::CopyValue
    ),
    cmd!(
        "copy-key",
        "copy key",
        "Copy the key",
        Scope::On(TAIL),
        ["Y"],
        false,
        true,
        Action::CopyKey
    ),
    cmd!(
        "tombstone",
        "tombstone",
        "Produce a tombstone for this key",
        Scope::On(TAIL),
        ["K"],
        false,
        true,
        Action::Tombstone
    ),
    cmd!(
        "reset-offsets",
        "reset offsets",
        "Reset the group's offsets",
        Scope::On(&[Groups, GroupDetail]),
        ["R"],
        true,
        true,
        Action::ResetOffsets
    ),
    cmd!(
        "change-config",
        "change value",
        "Change the setting",
        Scope::On(&[TopicConfig]),
        ["enter"],
        true,
        false,
        Action::Open
    ),
    cmd!(
        "reset-config",
        "reset to inherited",
        "Reset the setting to its inherited value",
        Scope::On(&[TopicConfig]),
        ["u"],
        true,
        true,
        Action::ResetConfig
    ),
    cmd!(
        "new-subject",
        "new subject",
        "Register a new subject",
        Scope::On(&[Schemas]),
        ["n"],
        true,
        true,
        Action::NewSubject
    ),
    cmd!(
        "diff",
        "diff with previous",
        "Diff with the previous version",
        Scope::On(&[SchemaDetail]),
        ["d"],
        true,
        true,
        Action::ToggleDiff
    ),
    cmd!(
        "edit-schema",
        "edit & register",
        "Edit the schema and register it",
        Scope::On(&[SchemaDetail]),
        ["e"],
        true,
        true,
        Action::EditSchema
    ),
    cmd!(
        "refresh",
        "refresh",
        "Refresh",
        Scope::On(REFRESHABLE),
        ["r"],
        true,
        true,
        Action::Refresh
    ),
    cmd!(
        "page-up",
        "page up",
        "Page up",
        Scope::Everywhere,
        ["pgup", "ctrl-u"],
        false,
        false,
        Action::PageUp
    ),
    cmd!(
        "page-down",
        "page down",
        "Page down",
        Scope::Everywhere,
        ["pgdn", "ctrl-d"],
        false,
        false,
        Action::PageDown
    ),
    cmd!(
        "top",
        "first",
        "Go to the first row",
        Scope::Everywhere,
        ["home", "g"],
        false,
        false,
        Action::Top
    ),
    cmd!(
        "bottom",
        "last",
        "Go to the last row (follow newest)",
        Scope::Everywhere,
        ["end", "G"],
        false,
        false,
        Action::Bottom
    ),
    cmd!(
        "back",
        "back",
        "Go back",
        Scope::Everywhere,
        ["esc", "backspace", "left", "h"],
        true,
        false,
        Action::Back
    ),
    cmd!(
        "topics",
        "topics",
        "Topics",
        Scope::Everywhere,
        ["1"],
        false,
        true,
        Action::ShowTopics
    ),
    cmd!(
        "groups",
        "groups",
        "Consumer groups",
        Scope::Everywhere,
        ["2"],
        false,
        true,
        Action::ShowGroups
    ),
    cmd!(
        "schemas",
        "schemas",
        "Schema Registry subjects",
        Scope::Everywhere,
        ["3"],
        false,
        true,
        Action::ShowSchemas
    ),
    cmd!(
        "brokers",
        "brokers",
        "Brokers and cluster health",
        Scope::Everywhere,
        ["4"],
        false,
        true,
        Action::ShowBrokers
    ),
    cmd!(
        "palette",
        "commands",
        "Command palette",
        Scope::Everywhere,
        [":", "ctrl-p"],
        true,
        false,
        Action::OpenPalette
    ),
    cmd!(
        "help",
        "keys",
        "Show the keys of this screen",
        Scope::Everywhere,
        ["?"],
        true,
        true,
        Action::ToggleHelp
    ),
    cmd!(
        "quit",
        "quit",
        "Quit",
        Scope::Everywhere,
        ["q"],
        false,
        true,
        Action::Quit
    ),
];

pub fn find(id: &str) -> Option<&'static CommandDef> {
    COMMANDS.iter().find(|c| c.id == id)
}

/// Commands that apply on `screen`, in footer order.
pub fn on(screen: Screen) -> impl Iterator<Item = &'static CommandDef> {
    COMMANDS.iter().filter(move |c| c.scope.includes(screen))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::keymap::Key;

    #[test]
    fn ids_are_unique_and_keys_parse() {
        let mut ids = HashSet::new();
        for command in COMMANDS {
            assert!(ids.insert(command.id), "duplicate id {}", command.id);
            for key in command.keys {
                assert!(Key::parse(key).is_ok(), "{}: bad key {key}", command.id);
            }
        }
    }

    #[test]
    fn no_default_key_is_bound_twice_on_a_screen() {
        for screen in [
            Topics,
            Messages,
            Detail,
            Groups,
            GroupDetail,
            TopicConfig,
            Schemas,
            SchemaDetail,
            Brokers,
        ] {
            let mut seen = HashSet::new();
            for command in on(screen) {
                for key in command.keys {
                    let key = Key::parse(key).unwrap();
                    assert!(
                        seen.insert(key),
                        "{screen:?}: {key} is bound twice (again by {})",
                        command.id
                    );
                }
            }
        }
    }
}
