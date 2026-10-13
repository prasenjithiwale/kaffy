use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use kaffy_kafka::StartAt;
use kaffy_ui::{Action, AppState, InputKind, Screen};

pub fn map_key(state: &AppState, key: KeyEvent) -> Option<Action> {
    // Alt+key would otherwise act as the plain key; nothing is bound to Alt yet.
    if key.modifiers.contains(KeyModifiers::ALT) {
        return None;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c') => Some(Action::Quit),
            KeyCode::Char('d') => Some(Action::PageDown),
            KeyCode::Char('u') => Some(Action::PageUp),
            _ => None,
        };
    }

    if state.show_help {
        return Some(Action::ToggleHelp);
    }

    if state.dialog.is_some() {
        return match key.code {
            KeyCode::Esc => Some(Action::DialogCancel),
            KeyCode::Enter => Some(Action::DialogEnter),
            KeyCode::Backspace => Some(Action::DialogBackspace),
            KeyCode::Char(c) => Some(Action::DialogKey(c)),
            _ => None,
        };
    }

    if state.input.is_some() {
        return match key.code {
            KeyCode::Enter => Some(Action::InputSubmit),
            KeyCode::Esc => Some(Action::InputCancel),
            KeyCode::Backspace => Some(Action::InputBackspace),
            KeyCode::Char(c) => Some(Action::InputChar(c)),
            _ => None,
        };
    }

    let common = match key.code {
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Char('?') => Some(Action::ToggleHelp),
        KeyCode::Char('1') => Some(Action::ShowTopics),
        KeyCode::Char('2') => Some(Action::ShowGroups),
        KeyCode::Char('3') => Some(Action::ShowSchemas),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::Up),
        KeyCode::Down | KeyCode::Char('j') => Some(Action::Down),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Top),
        KeyCode::End | KeyCode::Char('G') => Some(Action::Bottom),
        KeyCode::Esc | KeyCode::Backspace | KeyCode::Left | KeyCode::Char('h') => {
            Some(Action::Back)
        }
        _ => None,
    };
    if common.is_some() {
        return common;
    }

    match (state.screen, key.code) {
        (
            Screen::Topics | Screen::Messages,
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l'),
        ) => Some(Action::Open),
        (Screen::Groups, KeyCode::Enter | KeyCode::Right | KeyCode::Char('l')) => {
            Some(Action::Open)
        }
        (Screen::Topics | Screen::Messages | Screen::Groups, KeyCode::Char('/')) => {
            Some(Action::BeginInput(InputKind::Filter))
        }
        (
            Screen::Topics | Screen::Messages | Screen::Groups | Screen::GroupDetail,
            KeyCode::Char('r'),
        ) => Some(Action::Refresh),
        (Screen::Groups | Screen::GroupDetail, KeyCode::Char('R')) => Some(Action::ResetOffsets),
        (Screen::Topics, KeyCode::Char('n')) => Some(Action::CreateTopic),
        (Screen::Schemas, KeyCode::Enter | KeyCode::Right | KeyCode::Char('l')) => {
            Some(Action::Open)
        }
        (Screen::Schemas, KeyCode::Char('/')) => Some(Action::BeginInput(InputKind::Filter)),
        (Screen::Schemas, KeyCode::Char('n')) => Some(Action::NewSubject),
        (Screen::Schemas | Screen::SchemaDetail, KeyCode::Char('r')) => Some(Action::Refresh),
        (Screen::SchemaDetail, KeyCode::Char('d')) => Some(Action::ToggleDiff),
        (Screen::SchemaDetail, KeyCode::Char('e')) => Some(Action::EditSchema),
        (Screen::Topics, KeyCode::Char('D')) => Some(Action::DeleteTopic),
        (Screen::Topics, KeyCode::Char('T')) => Some(Action::Truncate),
        (Screen::Topics, KeyCode::Char('c')) => Some(Action::ShowConfig),
        (Screen::TopicConfig, KeyCode::Enter) => Some(Action::Open),
        (Screen::TopicConfig, KeyCode::Char('u')) => Some(Action::ResetConfig),
        (Screen::TopicConfig, KeyCode::Char('r')) => Some(Action::Refresh),
        (Screen::Messages | Screen::Detail, KeyCode::Char('K')) => Some(Action::Tombstone),
        (Screen::Messages | Screen::Detail, KeyCode::Char('p')) => Some(Action::Produce),
        (Screen::Messages | Screen::Detail, KeyCode::Char('E')) => Some(Action::EditProduce),
        (Screen::Messages, KeyCode::Char('X')) => Some(Action::Replay),
        (Screen::Messages, KeyCode::Char('x')) => Some(Action::Export),
        (Screen::Messages, KeyCode::Char(' ')) => Some(Action::TogglePause),
        (Screen::Messages, KeyCode::Char('b')) => Some(Action::Seek(StartAt::Beginning)),
        (Screen::Messages, KeyCode::Char('e')) => Some(Action::Seek(StartAt::End)),
        (Screen::Messages, KeyCode::Char('o')) => Some(Action::BeginInput(InputKind::SeekOffset)),
        (Screen::Messages, KeyCode::Char('t')) => {
            Some(Action::BeginInput(InputKind::SeekTimestamp))
        }
        (Screen::Messages | Screen::Detail, KeyCode::Char('y')) => Some(Action::CopyValue),
        (Screen::Messages | Screen::Detail, KeyCode::Char('Y')) => Some(Action::CopyKey),
        _ => None,
    }
}
