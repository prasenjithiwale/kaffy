use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use kaffy_kafka::StartAt;
use kaffy_ui::{Action, AppState, InputKind, Screen};

pub fn map_key(state: &AppState, key: KeyEvent) -> Option<Action> {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c') => Some(Action::Quit),
            KeyCode::Char('d') => Some(Action::PageDown),
            KeyCode::Char('u') => Some(Action::PageUp),
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
        (Screen::Topics | Screen::Messages, KeyCode::Char('/')) => {
            Some(Action::BeginInput(InputKind::Filter))
        }
        (Screen::Topics | Screen::Messages, KeyCode::Char('r')) => Some(Action::Refresh),
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
