//! Keys to actions. Defaults come from [`crate::commands`]; the config's
//! `[keys]` table can rebind any command.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::{self, COMMANDS, CommandDef, Scope};
use crate::palette::PaletteKey;
use crate::state::{Action, AppState};

/// A key as written in the config: `x`, `X`, `ctrl-d`, `space`, `enter`, `pgdn`, …
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key {
    code: KeyCode,
    ctrl: bool,
}

const NAMES: &[(&str, KeyCode)] = &[
    ("up", KeyCode::Up),
    ("down", KeyCode::Down),
    ("left", KeyCode::Left),
    ("right", KeyCode::Right),
    ("enter", KeyCode::Enter),
    ("return", KeyCode::Enter),
    ("esc", KeyCode::Esc),
    ("escape", KeyCode::Esc),
    ("space", KeyCode::Char(' ')),
    ("backspace", KeyCode::Backspace),
    ("tab", KeyCode::Tab),
    ("pgup", KeyCode::PageUp),
    ("pageup", KeyCode::PageUp),
    ("pgdn", KeyCode::PageDown),
    ("pagedown", KeyCode::PageDown),
    ("home", KeyCode::Home),
    ("end", KeyCode::End),
    ("delete", KeyCode::Delete),
    ("del", KeyCode::Delete),
];

impl Key {
    pub fn parse(text: &str) -> Result<Self, String> {
        let (ctrl, rest) = match text
            .strip_prefix("ctrl-")
            .or_else(|| text.strip_prefix("ctrl+"))
            .or_else(|| text.strip_prefix("C-"))
        {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let mut chars = rest.chars();
        let code = match (chars.next(), chars.next()) {
            (Some(c), None) => KeyCode::Char(if ctrl { c.to_ascii_lowercase() } else { c }),
            _ => {
                let lower = rest.to_ascii_lowercase();
                if let Some(&(_, code)) = NAMES.iter().find(|(name, _)| *name == lower) {
                    code
                } else if let Some(n) = lower.strip_prefix('f').and_then(|n| n.parse().ok()) {
                    KeyCode::F(n)
                } else {
                    return Err(format!(
                        "unknown key `{text}`; use a character, ctrl-<char>, or one of \
                         up down left right enter esc space backspace tab pgup pgdn home end delete f1-f12"
                    ));
                }
            }
        };
        if ctrl && !matches!(code, KeyCode::Char(c) if c.is_ascii_alphabetic()) {
            return Err(format!("`{text}`: only ctrl-<letter> is supported"));
        }
        Ok(Self { code, ctrl })
    }

    pub fn from_event(event: &KeyEvent) -> Self {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let code = match event.code {
            KeyCode::Char(c) if ctrl => KeyCode::Char(c.to_ascii_lowercase()),
            code => code,
        };
        Self { code, ctrl }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ctrl {
            f.write_str("ctrl-")?;
        }
        match self.code {
            KeyCode::Char(' ') => f.write_str("space"),
            KeyCode::Char(c) => write!(f, "{c}"),
            KeyCode::Up => f.write_str("↑"),
            KeyCode::Down => f.write_str("↓"),
            KeyCode::Left => f.write_str("←"),
            KeyCode::Right => f.write_str("→"),
            KeyCode::Enter => f.write_str("⏎"),
            KeyCode::Esc => f.write_str("esc"),
            KeyCode::Backspace => f.write_str("⌫"),
            KeyCode::Tab => f.write_str("tab"),
            KeyCode::PageUp => f.write_str("PgUp"),
            KeyCode::PageDown => f.write_str("PgDn"),
            KeyCode::Home => f.write_str("Home"),
            KeyCode::End => f.write_str("End"),
            KeyCode::Delete => f.write_str("Del"),
            KeyCode::F(n) => write!(f, "F{n}"),
            other => write!(f, "{other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Keymap {
    keys: HashMap<&'static str, Vec<Key>>,
}

impl Default for Keymap {
    fn default() -> Self {
        let keys = COMMANDS
            .iter()
            .map(|c| {
                let keys = c
                    .keys
                    .iter()
                    .map(|k| Key::parse(k).expect("valid default key"));
                (c.id, keys.collect())
            })
            .collect();
        Self { keys }
    }
}

fn overlap(a: Scope, b: Scope) -> bool {
    match (a, b) {
        (Scope::Everywhere, _) | (_, Scope::Everywhere) => true,
        (Scope::On(a), Scope::On(b)) => a.iter().any(|s| b.contains(s)),
    }
}

impl Keymap {
    /// The defaults with the config's `[keys]` applied. A rebound key is taken
    /// away from any other command that shares a screen with it, so the
    /// footer and help never show a key that no longer works.
    pub fn with_overrides(overrides: &BTreeMap<String, Vec<String>>) -> Result<Self, String> {
        let mut map = Self::default();
        let mut parsed = Vec::new();
        for (id, keys) in overrides {
            let command = commands::find(id).ok_or_else(|| {
                let known: Vec<&str> = COMMANDS.iter().map(|c| c.id).collect();
                format!(
                    "[keys]: unknown command `{id}`; known: {}",
                    known.join(", ")
                )
            })?;
            let keys = keys
                .iter()
                .map(|k| Key::parse(k).map_err(|e| format!("[keys] {id}: {e}")))
                .collect::<Result<Vec<_>, _>>()?;
            parsed.push((command, keys));
        }
        for (command, keys) in &parsed {
            for other in COMMANDS.iter().filter(|o| o.id != command.id) {
                if overlap(command.scope, other.scope) {
                    if let Some(other_keys) = map.keys.get_mut(other.id) {
                        other_keys.retain(|k| !keys.contains(k));
                    }
                }
            }
        }
        for (command, keys) in parsed {
            map.keys.insert(command.id, keys);
        }
        Ok(map)
    }

    pub fn keys(&self, id: &str) -> &[Key] {
        self.keys.get(id).map_or(&[], Vec::as_slice)
    }

    /// The first key of a command, for hints; `None` if it is unbound.
    pub fn hint(&self, id: &str) -> Option<String> {
        self.keys(id).first().map(Key::to_string)
    }

    pub fn command(&self, screen: crate::Screen, key: Key) -> Option<&'static CommandDef> {
        commands::on(screen).find(|c| self.keys(c.id).contains(&key))
    }

    /// What a key press does in the current state.
    pub fn action(&self, state: &AppState, event: KeyEvent) -> Option<Action> {
        // Alt+key would otherwise act as the plain key; nothing is bound to Alt.
        if event.modifiers.contains(KeyModifiers::ALT) {
            return None;
        }
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && event.code == KeyCode::Char('c') {
            return Some(Action::Quit);
        }
        if state.show_help {
            return Some(Action::ToggleHelp);
        }
        if state.palette.is_some() {
            return Some(Action::Palette(match (event.code, ctrl) {
                (KeyCode::Esc, _) => PaletteKey::Cancel,
                (KeyCode::Enter, _) => PaletteKey::Submit,
                (KeyCode::Up, _) | (KeyCode::Char('p' | 'k'), true) => PaletteKey::Up,
                (KeyCode::Down, _) | (KeyCode::Char('n' | 'j'), true) => PaletteKey::Down,
                (KeyCode::Backspace, _) => PaletteKey::Backspace,
                (KeyCode::Char(c), false) => PaletteKey::Char(c),
                _ => return None,
            }));
        }
        if state.dialog.is_some() {
            return match event.code {
                KeyCode::Esc => Some(Action::DialogCancel),
                KeyCode::Enter => Some(Action::DialogEnter),
                KeyCode::Backspace => Some(Action::DialogBackspace),
                KeyCode::Char(c) if !ctrl => Some(Action::DialogKey(c)),
                _ => None,
            };
        }
        if state.input.is_some() {
            return match event.code {
                KeyCode::Enter => Some(Action::InputSubmit),
                KeyCode::Esc => Some(Action::InputCancel),
                KeyCode::Backspace => Some(Action::InputBackspace),
                KeyCode::Char(c) if !ctrl => Some(Action::InputChar(c)),
                _ => None,
            };
        }
        self.command(state.screen, Key::from_event(&event))
            .map(|c| (c.action)())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Screen;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn parses_and_shows_keys() {
        for (text, shown) in [
            ("x", "x"),
            ("X", "X"),
            ("ctrl-d", "ctrl-d"),
            ("ctrl+D", "ctrl-d"),
            ("space", "space"),
            ("PgDn", "PgDn"),
            ("enter", "⏎"),
            ("f5", "F5"),
            (":", ":"),
        ] {
            assert_eq!(Key::parse(text).unwrap().to_string(), shown, "{text}");
        }
        assert!(Key::parse("hyper-x").is_err());
        assert!(Key::parse("ctrl-1").is_err());
        assert_eq!(
            Key::from_event(&KeyEvent::new(KeyCode::Char('D'), KeyModifiers::CONTROL)),
            Key::parse("ctrl-d").unwrap()
        );
    }

    #[test]
    fn defaults_map_keys_per_screen() {
        let map = Keymap::default();
        let mut state = AppState::new("t", false, false);
        assert_eq!(
            map.action(&state, press(KeyCode::Char('n'))),
            Some(Action::CreateTopic)
        );
        state.screen = Screen::Schemas;
        assert_eq!(
            map.action(&state, press(KeyCode::Char('n'))),
            Some(Action::NewSubject)
        );
        assert_eq!(
            map.action(
                &state,
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
            ),
            Some(Action::Quit)
        );
    }

    #[test]
    fn overrides_take_the_key_from_other_commands() {
        let overrides = BTreeMap::from([
            ("pause".to_owned(), vec!["p".to_owned()]),
            ("produce".to_owned(), vec!["P".to_owned()]),
        ]);
        let map = Keymap::with_overrides(&overrides).unwrap();
        let mut state = AppState::new("t", false, false);
        state.screen = Screen::Messages;
        assert_eq!(
            map.action(&state, press(KeyCode::Char('p'))),
            Some(Action::TogglePause)
        );
        assert_eq!(
            map.action(&state, press(KeyCode::Char('P'))),
            Some(Action::Produce)
        );
        assert_eq!(map.action(&state, press(KeyCode::Char(' '))), None);
        assert_eq!(map.hint("produce").as_deref(), Some("P"));

        let unknown = BTreeMap::from([("pasue".to_owned(), vec!["p".to_owned()])]);
        assert!(
            Keymap::with_overrides(&unknown)
                .unwrap_err()
                .contains("pasue")
        );
        let bad = BTreeMap::from([("pause".to_owned(), vec!["meta-p".to_owned()])]);
        assert!(Keymap::with_overrides(&bad).is_err());
    }

    #[test]
    fn an_empty_list_unbinds() {
        let overrides = BTreeMap::from([("quit".to_owned(), vec![])]);
        let map = Keymap::with_overrides(&overrides).unwrap();
        let state = AppState::new("t", false, false);
        assert_eq!(map.action(&state, press(KeyCode::Char('q'))), None);
        assert_eq!(map.hint("quit"), None);
    }
}
