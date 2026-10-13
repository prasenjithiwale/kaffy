//! Shared styles. Uses the terminal's own palette so light and dark themes both work.

use ratatui::style::{Color, Modifier, Style};

pub const ACCENT: Color = Color::Cyan;

pub fn title() -> Style {
    Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)
}

pub fn dim() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

pub fn label() -> Style {
    Style::new().fg(ACCENT)
}

pub fn selected() -> Style {
    Style::new().add_modifier(Modifier::REVERSED)
}

pub fn header_row() -> Style {
    Style::new().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
}

pub fn key_hint() -> Style {
    Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)
}

pub fn error() -> Style {
    Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
}

pub fn warning() -> Style {
    Style::new().fg(Color::Yellow)
}

pub fn ok() -> Style {
    Style::new().fg(Color::Green)
}

pub fn badge(color: Color) -> Style {
    Style::new()
        .fg(Color::Black)
        .bg(color)
        .add_modifier(Modifier::BOLD)
}

pub mod json {
    use ratatui::style::{Color, Style};

    pub fn key() -> Style {
        Style::new().fg(Color::Cyan)
    }
    pub fn string() -> Style {
        Style::new().fg(Color::Green)
    }
    pub fn number() -> Style {
        Style::new().fg(Color::Yellow)
    }
    pub fn literal() -> Style {
        Style::new().fg(Color::Magenta)
    }
}
