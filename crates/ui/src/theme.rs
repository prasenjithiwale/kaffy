//! Colors and shared styles.
//!
//! The dark and light palettes are kaffy's own true-color schemes; `Terminal`
//! uses the terminal's 16 colors, for terminals without true color. The palette
//! is picked once at startup with [`init`]; until then the dark one is used.

use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeName {
    Dark,
    Light,
    /// The terminal's own 16 colors.
    Terminal,
}

impl ThemeName {
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "dark" => Some(Self::Dark),
            "light" => Some(Self::Light),
            "terminal" | "16" | "ansi" => Some(Self::Terminal),
            _ => None,
        }
    }

    /// A guess from the environment: true color support (`COLORTERM`) and the
    /// background a terminal advertises (`COLORFGBG`, set by some terminals).
    pub fn detect() -> Self {
        let truecolor = std::env::var("COLORTERM")
            .is_ok_and(|v| matches!(v.as_str(), "truecolor" | "24bit"))
            || std::env::var("TERM_PROGRAM").is_ok_and(|v| {
                matches!(
                    v.as_str(),
                    "iTerm.app" | "WezTerm" | "ghostty" | "vscode" | "WarpTerminal"
                )
            });
        if !truecolor {
            return Self::Terminal;
        }
        // "fg;bg" (or "fg;default;bg"); a light background is 7 or 15.
        let light = std::env::var("COLORFGBG").is_ok_and(|v| {
            v.rsplit(';')
                .next()
                .is_some_and(|bg| matches!(bg, "7" | "15"))
        });
        if light { Self::Light } else { Self::Dark }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Palette {
    pub name: ThemeName,
    /// The screen background; `Reset` keeps the terminal's.
    pub bg: Color,
    /// Header and footer bars, zebra rows.
    pub surface: Color,
    /// Dialogs and key chips.
    pub raised: Color,
    pub border: Color,
    pub text: Color,
    pub muted: Color,
    pub faint: Color,
    pub accent: Color,
    /// Text on an accent background.
    pub on_accent: Color,
    pub selection: Color,
    pub ok: Color,
    pub warn: Color,
    pub err: Color,
    pub info: Color,
    pub special: Color,
    pub json_key: Color,
    pub json_string: Color,
    pub json_number: Color,
    pub json_literal: Color,
    /// Cycled through to tell partitions apart.
    pub partitions: [Color; 6],
}

const fn rgb(hex: u32) -> Color {
    Color::Rgb((hex >> 16) as u8, (hex >> 8) as u8, hex as u8)
}

/// Matches the kaffy website: deep slate with a teal accent.
const DARK: Palette = Palette {
    name: ThemeName::Dark,
    bg: rgb(0x0f1a24),
    surface: rgb(0x15232f),
    raised: rgb(0x1c2e3c),
    border: rgb(0x2b4152),
    text: rgb(0xd9e3ea),
    muted: rgb(0x8d9fae),
    faint: rgb(0x5a6d7c),
    accent: rgb(0x4fc8d0),
    on_accent: rgb(0x0b141c),
    selection: rgb(0x1f3d4b),
    ok: rgb(0x6fcf97),
    warn: rgb(0xf2c14e),
    err: rgb(0xff6b6b),
    info: rgb(0x82aaff),
    special: rgb(0xc792ea),
    json_key: rgb(0x7fd6dd),
    json_string: rgb(0xa8d8a0),
    json_number: rgb(0xf6c177),
    json_literal: rgb(0xc792ea),
    partitions: [
        rgb(0x4fc8d0),
        rgb(0xf6c177),
        rgb(0xc792ea),
        rgb(0x82aaff),
        rgb(0x6fcf97),
        rgb(0xff9e7a),
    ],
};

const LIGHT: Palette = Palette {
    name: ThemeName::Light,
    bg: rgb(0xfbfaf7),
    surface: rgb(0xf1efe9),
    raised: rgb(0xffffff),
    border: rgb(0xd5d0c5),
    text: rgb(0x1d2830),
    muted: rgb(0x5d6a74),
    faint: rgb(0x98a1a8),
    accent: rgb(0x0b7a84),
    on_accent: rgb(0xffffff),
    selection: rgb(0xd6ecee),
    ok: rgb(0x1c8a4a),
    warn: rgb(0x9a6700),
    err: rgb(0xc62828),
    info: rgb(0x2f5fd0),
    special: rgb(0x8a3fb8),
    json_key: rgb(0x0b7a84),
    json_string: rgb(0x2e7d32),
    json_number: rgb(0x9a6700),
    json_literal: rgb(0x8a3fb8),
    partitions: [
        rgb(0x0b7a84),
        rgb(0x9a6700),
        rgb(0x8a3fb8),
        rgb(0x2f5fd0),
        rgb(0x1c8a4a),
        rgb(0xb5482a),
    ],
};

const TERMINAL: Palette = Palette {
    name: ThemeName::Terminal,
    bg: Color::Reset,
    surface: Color::Reset,
    raised: Color::Reset,
    border: Color::DarkGray,
    text: Color::Reset,
    muted: Color::Gray,
    faint: Color::DarkGray,
    accent: Color::Cyan,
    on_accent: Color::Black,
    selection: Color::Reset,
    ok: Color::Green,
    warn: Color::Yellow,
    err: Color::Red,
    info: Color::Blue,
    special: Color::Magenta,
    json_key: Color::Cyan,
    json_string: Color::Green,
    json_number: Color::Yellow,
    json_literal: Color::Magenta,
    partitions: [
        Color::Cyan,
        Color::Yellow,
        Color::Magenta,
        Color::Blue,
        Color::Green,
        Color::Red,
    ],
};

static PALETTE: OnceLock<Palette> = OnceLock::new();

/// Picks the palette; later calls are ignored.
pub fn init(name: ThemeName) {
    let _ = PALETTE.set(match name {
        ThemeName::Dark => DARK,
        ThemeName::Light => LIGHT,
        ThemeName::Terminal => TERMINAL,
    });
}

pub fn palette() -> &'static Palette {
    PALETTE.get_or_init(|| DARK)
}

fn p() -> &'static Palette {
    palette()
}

fn truecolor() -> bool {
    p().name != ThemeName::Terminal
}

fn fg(color: Color) -> Style {
    Style::new().fg(color)
}

/// The whole screen.
pub fn base() -> Style {
    Style::new().fg(p().text).bg(p().bg)
}

pub fn bar() -> Style {
    Style::new().fg(p().text).bg(p().surface)
}

pub fn raised() -> Style {
    Style::new().fg(p().text).bg(p().raised)
}

pub fn title() -> Style {
    fg(p().text).add_modifier(Modifier::BOLD)
}

pub fn accent() -> Style {
    fg(p().accent)
}

pub fn text() -> Style {
    fg(p().text)
}

pub fn dim() -> Style {
    if truecolor() {
        fg(p().muted)
    } else {
        Style::new().add_modifier(Modifier::DIM)
    }
}

pub fn faint() -> Style {
    fg(p().faint)
}

pub fn label() -> Style {
    fg(p().muted)
}

pub fn border() -> Style {
    fg(p().border)
}

pub fn border_focus() -> Style {
    fg(p().accent)
}

pub fn selected() -> Style {
    if truecolor() {
        Style::new().bg(p().selection).add_modifier(Modifier::BOLD)
    } else {
        Style::new().add_modifier(Modifier::REVERSED)
    }
}

pub fn zebra() -> Style {
    if truecolor() {
        Style::new().bg(p().surface)
    } else {
        Style::new()
    }
}

pub fn header_row() -> Style {
    fg(p().muted).add_modifier(Modifier::BOLD)
}

pub fn key_hint() -> Style {
    fg(p().accent).add_modifier(Modifier::BOLD)
}

/// A key drawn as a small chip.
pub fn key_chip() -> Style {
    fg(p().accent).bg(p().raised).add_modifier(Modifier::BOLD)
}

pub fn error() -> Style {
    fg(p().err).add_modifier(Modifier::BOLD)
}

pub fn warning() -> Style {
    fg(p().warn)
}

pub fn ok() -> Style {
    fg(p().ok)
}

pub fn info() -> Style {
    fg(p().info)
}

pub fn special() -> Style {
    fg(p().special)
}

/// Solid label, such as the active tab or the PROD marker.
pub fn badge(color: Color) -> Style {
    Style::new()
        .fg(p().on_accent)
        .bg(color)
        .add_modifier(Modifier::BOLD)
}

pub fn accent_color() -> Color {
    p().accent
}

pub fn err_color() -> Color {
    p().err
}

pub fn warn_color() -> Color {
    p().warn
}

pub fn partition(partition: i32) -> Style {
    let colors = &p().partitions;
    fg(colors[partition.rem_euclid(colors.len() as i32) as usize])
}

pub mod json {
    use ratatui::style::Style;

    pub fn key() -> Style {
        Style::new().fg(super::p().json_key)
    }
    pub fn string() -> Style {
        Style::new().fg(super::p().json_string)
    }
    pub fn number() -> Style {
        Style::new().fg(super::p().json_number)
    }
    pub fn literal() -> Style {
        Style::new().fg(super::p().json_literal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_theme_names() {
        assert_eq!(ThemeName::parse("Dark"), Some(ThemeName::Dark));
        assert_eq!(ThemeName::parse(" light "), Some(ThemeName::Light));
        assert_eq!(ThemeName::parse("16"), Some(ThemeName::Terminal));
        assert_eq!(ThemeName::parse("solarized"), None);
    }
}
