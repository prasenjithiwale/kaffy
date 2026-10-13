//! Terminal UI for kaffy: application state, the reducer and the ratatui views.

pub mod dialog;
pub mod export;
pub mod filter;
pub mod fmt;
pub mod groups;
pub mod schemas;
pub mod state;
pub mod template;
mod theme;
mod view;

pub use state::{Action, AppState, Effect, InputKind, Screen};
pub use view::render;
