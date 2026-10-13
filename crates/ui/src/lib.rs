//! Terminal UI for kaffy: application state, the reducer and the ratatui views.

pub mod filter;
pub mod fmt;
pub mod state;
mod theme;
mod view;

pub use state::{Action, AppState, Effect, InputKind, Screen};
pub use view::render;
