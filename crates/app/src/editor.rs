//! Editing text in the user's `$EDITOR` while the TUI is suspended.

use std::process::Command;

use anyhow::{Context, Result, bail};
use ratatui::DefaultTerminal;

/// Leaves the TUI, edits `text` in `$VISUAL`/`$EDITOR` (default `vi`) and
/// returns the saved text. The TUI is restored even when editing fails.
pub fn edit(terminal: &mut DefaultTerminal, text: &str) -> Result<String> {
    let dir = std::env::temp_dir().join(format!("kaffy-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("message.kaffy");
    std::fs::write(&path, text).context("writing the message template")?;

    ratatui::restore();
    let status = run_editor(&path);
    *terminal = ratatui::init();
    // The new terminal starts with empty buffers; wipe what the editor left on
    // screen so the first draw matches. (Terminal::clear would also query the
    // cursor position, which needs a reply the event reader may swallow.)
    crossterm::execute!(
        std::io::stdout(),
        crossterm::terminal::Clear(crossterm::terminal::ClearType::All)
    )?;

    let edited = std::fs::read_to_string(&path).context("reading the edited message");
    let _ = std::fs::remove_file(&path);
    let status = status?;
    if !status.success() {
        bail!("the editor exited with {status}; nothing was sent");
    }
    edited
}

fn run_editor(path: &std::path::Path) -> Result<std::process::ExitStatus> {
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_owned());
    // Values like "code --wait" carry arguments.
    let mut parts = editor.split_whitespace();
    let program = parts.next().context("$EDITOR is empty")?;
    Command::new(program)
        .args(parts)
        .arg(path)
        .status()
        .with_context(|| format!("starting {editor:?} (set $EDITOR to your editor)"))
}
