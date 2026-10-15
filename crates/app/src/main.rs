mod editor;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{Event as TermEvent, EventStream, KeyEventKind};
use futures::StreamExt;
use kaffy_config::{Config, NameFilter, Profile, SchemaRegistry};
use kaffy_kafka::{Command, Worker};
use kaffy_ui::keymap::Keymap;
use kaffy_ui::theme::ThemeName;
use kaffy_ui::{Action, AppState, Effect};
use ratatui::DefaultTerminal;

/// Redraw at most this often; Kafka events only mark the screen dirty.
const FRAME_INTERVAL: Duration = Duration::from_millis(33);

/// A terminal UI for exploring and operating Kafka clusters.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Config file [default: $KAFFY_CONFIG or ~/.config/kaffy/config.toml]
    #[arg(long, short)]
    config: Option<PathBuf>,

    /// Profile from the config file [default: `default_profile`, else the first]
    #[arg(long, short)]
    profile: Option<String>,

    /// Connect to these brokers (plaintext, read-only) without a config file
    #[arg(long, short, conflicts_with = "profile")]
    bootstrap: Option<String>,

    /// Schema Registry URL for decoding Avro; overrides the profile's
    #[arg(long, value_name = "URL")]
    schema_registry: Option<String>,

    /// Colors: dark, light or terminal (16 colors) [default: $KAFFY_THEME, the config, else detected]
    #[arg(long, value_name = "NAME")]
    theme: Option<String>,

    /// Show only these topics: comma-separated globs, `!` excludes (e.g. 'orders-*,!*.dlq');
    /// overrides the profile's `topics`
    #[arg(long, value_name = "PATTERNS", value_parser = NameFilter::parse_list)]
    topics: Option<NameFilter>,

    /// Show only these consumer groups (same syntax); overrides the profile's `groups`
    #[arg(long, value_name = "PATTERNS", value_parser = NameFilter::parse_list)]
    groups: Option<NameFilter>,

    /// Run against a built-in fake cluster; no broker needed
    #[arg(long, conflicts_with_all = ["profile", "bootstrap", "schema_registry"])]
    demo: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let log_path = init_logging()?;
    tracing::info!(?args, "starting kaffy");
    let ui_config = ui_config(&args)?;
    let theme = pick_theme(&args, &ui_config)?;
    tracing::info!(?theme, "theme");
    kaffy_ui::theme::init(theme);
    let keymap = Keymap::with_overrides(&ui_config.keys).map_err(anyhow::Error::msg)?;

    let (worker, mut state) = if args.demo {
        // The demo cluster is fake, so it accepts writes: try produce, replay and resets.
        let mut state = AppState::new("demo", false, false);
        state.topic_scope = args.topics.clone().unwrap_or_default();
        state.groups.scope = args.groups.clone().unwrap_or_default();
        (kaffy_kafka::spawn_demo(), state)
    } else {
        let profile = resolve_profile(&args)?;
        tracing::info!(profile = %profile.name, bootstrap = %profile.bootstrap_servers, "connecting");
        let worker = kaffy_kafka::spawn(&profile)
            .with_context(|| format!("could not set up profile `{}`", profile.name))?;
        let mut state = AppState::new(&profile.name, profile.read_only, profile.prod);
        state.topic_scope = profile.topics.clone();
        state.groups.scope = profile.groups.clone();
        (worker, state)
    };

    state.keymap = keymap;
    let mut terminal = ratatui::init();
    let result = App::new(state, worker).run(&mut terminal).await;
    ratatui::restore();
    result.with_context(|| format!("kaffy failed; see the log at {}", log_path.display()))
}

/// The config file's UI settings (theme and keys), which apply to `--demo` and
/// `--bootstrap` too. No file means the defaults.
fn ui_config(args: &Args) -> Result<Config> {
    match args.config.clone().or_else(Config::default_path) {
        Some(path) if path.exists() => Ok(Config::load(&path)?),
        _ => Ok(Config::default()),
    }
}

/// `--theme`, then `$KAFFY_THEME`, then the config file's `theme`, else a guess
/// from the terminal.
fn pick_theme(args: &Args, config: &Config) -> Result<ThemeName> {
    let parse = |name: &str, source: &str| {
        ThemeName::parse(name).with_context(|| {
            format!("unknown theme `{name}` in {source}; use dark, light or terminal")
        })
    };
    if let Some(name) = &args.theme {
        return parse(name, "--theme");
    }
    if let Ok(name) = std::env::var("KAFFY_THEME") {
        return parse(&name, "$KAFFY_THEME");
    }
    match &config.theme {
        Some(name) => parse(name, "the config file"),
        None => Ok(ThemeName::detect()),
    }
}

fn resolve_profile(args: &Args) -> Result<Profile> {
    let mut profile = load_profile(args)?;
    if let Some(topics) = &args.topics {
        profile.topics = topics.clone();
    }
    if let Some(groups) = &args.groups {
        profile.groups = groups.clone();
    }
    if let Some(url) = &args.schema_registry {
        profile.schema_registry = Some(SchemaRegistry {
            url: url.clone(),
            username: None,
            password_env: None,
        });
    }
    Ok(profile)
}

fn load_profile(args: &Args) -> Result<Profile> {
    if let Some(bootstrap) = &args.bootstrap {
        return Ok(Profile::adhoc(bootstrap));
    }
    let path = match &args.config {
        Some(path) => path.clone(),
        None => Config::default_path().context("cannot locate a home directory for the config")?,
    };
    if !path.exists() {
        bail!(
            "no config file at {}\n\n\
             Create one (see config.example.toml), or run with\n  \
             kaffy --bootstrap localhost:9092   to connect without a config\n  \
             kaffy --demo                       to try the UI with fake data",
            path.display()
        );
    }
    let config = Config::load(&path)?;
    Ok(config.profile(args.profile.as_deref())?.clone())
}

/// Logs go to a file: anything written to the terminal would corrupt the UI.
fn init_logging() -> Result<PathBuf> {
    let dir = dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("kaffy");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("kaffy.log"))?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::sync::Mutex::new(file))
        .with_ansi(false)
        .init();
    Ok(dir.join("kaffy.log"))
}

struct App {
    state: AppState,
    worker: Worker,
    clipboard: Option<arboard::Clipboard>,
    /// A template to open in `$EDITOR` once the current event is handled.
    pending_edit: Option<String>,
}

impl App {
    fn new(state: AppState, worker: Worker) -> Self {
        Self {
            state,
            worker,
            clipboard: None,
            pending_edit: None,
        }
    }

    async fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        self.send(Command::ListTopics);
        let mut terminal_events = EventStream::new();
        let mut frames = tokio::time::interval(FRAME_INTERVAL);
        frames.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut dirty = true;

        while !self.state.should_quit {
            if let Some(template) = self.pending_edit.take() {
                let result = editor::edit(terminal, &template);
                // The old stream may hold keys typed in the editor; start fresh.
                terminal_events = EventStream::new();
                match result {
                    Ok(text) => self.dispatch(Action::Edited(text)),
                    Err(e) => self.state.flash_error(format!("editor: {e:#}")),
                }
                dirty = true;
            }
            tokio::select! {
                _ = frames.tick() => {
                    self.dispatch(Action::Tick);
                    if dirty {
                        terminal.draw(|frame| kaffy_ui::render(frame, &mut self.state))?;
                        dirty = false;
                    }
                }
                event = terminal_events.next() => match event {
                    Some(Ok(TermEvent::Key(key))) if key.kind == KeyEventKind::Press => {
                        if let Some(action) = self.state.keymap.action(&self.state, key) {
                            self.dispatch(action);
                        }
                        dirty = true;
                    }
                    Some(Ok(_)) => dirty = true,
                    Some(Err(e)) => return Err(e).context("reading terminal events"),
                    None => break,
                },
                event = self.worker.events.recv() => match event {
                    Some(event) => {
                        if let Some(effect) = self.state.on_event(event) {
                            self.apply(effect);
                        }
                        dirty = true;
                    }
                    None => bail!("the Kafka worker stopped unexpectedly"),
                },
            }
        }
        Ok(())
    }

    fn dispatch(&mut self, action: Action) {
        if let Some(effect) = self.state.update(action) {
            self.apply(effect);
        }
    }

    fn apply(&mut self, effect: Effect) {
        match effect {
            Effect::Kafka(command) => self.send(command),
            Effect::Copy { what, text } => self.copy(what, text),
            Effect::Many(effects) => effects.into_iter().for_each(|e| self.apply(e)),
            Effect::WriteFile {
                path,
                contents,
                summary,
            } => self.write_file(&path, &contents, &summary),
            Effect::Edit { template } => self.pending_edit = Some(template),
        }
    }

    /// Writes into the working directory, never over an existing file.
    fn write_file(&mut self, path: &str, contents: &str, summary: &str) {
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .and_then(|mut file| std::io::Write::write_all(&mut file, contents.as_bytes()));
        match result {
            Ok(()) => {
                let shown = std::env::current_dir()
                    .map(|dir| dir.join(path).display().to_string())
                    .unwrap_or_else(|_| path.to_owned());
                self.state
                    .flash_info(format!("exported {summary} to {shown}"));
            }
            Err(e) => self
                .state
                .flash_error(format!("export to {path} failed: {e}")),
        }
    }

    fn send(&mut self, command: Command) {
        if self.worker.commands.send(command).is_err() {
            self.state.flash_error("the Kafka worker stopped");
        }
    }

    fn copy(&mut self, what: &str, text: String) {
        let result = match &mut self.clipboard {
            Some(clipboard) => clipboard.set_text(text),
            None => arboard::Clipboard::new().and_then(|mut clipboard| {
                let result = clipboard.set_text(text);
                self.clipboard = Some(clipboard);
                result
            }),
        };
        match result {
            Ok(()) => self.state.flash_info(format!("copied {what} to clipboard")),
            Err(e) => self.state.flash_error(format!("clipboard: {e}")),
        }
    }
}
