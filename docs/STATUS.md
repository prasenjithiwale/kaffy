# kaffy: project status and handoff

Last updated: 2026-10-13. Plan and feature list: [kafka-tui-feasibility.md](kafka-tui-feasibility.md).

## Where things stand

| Phase | State | Released |
|---|---|---|
| 1. MVP: profiles, topics, live tail, seek, filters, JSON/Avro, detail, clipboard | done | v0.1.0 |
| 2. Operations: consumer groups and lag, offset reset, replay, produce, edit & re-produce, export | done | v0.2.0 |
| 3. Schema & admin: Schema Registry browser/diff/register, Protobuf, topic create/delete/config, DeleteRecords, tombstones | done | v0.3.0 |
| 4. Polish: themes, keybindings, command palette, broker health, MSK IAM / Confluent Cloud auth | in progress: broker health view, UI revamp with dark/light themes done | |

v0.3.0 was released on 2026-10-13. The workspace version is `0.3.0`.

## Releasing

The release pipeline is automated; a version tag triggers it. Steps (as done for v0.3.0):

1. **Bump the version** in the root `Cargo.toml` (`[workspace.package] version`), then run
   `cargo build` so `Cargo.lock` updates. `release.yml` refuses a tag that doesn't match this version.
2. **Refresh the website screenshots.** `crates/ui/examples/snapshot.rs` renders real screens from the
   demo cluster as HTML: `cargo run -p kaffy-ui --example snapshot > <scratch>/screens.html`. Add
   new screens to it as features land (the brokers screen is already in it, not yet on the site).
3. **Update the kaffy page on the folio site** (`~/Projects/folio`, public repo
   `prasenjithiwale/folio`, served at https://prasenjithiwale.github.io/folio/kaffy/):
   - Tabs in the `<figure class="terminal">` viewer in `kaffy/index.html` are a `<button role="tab">`
     plus a `<div role="tabpanel">` with the `<pre class="tui">` from the snapshot and a caption. Keep
     ids unique and only the first panel visible. Refresh every panel, since the header changes.
   - Update the copy, the keys table and the home page blurb (`index.html`).
   - Commit and push folio **before** tagging, so the site describes the release it serves.
4. **Commit and push kaffy**, wait for CI to pass, then tag:
   `git tag -a vX.Y.Z -m "kaffy vX.Y.Z: ..." && git push origin vX.Y.Z`.
5. **Watch the Release workflow** (`gh run list --repo prasenjithiwale/kaffy`). Its jobs are:
   1. **build:** checks the version, builds on ubuntu-22.04, packages, and fails if the binary links libcurl.
   2. **publish:** commits `kaffy/releases/vX.Y.Z/*`, `latest.txt` and `latest.json` into folio through
      the `FOLIO_DEPLOY_KEY` deploy key.
   3. **smoke-test:** waits for GitHub Pages, then runs the documented
      `curl -fsSL https://prasenjithiwale.github.io/folio/kaffy/install.sh | sh` on Ubuntu 22.04 and
      Debian 12 and checks `kaffy --version`.
6. **Verify:** `curl -s https://prasenjithiwale.github.io/folio/kaffy/releases/latest.txt` shows the tag,
   then `git pull` in `~/Projects/folio` to pick up the release commit.

Before pushing any Rust change, run CI's exact clippy (see *Toolchains* below); CI uses a newer Rust
than Homebrew's and has failed on new lints before.

## Repositories and services

- **kaffy:** github.com/prasenjithiwale/kaffy, **private**. CI: `.github/workflows/ci.yml` (fmt, clippy
  `-D warnings`, unit tests, broker tests against an `apache/kafka` service container). Releases:
  `.github/workflows/release.yml` (on `v*` tags).
- **folio:** github.com/prasenjithiwale/folio, **public**, GitHub Pages from `main` /. Plain HTML/CSS,
  no build step. `kaffy/install.sh` is the installer. `kaffy/releases/` is written only by kaffy's
  release workflow: don't edit it by hand.
- **Secret:** `FOLIO_DEPLOY_KEY` in kaffy holds the private half of a write deploy key on folio. The
  local copy was deleted after setup.

## Local environment (this Mac)

- **Homebrew Kafka 4.3.1** on `localhost:9092` (KRaft), started with `brew services run kafka`, so it
  doesn't start at login. `scripts/seed-local.sh` creates sample topics. `crates/kafka/tests/local_broker.rs`
  runs against it.
- **Docker dev stack**, `dev/docker-compose.yml`: Kafka on `localhost:29092` plus Confluent Schema
  Registry 7.9.1 on `localhost:8081`. `docker compose -f dev/docker-compose.yml up -d --wait` / `down`.
  The registry image is about 4.8 GB. Your `jobscout` Redis and MySQL containers also run in Docker;
  leave them alone.
  - Docker's `desktop` credential helper hangs `docker pull` here. Pull public images with
    `DOCKER_CONFIG=$(mktemp -d) docker pull <image>`. This hides the compose plugin, so pull images
    one by one, then `docker compose up`.
- **Disk is tight** (about 13 GB free at last check; it has hit 0 twice). The build folder grows fast:
  dev builds use `debug = "line-tables-only"` and no dependency debug info for that reason. If space
  runs out, run `rm -rf target/debug/incremental` and
  `cargo clean -p kaffy -p kaffy-ui -p kaffy-kafka -p kaffy-schema -p kaffy-config`. Check `df -h ~`
  before anything large.
- **Toolchains:** `cargo` on PATH is Homebrew's Rust 1.90. CI uses the latest stable (1.99 on
  2026-10-13), whose clippy has newer lints. Before pushing, run:
  `PATH="$HOME/.cargo/bin:$PATH" CARGO_TARGET_DIR=<scratch dir> rustup run stable cargo clippy --workspace --all-targets --locked -- -D warnings`,
  then delete the scratch target folder (about 0.5 GB).
- **Headless browser screenshots:** Chrome isn't installed, and Arc has no headless mode. For visual
  review of folio pages, install Playwright's WebKit into a scratch dir (about 250 MB) and delete it afterwards.

## Testing

```sh
cargo test --workspace                                           # unit + render tests (100)
cargo test -p kaffy-kafka --test local_broker -- --ignored       # Homebrew broker on 9092 (6 tests)
docker compose -f dev/docker-compose.yml up -d --wait
cargo test -p kaffy-schema --test real_registry -- --ignored     # real Schema Registry (1 test)
cargo test -p kaffy-kafka --test schema_registry -- --ignored    # Kafka 29092 + registry: Protobuf (2 tests)
```

The ignored tests clean up the topics, groups and subjects they create, and names carry a counter
because macOS clocks tick in microseconds. If a run is interrupted, check for leftovers named
`kaffy-test-*` on the brokers and the registry.

**Driving the real TUI:** there's no tmux here. The approach that worked was a small Python
pseudo-terminal driver (`pty.fork`, a fixed `TIOCSWINSZ` window size, and a minimal emulator handling
cursor moves and text) that sends keys and prints screens. Gotchas:
- Wait about 2 s after launching a freshly built binary, because macOS checks new binaries on first launch.
- Send Esc and the next key as separate writes, or they arrive as Alt+key.
- Answer "cursor position" queries or avoid them: the editor return path clears the screen directly
  for that reason.
- `$EDITOR` can be a script that edits the file, to test produce and schema editing end to end.

## Architecture in brief

Cargo workspace: `app` (binary, event loop, keymap, `$EDITOR`), `ui` (AppState + reducer, dialogs,
templates, export, filters, ratatui views), `kafka` (worker: tail, seek, groups, admin, produce, replay,
decoders, demo cluster), `schema` (payload rendering, Schema Registry client, Avro, Protobuf), `config`
(profiles). Key presses and worker events become `Action`s; `AppState::update` / `on_event` return an
optional `Effect` that the app executes (Kafka command, clipboard, editor, file write). Every write goes
through a worker-produced plan, then a preview dialog, then confirmation (`y`, or typing the name on
`prod = true` profiles; always typed for topic deletion and DeleteRecords). Read-only profiles are refused
in both the UI and the worker.

## Next task: Phase 4

Done: **UI revamp** (shared look in `crates/ui/src/view/kit.rs`, palettes in `theme.rs`: dark, light
and 16-color `terminal`, chosen by `--theme`, `$KAFFY_THEME`, config `theme`, else detected). The website
snapshot now emits true colors as inline styles; refresh every folio panel at the next release.
Done: **broker health view** (`4`): cluster id, reported controller, per-broker leaders/replicas/out-of-sync,
offline and under-replicated partitions, brokers missing from metadata. On KRaft the "controller" clients
see is a forwarding broker, not the active quorum controller; the screen says so.

Done: **keybindings** (`[keys]` in the config; registry in `crates/ui/src/commands.rs`, keymap in
`keymap.rs`) and the **command palette** (`:` / `Ctrl-P`, `palette.rs`).

Done: **MSK IAM** (`aws_msk_iam` on a profile; SigV4 token signer in `crates/kafka/src/msk_iam.rs`,
set on each client with `rd_kafka_oauthbearer_set_token` and renewed every 5 minutes) and documented
**Confluent Cloud** (SASL/PLAIN). Not tested against a real MSK cluster: the signer is checked against
AWS's published SigV4 example, and librdkafka accepting the token is tested offline.

Remaining, from the roadmap: static binaries and Homebrew / cargo-install
distribution. Consider macOS and arm64 builds too, since the site only offers Linux x86_64 today.
Release as v0.4.0 when Phase 4 is done (or earlier if the user asks), adding the brokers screen to the site.

## Working agreements with the user

- Releases are triggered by version tags only; binaries are committed into folio, not attached to
  GitHub Releases there.
- Confirm before outward-facing actions that weren't asked for: new repos, publishing, deleting
  anyone's data.
- Keep folio's home-page intro factual. The user hasn't supplied a bio, and the current line was
  written from public GitHub facts and flagged to them.
