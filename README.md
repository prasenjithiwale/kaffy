# kaffy

A terminal UI for exploring and operating Kafka clusters, written in Rust.
See [docs/kafka-tui-feasibility.md](docs/kafka-tui-feasibility.md) for scope and roadmap.

## Quick start

```sh
cargo run -- --demo                       # fake cluster, no broker needed
cargo run -- --bootstrap localhost:9092   # plaintext broker, no config file
cargo run -- --profile staging            # profile from ~/.config/kaffy/config.toml
cargo run -- --bootstrap localhost:9092 --schema-registry http://localhost:8081
```

The first build compiles librdkafka and OpenSSL from source (needs `cmake`), which takes a few minutes.
Logs are written to `~/Library/Caches/kaffy/kaffy.log` on macOS (`~/.cache/kaffy/kaffy.log` on Linux); set `RUST_LOG=debug` for more.

## Configuration

Copy [config.example.toml](config.example.toml) to `~/.config/kaffy/config.toml`, or pass `--config <path>` / set `$KAFFY_CONFIG`.
Passwords are never stored in the file: `sasl.password_env` names an environment variable to read them from.
Profiles are read-only unless `read_only = false`.

### Schema Registry

Payloads in the Confluent wire format (magic byte + schema id) are decoded through a Confluent-compatible
Schema Registry, set per profile with `schema_registry = { url = "...", username = "...", password_env = "..." }`
or with `--schema-registry <url>`. Avro is decoded to JSON (including schemas that reference other subjects);
JSON Schema payloads are shown as JSON; Protobuf decoding is not implemented yet.
Schemas are cached per id, and the filter and clipboard work on the decoded JSON.
Without a registry, such payloads are shown as a hex dump labelled with their schema id.

## Keys

| Screen | Keys |
|---|---|
| everywhere | `↑↓`/`jk` move, `PgUp`/`PgDn`/`Ctrl-u`/`Ctrl-d` page, `g`/`G` top/bottom, `esc` back (clears an active filter first), `q`/`Ctrl-c` quit |
| topics | `⏎` open, `/` filter, `r` refresh |
| messages | `⏎` detail, `space` pause/resume, `/` filter (see below), `b` from beginning, `e` from end, `o` seek to offset, `t` seek to time (`-15m`, `2026-10-13 09:30`, RFC 3339, epoch ms), `r` reload, `y`/`Y` copy value/key |
| detail | `↑↓` scroll, `y`/`Y` copy value/key |

### Message filter

Press `/` on the messages screen. Terms are separated by spaces and must all match:

| Term | Matches |
|---|---|
| `word` | key or value contains `word` (case-insensitive) |
| `~regex` | key or value matches the regex (case-sensitive; prefix `(?i)` to ignore case) |
| `key:x`, `value:x`, `header:x` (or `k:` `v:` `h:`) | that field contains `x`; headers are matched as `name=value` |
| `key~re`, `value~re`, `header~re` | that field matches the regex |
| `p:0,2` | partition 0 or 2 |
| `after:-15m`, `before:2026-10-13T09:30` | timestamp range (relative, local time, RFC 3339 or epoch ms) |
| `!term` | negates a term |

Wrap a whole term in quotes to include spaces: `"key:a b"`. Quotes elsewhere are literal,
so `~"status":"(PAID|SHIPPED)"` works as typed.

Opening a topic reads the last 100 messages of each partition, then follows new ones. Up to 5,000 messages are kept in memory.
The viewer assigns partitions directly and never commits offsets, so it does not join or disturb any consumer group.

## Layout

```
crates/
├── app/     # `kaffy` binary: CLI, event loop, keymap, clipboard
├── ui/      # AppState + reducer, ratatui views
├── kafka/   # rdkafka worker (metadata, tail, seek) and the --demo fake cluster
├── schema/  # payload decoding (JSON / text / hex; Schema Registry later)
└── config/  # cluster profiles → librdkafka properties
```

Key presses and Kafka events become `Action`s; `AppState::update` applies them and may return an `Effect`
(a Kafka command or a clipboard copy) for the app to carry out. Kafka I/O runs in background tasks behind channels,
and the screen is redrawn at most ~30 times a second.

## Local Kafka

A single-node KRaft broker from Homebrew is enough for development:

```sh
brew install kafka
brew services run kafka          # start in the background (`start` also launches it at login)
scripts/seed-local.sh            # topics orders, payments, user-events, customer-profiles with sample data
cargo run -- --bootstrap localhost:9092
brew services stop kafka
```

Broker data lives in `/opt/homebrew/var/lib/kraft-combined-logs`, logs in `/opt/homebrew/var/log/kafka`.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
cargo test -p kaffy-kafka --test local_broker -- --ignored   # end-to-end, needs a broker on localhost:9092
```

Dev builds skip debug info for dependencies (see `Cargo.toml`) to keep `target/` small.
