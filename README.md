# kaffy

A terminal UI for exploring and operating Kafka clusters, written in Rust.
See [docs/kafka-tui-feasibility.md](docs/kafka-tui-feasibility.md) for scope and roadmap.

## Install

On Linux x86_64 (static binary, any distribution) and macOS 11+ (Apple Silicon or Intel):

```sh
curl -fsSL https://prasenjithiwale.github.io/folio/kaffy/install.sh | sh
```

Or with Homebrew, which needs you to trust the tap's formula first:

```sh
brew trust --formula prasenjithiwale/tap/kaffy && brew install prasenjithiwale/tap/kaffy
```

See https://prasenjithiwale.github.io/folio/kaffy/ for options and manual downloads.

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

### Cloud clusters

**Confluent Cloud** uses an API key and secret over SASL/PLAIN, and the Schema Registry takes its own key:

```toml
[profiles.confluent]
bootstrap_servers = "pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092"
security_protocol = "sasl_ssl"
sasl = { mechanism = "PLAIN", username = "<cluster API key>", password_env = "KAFFY_CC_SECRET" }
schema_registry = { url = "https://psrc-xxxxx.eu-west-1.aws.confluent.cloud", username = "<registry API key>", password_env = "KAFFY_CC_SR_SECRET" }
```

**Amazon MSK with IAM** connects to the cluster's IAM listener (port 9098):

```toml
[profiles.msk]
bootstrap_servers = "b-1.mycluster.abc123.c2.kafka.eu-west-1.amazonaws.com:9098"
security_protocol = "sasl_ssl"
aws_msk_iam = { region = "eu-west-1", profile = "msk-dev" }   # profile is optional
```

kaffy signs the IAM tokens itself (SigV4, as AWS's signer libraries do) and renews them every 5 minutes.
Credentials come from the named AWS profile through the AWS CLI (`aws configure export-credentials`, so SSO
and assumed roles work; run `aws sso login` first), falling back to `~/.aws/credentials`. Without a
profile it uses the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` variables, then
`$AWS_PROFILE` or `default`. The IAM policy needs `kafka-cluster:Connect` plus the read actions
(`DescribeTopic`, `ReadData`, `DescribeGroup`, …) and, for writes, the matching write actions.

TLS uses the system's CA certificates, so neither needs `[tls]`.

### Schema Registry

Payloads in the Confluent wire format (magic byte + schema id) are decoded through a Confluent-compatible
Schema Registry, set per profile with `schema_registry = { url = "...", username = "...", password_env = "..." }`
or with `--schema-registry <url>`. Avro is decoded to JSON (including schemas that reference other subjects);
JSON Schema payloads are shown as JSON, and Protobuf messages as JSON with their `.proto` field names
(the message type is read from the message-index path, so nested messages work too).
Schemas are cached per id, and the filter and clipboard work on the decoded JSON.
Without a registry, such payloads are shown as a hex dump labelled with their schema id.

### Protobuf without a registry

For topics carrying plain Protobuf, point kaffy at the `.proto` files (compiled in-process, no `protoc`
needed; Google's well-known types are built in):

```toml
[[profiles.local.protobuf]]
topic = "orders-proto"
files = ["proto/orders.proto"]   # relative to the config file
include = ["proto"]              # where imports are looked up
value = "acme.Order"             # key = "acme.OrderKey" too, if keys are Protobuf
```

The files are compiled at startup, so a mistake is reported before the UI opens.

### Schemas

The schemas tab (`3`) lists every subject with its type and latest version. A subject shows its versions;
`d` diffs a version with the one before it. `e` opens the latest schema in `$EDITOR`; the result is checked
for compatibility (with the registry's explanation when it isn't), shown as a diff, and registered only once
you confirm. `n` registers a new subject from an Avro, JSON Schema or Protobuf starting point.

## Themes

kaffy has its own dark and light color themes, and a `terminal` theme that uses your terminal's 16 colors.
On terminals that support true color (`COLORTERM=truecolor`, or iTerm2, WezTerm, Ghostty, VS Code, Warp)
it picks dark, or light when the terminal reports a light background (`COLORFGBG`); elsewhere, such as
macOS Terminal, it uses `terminal`. Choose one with `--theme dark|light|terminal`, `$KAFFY_THEME`, or
`theme = "light"` at the top of the config file.

## Keys

Press `?` on any screen for its keys. `1`, `2`, `3` and `4` switch between topics, consumer groups, schemas
and brokers.

**Command palette:** `:` or `Ctrl-P` opens a search box. Type part of a command ("reset", "tomb") to run it,
or part of a topic, consumer group or subject name to jump straight to it. Matching is fuzzy: `sr` finds
`sensor-readings`.

**Rebinding keys:** give a command new keys in the config's `[keys]` table. They replace that command's
defaults, an empty list unbinds it, and a key you take is removed from any other command on the same screens.
The help (`?`) and footer show your keys. Command names are listed in `crates/ui/src/commands.rs`; the common
ones are `pause`, `filter`, `produce`, `refresh`, `back`, `quit`, `palette` and `seek-beginning`.

```toml
[keys]
pause = "p"                  # instead of space
produce = "P"
palette = ["ctrl-k", ":"]
quit = []                    # unbound; ctrl-c always quits
```

| Screen | Keys |
|---|---|
| everywhere | `↑↓`/`jk` move, `PgUp`/`PgDn`/`Ctrl-u`/`Ctrl-d` page, `g`/`G` top/bottom, `esc` back (clears an active filter first), `?` help, `q`/`Ctrl-c` quit |
| topics | `⏎` open, `/` filter, `r` refresh, `c` config, `n` new topic, `D` delete topic, `T` delete records |
| messages | `⏎` detail, `space` pause/resume, `/` filter (see below), `b` from beginning, `e` from end, `o` seek to offset, `t` seek to time (`-15m`, `2026-10-13 09:30`, RFC 3339, epoch ms), `r` reload, `y`/`Y` copy value/key, `p` produce, `E` edit & produce, `K` tombstone, `X` replay, `x` export |
| detail | `↑↓` scroll, `y`/`Y` copy value/key, `E` edit & produce, `K` tombstone |
| topic config | `⏎` change a value, `u` reset to the inherited value |
| schemas | `⏎` open, `n` new subject, `/` filter |
| subject | `↑↓` version, `d` diff with the previous version, `e` edit & register |
| groups | `⏎` open, `R` reset offsets, `/` filter, `r` refresh |
| group detail | `R` reset offsets, `r` refresh |
| brokers | `↑↓` move through problem partitions, `r` refresh |

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

## Consumer groups

The groups tab lists every consumer group with its state, members, topics and total lag, refreshed every
5 seconds with a trend line. Opening a group shows lag per partition, its members and a lag chart.

## Broker health

The brokers tab (`4`) shows the cluster id, each broker with its address and how many partitions it leads
and replicates, and every partition that needs attention: offline (no leader) or under-replicated, with the
replicas that have fallen out of sync. A broker that replicas point at but the metadata leaves out is listed
as likely down. The screen refreshes every 5 seconds.

The controller shown is the one brokers report to clients. On KRaft clusters that is a broker that forwards
requests to the controller quorum, not the quorum's active controller.

## Changing things

Writes are disabled unless the profile has `read_only = false`, and the Kafka worker refuses them again
independently of the UI. Every change shows a preview first and is sent only after you confirm with `y`;
on profiles with `prod = true` you type the group or topic name instead.

- **Reset offsets** (`R` on a group): earliest, latest, a time, or an offset, for all of the group's topics
  or one. The preview shows current and new offsets and how many messages will be reprocessed or skipped.
  Kafka only accepts resets while the group has no active members; kaffy checks this again just before
  committing and verifies the committed offsets afterwards.
- **Produce** (`p`) opens `$VISUAL`/`$EDITOR` (default `vi`) with a template: topic, partition (or `auto`
  to let the key decide, as Java producers do), key, headers and the value. **Edit & produce** (`E`) fills
  the template from the selected message; Avro values are shown as JSON and encoded again with their schema.
- **Replay** (`X`) copies a range (from: beginning, an offset or a time; until: now, an offset or a time) to
  the same or another topic, keeping keys, values and headers. **The active filter applies**, so you can
  replay just the messages you are looking at. Messages keep their partition when both topics have the
  same partition count. Progress is shown while it runs; `c` cancels.
- **Export** (`x`) writes the messages currently shown (after the filter) to JSON, NDJSON or CSV in the
  working directory. Decoded Avro and JSON are written as JSON; binary payloads as base64. Export works on
  read-only profiles, since it doesn't write to Kafka.

- **Topics** (`n`, `D`, `c`): create topics, delete them, and change their configs. A config change keeps the
  topic's other settings, and kaffy reads the config back to confirm it took effect.
- **Delete records** (`T`): remove everything before an offset or a time (DeleteRecords). **Tombstones**
  (`K` on a message): produce a null value for its key, which deletes the key on compacted topics.

Deleting a topic and deleting records can't be undone, so they always require typing the topic name,
on every profile.

`kaffy --demo` accepts all of these against its fake cluster, so you can try them safely.

## Layout

```
crates/
├── app/     # `kaffy` binary: CLI, event loop, keymap, clipboard
├── ui/      # AppState + reducer, dialogs, produce templates, export, ratatui views
├── kafka/   # rdkafka worker (tail, seek, groups, resets, produce, replay) and the --demo fake cluster
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

# Schema Registry features: Kafka on 29092 and Confluent Schema Registry on 8081 in Docker
docker compose -f dev/docker-compose.yml up -d --wait
cargo test -p kaffy-schema --test real_registry -- --ignored
cargo test -p kaffy-kafka --test schema_registry -- --ignored
cargo run -- --bootstrap localhost:29092 --schema-registry http://localhost:8081
```

## Releases

`.github/workflows/ci.yml` runs formatting, clippy, unit tests and the broker tests (against a Kafka
service container) on every push. Pushing a tag that matches the crate version releases it:

```sh
git tag v0.4.0 && git push origin v0.4.0
```

`.github/workflows/release.yml` then builds a static Linux x86_64 binary (musl, in an Alpine container) and
macOS binaries for Apple Silicon and Intel, publishes the archives and checksums to the public
[folio](https://github.com/prasenjithiwale/folio) site (`FOLIO_DEPLOY_KEY`, a write deploy key on folio),
writes `Formula/kaffy.rb` into [homebrew-tap](https://github.com/prasenjithiwale/homebrew-tap)
(`HOMEBREW_TAP_DEPLOY_KEY`, generated by `scripts/homebrew-formula.sh`), and installs the result with the
one-liner on Ubuntu, Debian, Alpine and macOS and with `brew` on macOS. Run it by hand with
`dry_run: true` to build and check the binaries without publishing anything.

The screens on the website are rendered from the real UI with `cargo run -p kaffy-ui --example snapshot`.
The recorded tour and screenshots come from `dev/preview.tape` ([vhs](https://github.com/charmbracelet/vhs)), run
in a directory holding a `kaffy` binary.

Dev builds skip debug info for dependencies (see `Cargo.toml`) to keep `target/` small.

## License

kaffy is licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option. Unless you state otherwise, any contribution you submit
for inclusion in kaffy is dual-licensed the same way, without additional terms or conditions.

Release archives include `THIRD-PARTY-LICENSES.txt`, the licenses of everything compiled into the binary.
