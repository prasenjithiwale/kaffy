# kaffy

A terminal UI for exploring and operating Kafka clusters. Follow messages live, jump to any offset or time,
read Avro and Protobuf through your Schema Registry, watch consumer lag, and change things safely: every
write shows a preview and waits for you to confirm. One static binary for Linux and macOS, written in Rust.

![kaffy in demo mode: filtering a topic, reading a message, checking consumer lag, previewing an offset reset and diffing a schema](https://raw.githubusercontent.com/prasenjithiwale/folio/main/kaffy/media/preview.gif)

**Website:** https://prasenjithiwale.github.io/folio/kaffy/

## Features

- **Messages:** live tail across partitions; seek to the beginning, the end, an offset or a time (`-15m`,
  `2026-10-13 09:30`); filter by text, regex, field, header, partition and time range; copy, export to
  JSON/NDJSON/CSV. The viewer never commits offsets, so it doesn't disturb any consumer group.
- **Decoding:** Avro, JSON Schema and Protobuf through a Confluent-compatible Schema Registry, or plain
  Protobuf from your `.proto` files with no registry and no `protoc`.
- **Consumer groups:** state, members and lag for every group, with trends showing who is falling behind,
  and per-partition lag for one group.
- **Safe writes:** reset offsets, produce (in `$EDITOR`), edit and re-produce, replay a filtered range,
  tombstones, create/delete topics, change topic configs, delete records, register schemas. Each one shows
  exactly what will change first. Profiles are read-only unless you say otherwise.
- **Schemas and brokers:** browse subjects, diff versions, edit with a compatibility check; see broker
  health and every offline or under-replicated partition.
- **Clusters:** plaintext, SSL/mTLS, SASL (PLAIN, SCRAM), Confluent Cloud and Amazon MSK with IAM. Passwords
  come from environment variables, never the config file.
- **Focus:** show only the topics and groups you care about on a shared cluster, per profile or with
  `--topics` / `--groups`.
- **Comfort:** a fuzzy command palette (`:`), rebindable keys, dark, light and terminal-color themes, and
  `kaffy --demo`, a fake cluster with live data to try everything on.

## Why kaffy?

- **vs. `kcat` and the `kafka-*.sh` scripts:** those are for scripts and one-off commands. kaffy is for
  looking around: browsing, filtering and decoding interactively, and making changes you can see before
  they happen.
- **vs. web UIs (Kafka UI, AKHQ, Redpanda Console, …):** those are great when your team runs one. kaffy
  needs nothing deployed: it runs wherever you can reach the brokers, including an SSH session on a bastion
  host during an incident, with your own credentials.
- **Built to be safe on production:** read-only by default, enforced in the UI and again in the Kafka
  worker; a preview for every write; typed confirmation for destructive ones and on `prod = true` profiles.

## Limitations

Things kaffy doesn't do yet, so you know before you try:

- **No Windows build.** Linux (x86_64, arm64) and macOS only. The static Linux build should run under WSL2,
  but that is untested.
- **Authentication:** SASL PLAIN, SCRAM-SHA-256/512, TLS client certificates and Amazon MSK IAM. Kerberos
  (GSSAPI) and generic OAuth/OIDC (SASL/OAUTHBEARER outside MSK) aren't supported.
- **Kafka only, plus Schema Registry:** no Kafka Connect, ksqlDB, ACLs or quotas, and no Confluent Cloud or
  MSK management APIs (environments, API keys, cluster settings).
- **Messages are held in memory:** up to 5,000 per topic view, starting from the last 100 per partition by
  default. Older data is read a page at a time (see below); export writes the messages loaded and shown.
- **Payload formats:** JSON, text, Avro, JSON Schema and Protobuf; anything else is shown as a hex dump.

## Install

Linux on x86_64 or arm64 (static binary, any distribution) and macOS 11+ (Apple Silicon or Intel):

```sh
curl -fsSL https://prasenjithiwale.github.io/folio/kaffy/install.sh | sh
```

The script checks the download's SHA-256 checksum and puts `kaffy` in `~/.local/bin`. Or use Homebrew, which
needs you to trust the tap's formula first:

```sh
brew trust --formula prasenjithiwale/tap/kaffy && brew install prasenjithiwale/tap/kaffy
```

Or build it with Cargo (Rust 1.85+ and `cmake`; librdkafka and OpenSSL are compiled from source, which takes
a few minutes):

```sh
cargo install --locked kaffy
```

Archives and checksums for manual download are on the [website](https://prasenjithiwale.github.io/folio/kaffy/#install).
To build from source, see [Development](#development).

## Quick start

```sh
kaffy --demo                                  # fake cluster with live data, no broker needed
kaffy --bootstrap localhost:9092              # plaintext broker, no config file
kaffy --bootstrap localhost:9092 --schema-registry http://localhost:8081
kaffy --profile staging                       # a profile from ~/.config/kaffy/config.toml
kaffy --profile staging --topics 'checkout-*' --groups 'checkout-*'   # only what you work on
```

Press `?` on any screen for its keys, and `1`–`4` to switch between topics, consumer groups, schemas and
brokers. Logs go to `~/Library/Caches/kaffy/kaffy.log` on macOS (`~/.cache/kaffy/kaffy.log` on Linux); set
`RUST_LOG=debug` for more.

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

### Showing only some topics and groups

On a shared cluster, a profile can list the topics and consumer groups to show:

```toml
[profiles.staging]
topics = ["checkout-*", "payments", "!*.dlq"]   # globs: * any run of characters, ? one character
groups = ["checkout-*"]                          # a leading ! excludes; only excludes = all but those
```

`--topics 'checkout-*,!*.dlq'` and `--groups '…'` set them for one run (and override the profile's). Everything
else is hidden from the topics and groups tabs and the command palette. The active
patterns are shown next to the list title. Groups outside the patterns are skipped before their lag is
fetched, which also makes the groups tab faster on large clusters. Patterns are case-sensitive, like Kafka names.

This narrows what you see; it is not access control. The brokers tab still reports every partition, a group's
detail still lists all the topics it consumes, and anyone can run kaffy without the patterns. Use Kafka ACLs to
restrict access.

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
| messages | `⏎` detail, `space` pause/resume (next page when paged), `/` filter (see below), `b` from beginning, `e` from end, `o` seek to offset, `t` seek to time (`-15m`, `2026-10-13 09:30`, RFC 3339, epoch ms), `r` reload, `y`/`Y` copy value/key, `p` produce, `E` edit & produce, `K` tombstone, `X` replay, `x` export |
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

Looking back (`b` from the beginning, `o` an offset, `t` a time) reads one page of 4,500 messages, split
evenly across the partitions so each one starts where you asked, and stops; the status shows `PAGE`.
`space` replaces it with the next page, carrying on where each partition stopped. `e` goes back to
following new messages.
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
├── schema/  # payload decoding: JSON, text, hex, Avro, JSON Schema, Protobuf; Schema Registry client
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

Building needs Rust 1.85+ and `cmake`: the first build compiles librdkafka and OpenSSL from source, which
takes a few minutes. The original scope and roadmap are in
[docs/kafka-tui-feasibility.md](docs/kafka-tui-feasibility.md).

```sh
cargo build --release                       # target/release/kaffy
cargo run -- --demo
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

`.github/workflows/release.yml` then builds static Linux binaries for x86_64 and arm64 (musl, in an Alpine container) and
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

## Contributing

Bug reports, ideas and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Please report
security problems privately, as described in [SECURITY.md](SECURITY.md).

## License

kaffy is licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option. Unless you state otherwise, any contribution you submit
for inclusion in kaffy is dual-licensed the same way, without additional terms or conditions.

Release archives include `THIRD-PARTY-LICENSES.txt`, the licenses of everything compiled into the binary.
