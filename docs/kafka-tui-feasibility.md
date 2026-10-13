# Kafka Event Viewer TUI (Rust): Feasibility & Scope

## 1. Verdict

It is feasible to build this in Rust. The libraries for the terminal UI, the Kafka client and Schema Registry access are all mature enough.

Two requested features ("delete message" and "update message") cannot work literally, because Kafka messages are immutable. The closest workable versions are described in section 3.

---

## 2. Recommended Tech Stack

| Concern | Crate / Tool | Notes |
|---|---|---|
| TUI rendering | `ratatui` | Widgets, layouts, tables, charts, sparklines |
| Terminal backend | `crossterm` | Cross-platform input and raw mode (re-exported by ratatui) |
| Async runtime | `tokio` | Event loop, channels, timers |
| Kafka client | `rdkafka` (librdkafka) | Consumer, producer and AdminClient. Battle-tested; it is the same C library kcat uses |
| Schema Registry | `schema-registry-client` | Async Confluent SR client with Avro / Protobuf / JSON Schema serdes |
| Avro | `apache-avro` | Decoding and encoding |
| Protobuf | `prost` + `prost-reflect` | Dynamic decoding needs descriptors |
| JSON | `serde_json` | Pretty-printing, JSONPath-style filtering |
| Config | `serde` + `toml` / `config` | Cluster profiles |
| Secrets | `keyring` | Store SASL passwords in the OS keychain |
| Syntax highlight | `syntect` or `tui-syntax-highlight` | Message payload view |
| Fuzzy search | `nucleo` / `fuzzy-matcher` | Topic and group pickers |
| Logging | `tracing` + `tracing-appender` | Logs go to a file, never to the terminal |
| Errors | `anyhow` / `thiserror` | |
| Clipboard | `arboard` | Copy key or value |

---

## 3. Feature Feasibility Matrix

| # | Feature | Feasibility | How it works | Caveats |
|---|---|---|---|---|
| 1 | Real-time message view | ✅ Easy | `StreamConsumer` with a throwaway group id and `enable.auto.commit=false` | Must not disturb real consumer groups |
| 2 | Seek: from beginning, end, offset or timestamp | ✅ Easy | `assign()` + `seek()`; `offsets_for_times()` for timestamps | |
| 3 | Filter / search | ✅ Easy | Client-side predicates on key, value, headers, partition, offset and time | Kafka has no server-side filtering; searching history means scanning the topic |
| 4 | Message detail view | ✅ Easy | Pretty JSON, Avro or Protobuf decoding, headers, metadata | |
| 5 | Topic list and metadata | ✅ Easy | `fetch_metadata()`, partitions, replicas, ISR, configs | |
| 6 | Consumer groups and lag | ✅ Easy | Committed offsets vs. high watermarks (`fetch_watermarks`), per partition and in total | Lag is a point-in-time snapshot; poll on an interval |
| 7 | Replay: reset group offsets | ✅ Medium | AdminClient / `commit` with explicit offsets (earliest, latest, timestamp, specific) | The group must be inactive (no live members) |
| 8 | Replay: re-produce a range | ✅ Medium | Consume a range, then produce it to the same or another topic | Duplicates downstream; preserve keys and headers |
| 9 | Produce a message | ✅ Easy | `FutureProducer` with key, value, headers and partition | Schema-encode when the topic uses SR |
| 10 | "Edit" a message | ⚠️ Workaround only | Open the message in an editor, then re-produce it as a new message | Messages are immutable; the original stays |
| 11 | "Delete" a message | ⚠️ Partial | (a) DeleteRecords truncates a partition up to an offset; (b) a tombstone (null value) on compacted topics; (c) delete the topic | Deleting one message from the middle of a log is impossible |
| 12 | Schema browse and diff | ✅ Medium | List subjects and versions; side-by-side diff | |
| 13 | Schema update | ✅ Medium | Compatibility check first, then register the new version | This is a Schema Registry operation, not a Kafka one |
| 14 | Topic create / delete / config edit | ✅ Easy | AdminClient | Destructive: needs confirmation |
| 15 | Export messages | ✅ Easy | JSON / NDJSON / CSV to file | |
| 16 | Multiple clusters | ✅ Easy | Profiles in a config file | |
| 17 | Broker / cluster health | ✅ Medium | Broker list, controller, under-replicated partitions | |

---

## 4. Existing Tools (Competition)

| Tool | Language | Notes |
|---|---|---|
| Yozefu | Rust (ratatui) | Explore and search records; consume-only; 500-record ring buffer |
| kaftui | Rust (ratatui) | Browse topics, SR deserializers |
| kcat | C | CLI, not a TUI |
| AKHQ / Kafka UI / Redpanda Console | Java / Go | Web UIs |

**Differentiation opportunity:** an operations-focused TUI rather than a viewer. That means lag dashboards, replay, offset resets, safe destructive operations, schema management, and k9s-style navigation.

---

## 5. Technical Risks

| Risk | Impact | Mitigation |
|---|---|---|
| librdkafka is a C dependency | Harder cross-compiles and static binaries | `rdkafka` `cmake-build` feature; CI builds per OS; or `dynamic-linking` for dev |
| High-throughput topics | UI freeze, memory growth | Bounded ring buffer, pause/sample modes, render capped at ~30 fps, batch events |
| Auth variety | Connection failures | Support PLAINTEXT, SSL/mTLS, SASL PLAIN/SCRAM, OAUTHBEARER; MSK IAM later |
| Protobuf without SR | Undecodable payloads | Accept `.proto` / descriptor-set paths per topic |
| Destructive operations | Data loss in prod | Typed confirmation, read-only mode by default, per-profile "prod" flag |
| Terminal differences | Broken rendering | Test on iTerm2, Windows Terminal, Alacritty, tmux |

---

## 6. Architecture

```
┌────────────────────────── main loop (tokio::select!) ──────────────────────────┐
│  terminal events │ kafka consumer stream │ admin / lag poller │ render tick     │
└─────────┬────────────────┬──────────────────────┬────────────────────┬──────────┘
          ▼                ▼                      ▼                    ▼
      Action enum ───────► App State (single store) ◄──────── async task results
                                   │
                                   ▼
                          ratatui render (views)
```

- **Unidirectional flow:** events → `Action` → reducer updates `AppState` → render. This is similar to an event bus feeding a state store.
- Kafka I/O runs in background tokio tasks and talks to the UI over `mpsc` channels, so the UI thread never blocks.
- Rendering is driven by a tick (~30 fps), not by each message.

### Suggested Cargo workspace

```
kafka-tui/
├── crates/
│   ├── app/        # main binary, event loop, keymaps
│   ├── ui/         # ratatui views & widgets
│   ├── kafka/      # consumer, producer, admin, lag
│   ├── schema/     # Schema Registry + decoders
│   └── config/     # profiles, secrets
```

### Main views

1. Cluster picker
2. Topics list (partitions, messages, size)
3. Live tail and message table
4. Message detail (key, value, headers)
5. Consumer groups and lag (with sparkline)
6. Schema Registry browser and diff
7. Replay / offset reset wizard
8. Command palette (`:` like k9s)

---

## 7. Scope & Phases

Estimates are rough, for one developer working part-time.

### Phase 1: MVP (~4–6 weeks)
- [ ] Config file with cluster profiles (PLAINTEXT, SASL, SSL)
- [ ] Topic list with metadata
- [ ] Live tail with pause/resume
- [ ] Seek: beginning, end, offset, timestamp
- [ ] JSON and Avro (via SR) decoding
- [ ] Filters: key, value contains / regex, partition, time range
- [ ] Message detail pane with syntax highlighting
- [ ] Copy key/value to clipboard

### Phase 2: Operations (~4 weeks)
- [ ] Consumer groups list
- [ ] Lag per partition and total, refreshed periodically, with sparkline
- [ ] Offset reset wizard (earliest / latest / timestamp / specific)
- [ ] Replay a range to the same or another topic
- [ ] Produce a message; "edit and re-produce"
- [ ] Export to JSON / NDJSON / CSV

### Phase 3: Schema & Admin (~4 weeks)
- [ ] Schema Registry: subjects, versions, diff
- [ ] Compatibility check, then register a new version
- [ ] Protobuf decoding (SR and local descriptors)
- [ ] Topic create / delete / config edit
- [ ] DeleteRecords (truncate) and tombstones
- [ ] Read-only mode and prod-safety confirmations

### Phase 4: Polish
- [ ] Themes (light/dark), configurable keybindings
- [ ] Command palette and fuzzy search
- [ ] Broker health view
- [ ] MSK IAM / Confluent Cloud auth
- [ ] Static binaries, Homebrew / cargo-install / release builds

---

## 8. Out of Scope

- Editing or deleting an individual message in place (not possible in Kafka)
- Server-side filtering (not supported by Kafka)
- Kafka Connect / ksqlDB management (could be a later plugin)
- Web UI

---

## 9. Local Dev Setup

- Docker Compose with Kafka (KRaft mode) and Schema Registry
- A seed script that produces JSON, Avro and Protobuf test data and creates a few consumer groups with lag
- Integration tests against containers (`testcontainers` crate)
