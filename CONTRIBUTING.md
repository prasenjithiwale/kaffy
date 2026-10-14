# Contributing to kaffy

Thanks for helping. Bug reports, ideas and pull requests are all welcome.

## Issues

- **Bugs:** use the bug report template. The version, your platform and how you connect (plain Kafka,
  Confluent Cloud, MSK, …) usually matter. kaffy's log is at `~/Library/Caches/kaffy/kaffy.log` on macOS
  and `~/.cache/kaffy/kaffy.log` on Linux; run with `RUST_LOG=debug` for more detail, and remove anything
  private before attaching it.
- **Ideas:** open a feature request and describe what you're trying to do, not only the feature.
- **Security problems:** don't open an issue; see [SECURITY.md](SECURITY.md).

For anything bigger than a small fix, please open an issue first so we can agree on the approach.

## Building and testing

You need Rust 1.85 or newer and `cmake` (librdkafka and OpenSSL are compiled from source; the first build
takes a few minutes).

```sh
cargo run -- --demo                 # fake cluster, no Kafka needed
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Tests marked `#[ignore]` need real services and clean up after themselves:

```sh
# a broker on localhost:9092, e.g. `brew install kafka && brew services run kafka`
cargo test -p kaffy-kafka --test local_broker -- --ignored

# Kafka on 29092 and Schema Registry on 8081
docker compose -f dev/docker-compose.yml up -d --wait
cargo test -p kaffy-schema --test real_registry -- --ignored
cargo test -p kaffy-kafka --test schema_registry -- --ignored
```

CI runs formatting, clippy with `-D warnings` on the latest stable Rust, the unit tests and the broker
tests on every push and pull request; a pull request needs it to pass.

## How the code fits together

The [README](README.md#layout) describes the crates. Key presses and Kafka events become `Action`s,
`AppState::update` applies them and may return an `Effect` for the app to carry out, and Kafka I/O runs
in background tasks behind channels.

## Rules for changes

- **Writes go plan → preview → confirm.** Any new operation that changes a cluster or registry must show
  exactly what will change and wait for confirmation; destructive ones need the name typed.
- **Read-only profiles refuse writes twice:** in `ui` and again in the `kafka` worker. Keep both checks.
- **Never store or log secrets.** Passwords come from environment variables named in the config.
- **Add tests** for new behaviour: unit tests where possible, an ignored broker test when it needs Kafka.
- **Keep `kaffy --demo` working:** new screens and operations should work against the demo cluster.
- Match the surrounding code's style, and keep user-facing text short and plain.

## License

kaffy is dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE). Unless you state
otherwise, any contribution you submit for inclusion is licensed the same way, without additional terms
or conditions.
