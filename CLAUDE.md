# kaffy

A terminal UI for exploring and operating Kafka clusters, in Rust (ratatui + rdkafka). See README.md for
usage and `docs/STATUS.md` for the full project state, environment notes and release runbook.

## Start here

**Next task: Phase 4** (v0.3.0 is released). The UI revamp (themes) and broker health view are done; see "Next task: Phase 4"
in `docs/STATUS.md` for what remains (keybindings, command palette, MSK IAM / Confluent Cloud
auth, distribution). Releases follow "Releasing" in the same file.

## Rules that bit before

- **Disk space is tight on this Mac.** Check `df -h ~` before large builds or downloads. Clean
  `target/debug/incremental` and the workspace crates' build output if it runs low.
- **CI uses a newer Rust than Homebrew's `cargo`.** Before pushing, run CI's clippy with rustup's
  stable toolchain in a scratch target dir (exact command in `docs/STATUS.md`).
- **Never edit `folio/kaffy/releases/` by hand;** the release workflow owns it.
- Ignored tests need the Homebrew broker (9092) or the Docker dev stack (`dev/docker-compose.yml`:
  29092 + Schema Registry 8081). They clean up after themselves.
- Writes in the app always go plan → preview → confirm. Keep it that way for any new write operation,
  and refuse writes on read-only profiles in both `ui` and the `kafka` worker.
