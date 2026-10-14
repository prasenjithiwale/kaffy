<!-- Thanks! For anything bigger than a small fix, please link the issue where the approach was agreed. -->

## What and why


## Checklist

- [ ] `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` pass
- [ ] Tests added or updated (unit, or an ignored broker test if it needs Kafka)
- [ ] Any new write goes plan → preview → confirm, and is refused on read-only profiles in `ui` and the `kafka` worker
- [ ] Works in `kaffy --demo`
- [ ] README updated if behaviour or keys changed
