# Security policy

kaffy connects to Kafka clusters with your credentials and can change them (offsets, topics, configs,
schemas), so security problems matter even in a small tool.

## Reporting a vulnerability

Please **don't open a public issue**. Report it privately through GitHub instead:
[Report a vulnerability](https://github.com/prasenjithiwale/kaffy/security/advisories/new)
(the repository's **Security** tab → **Report a vulnerability**).

Include what you found, how to reproduce it, the kaffy version (`kaffy --version`) and your platform.
Leave out real passwords, tokens and broker addresses.

You'll get an acknowledgement within a week. Once a fix is released, the advisory is published with
credit to you, unless you'd rather stay anonymous.

## Supported versions

Fixes go into the latest release only. Update with the install script, `brew upgrade kaffy` or
`cargo install --locked kaffy`.

## What counts

For example:

- a write reaching Kafka on a read-only profile, or without the preview and confirmation;
- credentials written to the log, the config file, exports or the screen;
- TLS or SASL settings being ignored or weakened;
- the install script or release archives being open to tampering.

Bugs that need an attacker who can already change your config file or environment are usually out of
scope, but report them if you're unsure.
