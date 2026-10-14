#!/bin/sh
# Prints the licenses of everything compiled into the kaffy binary: the C libraries
# that rdkafka builds in, then every Rust crate (via cargo-about, see about.toml).
#
#   scripts/third-party-licenses.sh > THIRD-PARTY-LICENSES.txt
#
# Needs cargo-about (cargo install --locked --features cli cargo-about) and jq.
# The release workflow runs this and ships the result in every archive.

set -eu
cd "$(dirname "$0")/.."

metadata=$(cargo metadata --format-version 1 --locked)

# The unpacked source of a crate in the lock file.
src() {
    manifest=$(printf '%s' "$metadata" \
        | jq -er --arg p "$1" '[.packages[] | select(.name == $p)] | first | .manifest_path') \
        || { echo "$1 is not in Cargo.lock" >&2; exit 1; }
    dirname "$manifest"
}

section() {
    printf '\n################################################################################\n'
    printf '%s\n' "$1"
    printf '################################################################################\n\n'
}

rdkafka=$(src rdkafka-sys)
openssl=$(src openssl-src)
libz=$(src libz-sys)

cat <<'EOF'
Third-party software in kaffy
=============================

The kaffy binary contains the software listed below. Each part is distributed
under its own license, reproduced in full here as those licenses require.
EOF

section "librdkafka, the Apache Kafka C client, and the code it bundles"
cat "$rdkafka/librdkafka/LICENSES.txt"

section "OpenSSL"
cat "$openssl/openssl/LICENSE.txt"

section "zlib"
cat "$libz/src/zlib/LICENSE"

section "Rust crates"
cargo about generate --locked about.hbs
