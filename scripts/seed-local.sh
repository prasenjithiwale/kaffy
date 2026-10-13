#!/usr/bin/env bash
# Creates test topics on a local broker and fills them with sample data.
# Usage: scripts/seed-local.sh [bootstrap]   (default localhost:9092)
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-$(dirname "$(command -v kafka-topics || echo /opt/homebrew/opt/kafka/bin/kafka-topics)")}"

topic() { # name partitions [extra config]
  "$KAFKA_BIN/kafka-topics" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" ${3:+--config "$3"} >/dev/null
  echo "topic $1 ($2 partitions)"
}

produce() { # topic, reads "headers<TAB>key<TAB>value" lines from stdin
  "$KAFKA_BIN/kafka-console-producer" --bootstrap-server "$BOOTSTRAP" --topic "$1" \
    --reader-property parse.key=true --reader-property parse.headers=true \
    --reader-property key.separator=$'\t' --reader-property headers.delimiter=$'\t' \
    --reader-property headers.separator=, --reader-property headers.key.separator=:
}

topic orders 3
topic payments 3
topic user-events 1
topic customer-profiles 2 cleanup.policy=compact

statuses=(CREATED PAID SHIPPED CANCELLED)
for i in $(seq 1 300); do
  printf 'source:seed,trace-id:%032x\tcust-%03d\t{"order_id":"ord-%05d","customer":"cust-%03d","status":"%s","amount":%d.%02d,"currency":"EUR"}\n' \
    "$RANDOM$RANDOM" $((i % 40)) "$i" $((i % 40)) "${statuses[i % 4]}" $((RANDOM % 500)) $((RANDOM % 100))
done | produce orders

for i in $(seq 1 150); do
  printf 'source:seed\tcust-%03d\t{"payment_id":"pay-%05d","method":"card","approved":%s}\n' \
    $((i % 40)) "$i" "$([ $((i % 9)) -eq 0 ] && echo false || echo true)"
done | produce payments

for i in $(seq 1 100); do
  printf 'source:seed\tuser-%02d\tuser-%02d viewed /products/%d\n' $((i % 12)) $((i % 12)) "$i"
done | produce user-events

for i in $(seq 1 20); do
  printf 'source:seed\tcust-%03d\t{"customer":"cust-%03d","tier":"gold"}\n' "$i" "$i"
done | produce customer-profiles

echo "seeded $BOOTSTRAP"
