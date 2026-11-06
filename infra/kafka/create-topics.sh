#!/usr/bin/env bash
# Creates the topics from PLAN.md section 5.2. Idempotent: safe to run any number of times.
# Partition counts can be overridden by the benchmark harness via environment variables.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
TRADES_PARTITIONS="${TRADES_PARTITIONS:-6}"
PASSTHROUGH_PARTITIONS="${PASSTHROUGH_PARTITIONS:-${TRADES_PARTITIONS}}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists "$@"
}

# Key = symbol. Retention 1 day so a run can be replayed.
create --topic trades --partitions "$TRADES_PARTITIONS" --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=86400000

# Pass-through output. LogAppendTime so latency uses the timestamp Kafka assigns on write.
create --topic passthrough_out --partitions "$PASSTHROUGH_PARTITIONS" --replication-factor 3 \
  --config min.insync.replicas=2 --config message.timestamp.type=LogAppendTime --config retention.ms=86400000

# Reproduction (a) only: RF 1, so min ISR must be 1 (the broker default of 2 would reject every write).
create --topic perf_test --partitions 1 --replication-factor 1 \
  --config min.insync.replicas=1 --config retention.ms=3600000

echo "--- topics ---"
"$KT" --bootstrap-server "$BOOTSTRAP" --describe
