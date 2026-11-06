#!/usr/bin/env bash
# Robustness check: a paced (streaming, many micro-batches) virtual run through the whole pipeline,
# optionally with faults, then the verifier. Ends with PASS (exit 0) or FAIL (exit 1).
#
#   scripts/live_check.sh                                   # 60 virtual minutes at 60x, no faults
#   KILL_SPARK_AT=25 scripts/live_check.sh                  # SIGKILL Spark 25 s in, restart it 10 s later
#   KILL_BROKER_AT=20 scripts/live_check.sh                 # SIGKILL the leader of trades/0 at 20 s
#
# Settings (environment): MINUTES (60), SPEED (60), ACKS (all), IDEMPOTENCE (true), KILL_SPARK_AT,
# SPARK_DOWN_S (10), KILL_BROKER_AT, RUN_ID, BUILD (1; 0 reuses the images already built).
# This is a quick check, not an experiment: Experiments 2 and 3 (T26, T30) repeat these faults
# under the benchmark harness. IDEMPOTENCE=false with KILL_BROKER_AT can duplicate trades on
# producer retries; the verifier then reports a mismatched bar (seen on 2026-11-06).
set -euo pipefail

MINUTES="${MINUTES:-60}"
SPEED="${SPEED:-60}"
ACKS="${ACKS:-all}"
IDEMPOTENCE="${IDEMPOTENCE:-true}"
SPARK_DOWN_S="${SPARK_DOWN_S:-10}"
RUN_ID="${RUN_ID:-live-$(date -u +%Y%m%dT%H%M%SZ)}"
COMPOSE="${COMPOSE:-docker compose}"
GEN_ARGS=(--mode=virtual --speed="$SPEED" --minutes="$MINUTES" --seed=42 --shock-rate-per-min=0.2)
mkdir -p "runs/$RUN_ID"

step() { printf '\n== [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

step "reset, build, start Kafka x3, PostgreSQL, Grafana, Spark"
$COMPOSE --profile smoke down -v --remove-orphans >/dev/null 2>&1
if [[ "${BUILD:-1}" == 1 ]]; then $COMPOSE build generator spark-bars >/dev/null; fi
$COMPOSE up -d --wait kafka-1 kafka-2 kafka-3 kafka-init postgres grafana spark-bars >/dev/null 2>&1

step "generator: ${MINUTES} virtual minutes at ${SPEED}x, acks=$ACKS idempotence=$IDEMPOTENCE"
$COMPOSE run -d --name "gen-$RUN_ID" --no-deps --user "$(id -u):$(id -g)" generator \
  "${GEN_ARGS[@]}" --acks="$ACKS" --idempotence="$IDEMPOTENCE" --run-id="$RUN_ID" --log-trades >/dev/null
T0=$(date +%s)

if [[ -n "${KILL_SPARK_AT:-}" ]]; then
  ( sleep "$KILL_SPARK_AT"
    step "FAULT: docker kill spark-bars (SIGKILL); restart in ${SPARK_DOWN_S}s from the same checkpoint"
    docker kill spark-bars >/dev/null
    sleep "$SPARK_DOWN_S"
    $COMPOSE up -d --no-deps spark-bars >/dev/null 2>&1
    step "spark-bars restarted" ) &
fi
if [[ -n "${KILL_BROKER_AT:-}" ]]; then
  ( sleep "$KILL_BROKER_AT"
    leader=$(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
               --describe --topic trades | awk '$3 == "Partition:" && $4 == "0" {print $6}')
    [[ -z "$leader" ]] && leader=1
    step "FAULT: docker kill kafka-$leader (leader of trades partition 0)"
    docker kill "kafka-$leader" >/dev/null
    echo "kafka-$leader" > "runs/$RUN_ID/killed_broker" ) &
fi

until [[ "$(docker inspect -f '{{.State.Status}}' "gen-$RUN_ID")" == exited ]]; do sleep 2; done
wait  # fault helpers
GEN_EXIT=$(docker inspect -f '{{.State.ExitCode}}' "gen-$RUN_ID")
step "generator finished after $(( $(date +%s) - T0 ))s, exit code $GEN_EXIT"
docker logs "gen-$RUN_ID" 2>&1 | tail -1
docker rm "gen-$RUN_ID" >/dev/null

if [[ -f "runs/$RUN_ID/killed_broker" ]]; then
  step "restart $(cat "runs/$RUN_ID/killed_broker")"
  $COMPOSE up -d --no-deps "$(cat "runs/$RUN_ID/killed_broker")" >/dev/null 2>&1
fi

step "expected trades for the same parameters, then verify"
PYTHONPATH=.:generator uv run --quiet python -m tradegen "${GEN_ARGS[@]}" --speed=max \
  --dump-csv "runs/$RUN_ID/trades.csv" >/dev/null
uv run --quiet python -m verify.verify --trades "runs/$RUN_ID/trades.csv" --wait 300 \
  --report "runs/$RUN_ID/verify.json"
echo "report: runs/$RUN_ID/verify.json"
