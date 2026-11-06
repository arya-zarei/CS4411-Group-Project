#!/usr/bin/env bash
# T14 / Gate G1: replay the golden dataset through Kafka and Spark into PostgreSQL, then verify.
# Ends with PASS (exit 0) or FAIL (exit 1). Run from the repository root: make golden
#
# The topic is preloaded completely before Spark starts (PLAN.md 5.9, deterministic replay mode).
# An unpaced replay packs 40 virtual minutes into a few wall seconds, so a few milliseconds of
# skew between partitions would look like more than the 10 s watermark and trades would be dropped
# as late. Preloading puts everything in one batch, so the result is exact and repeatable.
set -euo pipefail

RUN_ID="${RUN_ID:-golden-$(date -u +%Y%m%dT%H%M%SZ)}"
COMPOSE="${COMPOSE:-docker compose}"
TRADES=data/sample/trades.csv.gz
mkdir -p "runs/$RUN_ID"

step() { printf '\n== %s\n' "$*"; }

step "1/6 reset: remove containers and volumes (Kafka data, PostgreSQL, Spark checkpoint)"
$COMPOSE --profile smoke down -v --remove-orphans

step "2/6 build images (BUILD=0 skips this and uses the images already built)"
if [[ "${BUILD:-1}" == 1 ]]; then $COMPOSE build generator spark-bars; fi

step "3/6 start Kafka x3, topics, PostgreSQL, Grafana"
$COMPOSE up -d --wait kafka-1 kafka-2 kafka-3 kafka-init postgres grafana

step "4/6 preload $TRADES into the trades topic (unpaced, acks=all, per-trade ack log)"
$COMPOSE run --rm --no-deps -T --user "$(id -u):$(id -g)" generator \
  --mode=replay --file="/data/sample/trades.csv.gz" --speed=max --acks=all \
  --run-id="$RUN_ID" --log-trades

step "5/6 start the Spark bars job (reads the topic from the earliest offset)"
$COMPOSE up -d --no-deps --wait spark-bars

step "6/6 wait for Spark to finish, then verify bars and alerts against the oracle"
uv run python -m verify.verify --trades "$TRADES" --shocks data/sample/shocks.json \
  --wait 600 --report "runs/$RUN_ID/verify.json"
echo "report: runs/$RUN_ID/verify.json"
