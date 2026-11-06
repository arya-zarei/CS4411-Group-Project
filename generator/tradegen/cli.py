"""tradegen: publish trades to Kafka (PLAN.md 4.3, GEN-1 to GEN-5).

Examples:
  tradegen --mode virtual --speed 30 --minutes 40 --seed 42
  tradegen --mode virtual --speed max --minutes 40 --seed 42 --dump-csv /tmp/t.csv   (no Kafka needed)
  tradegen --mode replay --file data/sample/trades.csv.gz --speed max
  tradegen --mode realtime --rate 5000 --duration 300 --acks all --run-id r001 --log-trades
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from .model import CSV_HEADER, Trade, csv_line, flush_trade, replay_trades, virtual_params, virtual_trades
from .producer import TradeProducer, wait_until

DEFAULT_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="tradegen", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--mode", choices=["virtual", "realtime", "replay"], required=True)
    # virtual
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--minutes", type=int, default=40, help="virtual minutes to generate")
    ap.add_argument("--symbols", type=int, default=100)
    ap.add_argument("--mean-rate-per-symbol", type=float, default=0.25, help="trades/s per symbol (virtual)")
    ap.add_argument(
        "--shock-rate-per-min",
        type=float,
        default=None,
        help="GEN-5: shocks per virtual minute after warm-up",
    )
    ap.add_argument(
        "--speed", default="max", help="virtual seconds per wall second, or 'max' (virtual, replay)"
    )
    # replay
    ap.add_argument("--file", type=Path, help="CSV(.gz) or JSON lines to replay")
    # realtime
    ap.add_argument("--rate", type=float, default=1000, help="trades per second (realtime)")
    ap.add_argument("--duration", type=float, default=60, help="seconds (realtime)")
    # output
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default="trades")
    ap.add_argument("--no-flush-trade", action="store_true", help="do not send the closing __FLUSH__ trade")
    ap.add_argument("--dump-csv", type=Path, help="write trades to this CSV instead of Kafka")
    ap.add_argument("--run-id", default=None, help="writes runs/<run-id>/ (config, summary, ack log)")
    ap.add_argument("--runs-dir", type=Path, default=Path(os.environ.get("RUNS_DIR", "runs")))
    ap.add_argument("--log-trades", action="store_true", help="GEN-2: per-trade ack log (needs --run-id)")
    # producer settings (recorded in every run log)
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"])
    ap.add_argument(
        "--idempotence",
        default=None,
        choices=["true", "false"],
        help="default: true with --acks all (no duplicates on producer retries), false otherwise",
    )
    ap.add_argument("--linger-ms", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=1_000_000)
    ap.add_argument("--retries", type=int, default=2147483647)
    ap.add_argument("--queue-max-messages", type=int, default=500_000)
    a = ap.parse_args(argv)
    if a.mode == "replay" and not a.file:
        ap.error("--mode replay needs --file")
    if a.log_trades and not a.run_id:
        ap.error("--log-trades needs --run-id")
    a.speed = None if str(a.speed).lower() == "max" else float(a.speed)
    if a.idempotence is None:
        a.idempotence = "true" if a.acks == "all" else "false"
    elif a.idempotence == "true" and a.acks != "all":
        ap.error("--idempotence true needs --acks all")
    return a


def producer_conf(a: argparse.Namespace) -> dict:
    return {
        "acks": a.acks,
        "enable.idempotence": a.idempotence == "true",
        "linger.ms": a.linger_ms,
        "batch.size": a.batch_size,
        "retries": a.retries,
        "queue.buffering.max.messages": a.queue_max_messages,
    }


def git_commit() -> str | None:
    try:
        return (
            subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5
            ).stdout.strip()
            or None
        )
    except Exception:
        return None


def paced(trades, speed: float | None):
    """Send in event-time order; with a speed, wall time follows virtual time at that factor."""
    t0_wall = t0_event = None
    for t in trades:
        if speed is not None:
            if t0_wall is None:
                t0_wall, t0_event = time.monotonic(), t.event_time_ms
            target = t0_wall + (t.event_time_ms - t0_event) / 1000.0 / speed
            while time.monotonic() < target - 0.001:
                wait_until(target)
        yield t


def realtime_source(a: argparse.Namespace):
    from .model import RealtimeModel

    model = RealtimeModel(seed=a.seed, n_symbols=a.symbols)
    tick = 0.01  # 10 ms batches
    start = time.monotonic()
    sent = 0
    while (elapsed := time.monotonic() - start) < a.duration:
        due = int(a.rate * elapsed) - sent
        if due > 0:
            yield from model.batch(due, a.rate)
            sent += due
        else:
            time.sleep(tick / 2)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    if a.mode == "virtual":
        params = virtual_params(a.seed, a.minutes, a.symbols, a.mean_rate_per_symbol, a.shock_rate_per_min)
        source = paced(virtual_trades(params), a.speed)
    elif a.mode == "replay":
        params = {"file": str(a.file)}
        source = paced(replay_trades(a.file), a.speed)
    else:
        params = {"rate": a.rate, "duration": a.duration, "seed": a.seed, "symbols": a.symbols}
        source = realtime_source(a)

    run_dir = a.runs_dir / a.run_id if a.run_id else None
    config = {
        "tool": "tradegen",
        "mode": a.mode,
        "speed": a.speed if a.speed is not None else "max",
        "params": params,
        "topic": a.topic,
        "producer": producer_conf(a),
        "flush_trade": not a.no_flush_trade,
        "started_at": datetime.now(UTC).isoformat(),
        "git_commit": git_commit(),
        "host": {"python": sys.version.split()[0], "platform": platform.platform()},
    }
    if run_dir:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "generator_config.json").write_text(json.dumps(config, indent=2) + "\n")

    started = time.monotonic()
    last: Trade | None = None

    if a.dump_csv:
        with open(a.dump_csv, "w") as f:
            f.write(CSV_HEADER)
            for t in source:
                f.write(csv_line(t))
                last = t
        print(json.dumps({"dumped": str(a.dump_csv), "last_trade_id": last.trade_id if last else 0}))
        return 0

    prod = TradeProducer(
        a.bootstrap, a.topic, producer_conf(a), run_dir / "acks.csv" if a.log_trades and run_dir else None
    )
    try:
        for t in source:
            prod.send(t)
            last = t
        if not a.no_flush_trade:
            prod.send(flush_trade(last))
    except KeyboardInterrupt:
        print("interrupted; flushing", file=sys.stderr)
    undelivered = prod.close()
    elapsed = time.monotonic() - started
    summary = {
        "attempted": prod.attempted,
        "acked": prod.acked,
        "failed": prod.failed,
        "undelivered_at_exit": undelivered,
        "backpressure_waits": prod.backpressure_waits,
        "first_error": prod.first_error,
        "elapsed_s": round(elapsed, 3),
        "achieved_rate_per_s": round(prod.attempted / elapsed, 1) if elapsed > 0 else None,
        "last_trade_id": last.trade_id if last else 0,
    }
    print(json.dumps(summary))  # GEN-4
    if run_dir:
        (run_dir / "generator_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if prod.failed == 0 and undelivered == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
