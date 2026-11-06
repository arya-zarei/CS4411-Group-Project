"""Verifier: is the pipeline's output correct? (PLAN.md 5.8: VER-2, VER-3, VER-4, VER-5)

Computes the expected bars and alerts from a trade file with the oracle, reads what the pipeline wrote
to PostgreSQL, and compares. Exits 0 on PASS and 1 on FAIL, and writes a JSON report.

  python -m verify.verify --trades data/sample/trades.csv.gz --shocks data/sample/shocks.json
  python -m verify.verify --trades runs/demo/trades.csv --wait 300      # wait for the job to finish first

Rules:
* Bars: open, high, low, close, volume, trade_count and window_end must match exactly; VWAP within 1e-6.
* Alerts: the set of (symbol, window_start) must match; z-score and the other values within 1e-6.
  Alerts whose |z| is within 1e-9 of the threshold are ignored on both sides (floating-point ties).
* The reserved ``__FLUSH__`` trade is ignored for bars. Unless ``--no-flush`` is given, the verifier
  assumes the run ended with tradegen's flush trade (two minutes after the last trade), which closes
  every window; trade files do not contain it because tradegen adds it when sending.
* Windows that had not closed when the run ended are ignored (VER-5). A window is closed when its end
  is at or before (latest event time - watermark). Windows within one second of that cutoff are
  neither required nor forbidden.
* In insert mode (``--sink insert``) duplicate rows in ``bars_plain`` / ``alerts_plain`` are reported
  (Experiment 4); in upsert mode the primary key makes duplicates impossible.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

from verify import reference as ref

FLUSH_SYMBOL = "__FLUSH__"
FLUSH_DELAY_MS = 120_000  # tradegen sends __FLUSH__ two minutes after the last trade (PLAN.md 4.3)
VWAP_TOL = 1e-6
ALERT_TOL = 1e-6
BOUNDARY_TOL = 1e-9
EXACT_BAR_COLS = ["window_end_ms", "open", "high", "low", "close", "volume", "trade_count"]
ALERT_VALUE_COLS = ["close", "log_return", "baseline_mean", "baseline_std", "z_score"]
KEY = ["symbol", "window_start_ms"]
MAX_EXAMPLES = 10

DEFAULT_DSN = os.environ.get("PG_DSN", "postgresql://pipeline:change-me-locally@localhost:5432/market")


# --- expected ------------------------------------------------------------------------------------
def load_trades(path: Path) -> pd.DataFrame:
    if ".jsonl" in path.suffixes:
        return pd.read_json(path, lines=True)
    return pd.read_csv(path)


def with_flush(trades: pd.DataFrame) -> pd.DataFrame:
    """Add the __FLUSH__ trade tradegen sends after the last trade, unless the file already has one.
    Trade files (for example data/sample/trades.csv.gz) do not contain it; tradegen adds it when sending."""
    if (trades["symbol"] == FLUSH_SYMBOL).any():
        return trades
    flush = {
        "trade_id": int(trades["trade_id"].max()) + 1,
        "symbol": FLUSH_SYMBOL,
        "event_time_ms": int(trades["event_time_ms"].max()) + FLUSH_DELAY_MS,
        "price": 1.0,
        "size": 1,
    }
    return pd.concat([trades, pd.DataFrame([flush])], ignore_index=True)


def expected_output(
    trades: pd.DataFrame, watermark_ms: int
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    """Returns (required bars, required alerts, uncertain window keys, cutoff_ms)."""
    cutoff = int(trades["event_time_ms"].max()) - watermark_ms  # includes the flush trade, if any
    real = trades[trades["symbol"] != FLUSH_SYMBOL]
    bars = ref.compute_bars(real)
    alerts = ref.compute_alerts(bars)
    if alerts.empty:
        alerts = pd.DataFrame(columns=[*KEY, "window_end_ms", *ALERT_VALUE_COLS])
    closed = bars["window_end_ms"] <= cutoff - 1000
    uncertain = bars.loc[
        (bars["window_end_ms"] > cutoff - 1000) & (bars["window_end_ms"] <= cutoff + 1000), KEY
    ]
    alerts = alerts[alerts["window_end_ms"] <= cutoff - 1000]
    return bars[closed].reset_index(drop=True), alerts.reset_index(drop=True), uncertain, cutoff


# --- comparison (pure; unit-tested) --------------------------------------------------------------
def _keys(df: pd.DataFrame) -> set[tuple[str, int]]:
    return set(zip(df["symbol"], df["window_start_ms"].astype("int64"), strict=True))


def _examples(keys) -> list[list]:
    return [list(k) for k in sorted(keys)[:MAX_EXAMPLES]]


def compare_bars(expected: pd.DataFrame, actual: pd.DataFrame, uncertain: pd.DataFrame | None = None) -> dict:
    ignore = _keys(uncertain) if uncertain is not None and len(uncertain) else set()
    dup_rows = int(len(actual) - actual.drop_duplicates(KEY).shape[0])
    first = actual.drop_duplicates(KEY, keep="first")
    exp_keys, act_keys = _keys(expected), _keys(first) - ignore
    missing, extra = exp_keys - act_keys, act_keys - exp_keys
    merged = expected.merge(first, on=KEY, suffixes=("_exp", "_act"))
    bad = pd.Series(False, index=merged.index)
    per_col = {}
    for c in EXACT_BAR_COLS:
        diff = merged[f"{c}_exp"] != merged[f"{c}_act"]
        per_col[c] = int(diff.sum())
        bad |= diff
    vdiff = (merged["vwap_exp"] - merged["vwap_act"]).abs() > VWAP_TOL
    per_col["vwap"] = int(vdiff.sum())
    bad |= vdiff
    mismatched = merged.loc[bad, KEY]
    return {
        "expected": len(expected),
        "actual_rows": len(actual),
        "matched": int(len(merged) - bad.sum()),
        "missing": len(missing),
        "extra": len(extra),
        "duplicate_rows": dup_rows,
        "value_mismatches": int(bad.sum()),
        "mismatches_by_column": per_col,
        "ignored_unclosed": len(ignore),
        "examples": {
            "missing": _examples(missing),
            "extra": _examples(extra),
            "mismatched": _examples(_keys(mismatched)),
        },
        "ok": not (missing or extra or dup_rows or bad.any()),
    }


def compare_alerts(expected: pd.DataFrame, actual: pd.DataFrame) -> dict:
    def borderline(df):
        return _keys(df[((df["z_score"].abs() - ref.Z_THRESHOLD).abs() < BOUNDARY_TOL)]) if len(df) else set()

    dup_rows = int(len(actual) - actual.drop_duplicates(KEY).shape[0])
    first = actual.drop_duplicates(KEY, keep="first")
    skip = borderline(expected) | borderline(first)
    exp_keys, act_keys = _keys(expected) - skip, _keys(first) - skip
    missing, extra = exp_keys - act_keys, act_keys - exp_keys
    merged = expected.merge(first, on=KEY, suffixes=("_exp", "_act"))
    bad = pd.Series(False, index=merged.index)
    for c in ALERT_VALUE_COLS:
        bad |= (merged[f"{c}_exp"] - merged[f"{c}_act"]).abs() > ALERT_TOL
    return {
        "expected": len(expected),
        "actual_rows": len(actual),
        "matched": int(len(merged) - bad.sum()),
        "missing": len(missing),
        "extra": len(extra),
        "duplicate_rows": dup_rows,
        "value_mismatches": int(bad.sum()),
        "ignored_borderline": len(skip),
        "examples": {
            "missing": _examples(missing),
            "extra": _examples(extra),
            "mismatched": _examples(_keys(merged.loc[bad, KEY])),
        },
        "ok": not (missing or extra or dup_rows or bad.any()),
    }


def shock_report(shocks_path: Path, actual_alerts: pd.DataFrame, cutoff_ms: int) -> dict:
    """VER-4 (informational): share of injected shocks that produced an alert in the same or next bar."""
    shocks = pd.DataFrame(json.loads(shocks_path.read_text()))
    shocks = shocks[shocks["event_time_ms"] + 2 * ref.WINDOW_MS <= cutoff_ms]  # only shocks whose bars closed
    detected = 0
    for s in shocks.itertuples(index=False):
        ws = (s.event_time_ms // ref.WINDOW_MS) * ref.WINDOW_MS
        a = actual_alerts[
            (actual_alerts["symbol"] == s.symbol)
            & actual_alerts["window_start_ms"].isin([ws, ws + ref.WINDOW_MS])
        ]
        detected += int(len(a) > 0)
    return {
        "shocks": len(shocks),
        "detected": detected,
        "fraction": round(detected / len(shocks), 4) if len(shocks) else None,
    }


# --- database ------------------------------------------------------------------------------------
TABLES = {"upsert": ("bars", "alerts"), "insert": ("bars_plain", "alerts_plain")}
MS = "(extract(epoch from {c}) * 1000)::bigint"


def read_actual(conn, sink: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    bars_t, alerts_t = TABLES[sink]
    order = ", id" if sink == "insert" else ""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT symbol, {MS.format(c='window_start')}, {MS.format(c='window_end')}, "
            f"open, high, low, close, volume, vwap, trade_count "
            f"FROM {bars_t} ORDER BY symbol, window_start{order}"
        )
        bars = pd.DataFrame(
            cur.fetchall(),
            columns=[
                "symbol",
                "window_start_ms",
                "window_end_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "trade_count",
            ],
        )
        cur.execute(
            f"SELECT symbol, {MS.format(c='window_start')}, {MS.format(c='window_end')}, close, log_return, "
            f"baseline_mean, baseline_std, z_score FROM {alerts_t} ORDER BY symbol, window_start{order}"
        )
        alerts = pd.DataFrame(cur.fetchall(), columns=[*KEY, "window_end_ms", *ALERT_VALUE_COLS])
    return bars, alerts


def wait_for_pipeline(conn, sink: str, timeout_s: float, stable_s: float = 10.0) -> dict:
    """Wait until Spark has caught up (latest progress event has lag 0) and the bar count has not
    changed for ``stable_s`` seconds. Returns what was observed; never raises on timeout."""
    bars_t = TABLES[sink][0]
    deadline = time.monotonic() + timeout_s
    last_count, stable_since = -1, time.monotonic()
    lag = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {bars_t}")
            count = cur.fetchone()[0]
            cur.execute("SELECT lag_total FROM query_progress ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            lag = row[0] if row else None
        conn.commit()
        if count != last_count:
            last_count, stable_since = count, time.monotonic()
        if lag == 0 and count > 0 and time.monotonic() - stable_since >= stable_s:
            return {"waited": True, "timed_out": False, "bars": count, "lag": lag}
        time.sleep(1)
    return {"waited": True, "timed_out": True, "bars": last_count, "lag": lag}


# --- main ----------------------------------------------------------------------------------------
def run(args: argparse.Namespace) -> dict:
    import psycopg

    trades = load_trades(args.trades)
    if not args.no_flush:
        trades = with_flush(trades)
    exp_bars, exp_alerts, uncertain, cutoff = expected_output(trades, args.watermark_ms)
    with psycopg.connect(args.dsn) as conn:
        waited = wait_for_pipeline(conn, args.sink, args.wait) if args.wait else {"waited": False}
        act_bars, act_alerts = read_actual(conn, args.sink)
    report = {
        "trades_file": str(args.trades),
        "sink": args.sink,
        "watermark_ms": args.watermark_ms,
        "cutoff_ms": cutoff,
        "wait": waited,
        "bars": compare_bars(exp_bars, act_bars, uncertain),
        "alerts": compare_alerts(exp_alerts, act_alerts),
    }
    if args.shocks:
        report["shocks"] = shock_report(args.shocks, act_alerts, cutoff)
    report["status"] = "PASS" if report["bars"]["ok"] and report["alerts"]["ok"] else "FAIL"
    return report


def summary_line(report: dict) -> str:
    def part(name: str, r: dict) -> str:
        return (
            f"{name} {r['matched']}/{r['expected']} matched (missing {r['missing']}, extra {r['extra']}, "
            f"duplicate rows {r['duplicate_rows']}, mismatched {r['value_mismatches']})"
        )

    line = f"{report['status']}: {part('bars', report['bars'])}; {part('alerts', report['alerts'])}"
    if "shocks" in report:
        line += f"; shocks detected {report['shocks']['detected']}/{report['shocks']['shocks']}"
    return line


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--trades",
        type=Path,
        required=True,
        help="trade file the run was produced from (CSV, .gz, JSON lines)",
    )
    ap.add_argument("--shocks", type=Path, help="shocks.json, for the informational VER-4 report")
    ap.add_argument("--sink", choices=["upsert", "insert"], default="upsert")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--watermark-ms", type=int, default=10_000)
    ap.add_argument(
        "--no-flush",
        action="store_true",
        help="the run did not send a __FLUSH__ trade (tradegen --no-flush-trade)",
    )
    ap.add_argument("--wait", type=float, default=0, help="seconds to wait for the pipeline to finish first")
    ap.add_argument("--report", type=Path, help="write the JSON report here")
    args = ap.parse_args(argv)
    report = run(args)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(summary_line(report))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
