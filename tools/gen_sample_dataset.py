#!/usr/bin/env python3
"""Reference implementation and golden-dataset generator.

This script is the *oracle* for the project. It defines, in plain pandas:
  * how synthetic trades are generated (seeded, deterministic),
  * how 1-minute bars (OHLC + VWAP) are computed,
  * how the bar-based alert rule is evaluated.

The real generator (generator/), the Spark job (spark/) and the verifier
(verify/) must agree with the functions in this file. When they disagree,
this file wins until the team decides otherwise (record it in docs/decisions.md).

Usage:
    python tools/gen_sample_dataset.py --out data/sample          # (re)generate
    python tools/gen_sample_dataset.py --out data/sample --check  # compare with MANIFEST.json

Pinned behaviour: numpy.random.Generator(PCG64) streams are only guaranteed
stable for a given numpy version, so MANIFEST.json records the versions used.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# Parameters that define the data and the alert rule (see PLAN.md section 4, 5.5)
# ----------------------------------------------------------------------------
WINDOW_MS = 60_000          # 1-minute tumbling windows aligned to the epoch minute
BASELINE_BARS = 30          # alert baseline: previous 30 bars
Z_THRESHOLD = 3.0           # alert if |change - mean| > 3 * std
T0_MS = int(pd.Timestamp("2026-01-05T14:30:00Z").timestamp() * 1000)  # fixed virtual start

DEFAULTS = dict(
    seed=42,
    n_symbols=100,
    minutes=40,             # needs > 31 bars per symbol before the first alert is possible
    mean_rate_per_symbol=0.25,   # trades per second, averaged over symbols
    n_shocks=20,
    shock_start_s=32 * 60,  # shocks only where a full 30-bar baseline exists
    shock_end_s=38 * 60 + 30,
)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ----------------------------------------------------------------------------
# Trade generation
# ----------------------------------------------------------------------------
def make_universe(n_symbols: int, seed: int, mean_rate: float):
    ss = np.random.SeedSequence(seed)
    children = ss.spawn(n_symbols + 2)
    rng_u = np.random.default_rng(children[0])
    rng_shock = np.random.default_rng(children[1])
    ranks = rng_u.permutation(n_symbols) + 1
    w = ranks.astype(float) ** -0.5
    lam = mean_rate * w / w.mean()                                   # trades/second
    s0 = np.clip(np.exp(rng_u.normal(math.log(60), 0.8, n_symbols)), 10, 800).round(4)
    sigma_1min = rng_u.uniform(0.0005, 0.0012, n_symbols)            # std of 1-min log return
    symbols = [f"SYM{i + 1:03d}" for i in range(n_symbols)]
    uni = pd.DataFrame(dict(symbol=symbols, start_price=s0, sigma_1min=sigma_1min,
                            rate_per_s=lam.round(6)))
    return uni, children, rng_shock


def make_shocks(uni, rng, n_shocks, start_s, end_s):
    idx = rng.choice(len(uni), size=n_shocks, replace=False)
    t_ms = np.sort(rng.integers(start_s * 1000, end_s * 1000, n_shocks))
    sign = rng.choice([-1.0, 1.0], size=n_shocks)
    mag = rng.uniform(0.015, 0.03, n_shocks) * sign                  # log-price jump
    return pd.DataFrame(dict(symbol=uni["symbol"].to_numpy()[idx],
                             offset_ms=t_ms, event_time_ms=T0_MS + t_ms,
                             log_jump=mag.round(6)))


def make_trades(p: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    uni, children, rng_shock = make_universe(p["n_symbols"], p["seed"], p["mean_rate_per_symbol"])
    total_ms = p["minutes"] * 60_000
    shocks = make_shocks(uni, rng_shock, p["n_shocks"], p["shock_start_s"], p["shock_end_s"])
    frames = []
    for i, row in enumerate(uni.itertuples(index=False)):
        rng = np.random.default_rng(children[2 + i])
        n = int(rng.poisson(row.rate_per_s * total_ms / 1000))
        t = np.sort(rng.integers(0, total_ms, n))
        dt = np.diff(t, prepend=0) / 1000.0
        sigma_s = row.sigma_1min / math.sqrt(60.0)
        eps = rng.normal(0.0, 1.0, n) * sigma_s * np.sqrt(dt)
        logp = math.log(row.start_price) + np.cumsum(eps)
        for s in shocks[shocks.symbol == row.symbol].itertuples(index=False):
            logp = logp + np.where(t >= s.offset_ms, s.log_jump, 0.0)
        size = np.clip(np.round(np.exp(rng.normal(math.log(100), 0.8, n))), 1, 10_000)
        frames.append(pd.DataFrame(dict(symbol=row.symbol, event_time_ms=T0_MS + t,
                                        price=np.round(np.exp(logp), 4), size=size.astype(int))))
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["event_time_ms", "symbol"], kind="stable").reset_index(drop=True)
    df.insert(0, "trade_id", np.arange(1, len(df) + 1, dtype="int64"))
    return df, uni, shocks


# ----------------------------------------------------------------------------
# Reference bars and alerts (the oracle for Spark + verifier)
# ----------------------------------------------------------------------------
def compute_bars(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.sort_values(["event_time_ms", "trade_id"], kind="stable").copy()
    t["window_start_ms"] = (t["event_time_ms"] // WINDOW_MS) * WINDOW_MS
    t["notional"] = t["price"] * t["size"]
    g = t.groupby(["symbol", "window_start_ms"], sort=True)
    bars = g.agg(open=("price", "first"), high=("price", "max"), low=("price", "min"),
                 close=("price", "last"), volume=("size", "sum"), notional=("notional", "sum"),
                 trade_count=("trade_id", "count")).reset_index()
    bars["vwap"] = bars["notional"] / bars["volume"]
    bars["window_end_ms"] = bars["window_start_ms"] + WINDOW_MS
    return bars.drop(columns="notional")[["symbol", "window_start_ms", "window_end_ms", "open", "high",
                                          "low", "close", "volume", "vwap", "trade_count"]]


def compute_alerts(bars: pd.DataFrame) -> pd.DataFrame:
    """Alert rule: change = ln(close_t / close_{t-1}) over consecutive *existing* bars of a symbol.
    Baseline = mean and sample std (ddof=1) of the previous 30 changes. Alert if std > 0 and
    |change - mean| > 3 * std. The first eligible bar of a symbol is therefore its 32nd bar."""
    out = []
    for sym, b in bars.sort_values(["symbol", "window_start_ms"]).groupby("symbol", sort=True):
        b = b.reset_index(drop=True)
        chg = np.log(b["close"] / b["close"].shift(1))
        prev = chg.shift(1)
        mu = prev.rolling(BASELINE_BARS, min_periods=BASELINE_BARS).mean()
        sd = prev.rolling(BASELINE_BARS, min_periods=BASELINE_BARS).std(ddof=1)
        z = (chg - mu) / sd
        hit = (sd > 0) & ((chg - mu).abs() > Z_THRESHOLD * sd)
        a = b.loc[hit, ["symbol", "window_start_ms", "window_end_ms", "close"]].copy()
        a["log_return"] = chg[hit]
        a["baseline_mean"] = mu[hit]
        a["baseline_std"] = sd[hit]
        a["z_score"] = z[hit]
        out.append(a)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def shock_detection(shocks: pd.DataFrame, bars: pd.DataFrame, alerts: pd.DataFrame) -> dict:
    """A shock counts as detected if its symbol has an alert in the bar containing it or the next bar."""
    detected = 0
    for s in shocks.itertuples(index=False):
        ws = (s.event_time_ms // WINDOW_MS) * WINDOW_MS
        a = alerts[(alerts.symbol == s.symbol) & (alerts.window_start_ms.isin([ws, ws + WINDOW_MS]))]
        detected += int(len(a) > 0)
    return dict(shocks=len(shocks), detected=detected, total_alerts=len(alerts),
                alerts_not_explained_by_shocks=len(alerts) - detected)


# ----------------------------------------------------------------------------
# I/O
# ----------------------------------------------------------------------------
def csv_bytes(df: pd.DataFrame, **kw) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False, lineterminator="\n", **kw)
    return buf.getvalue().encode()


def write_gz(path: Path, raw: bytes) -> None:
    with open(path, "wb") as f, gzip.GzipFile(fileobj=f, mode="wb", mtime=0, compresslevel=9) as gz:
        gz.write(raw)


def build(p: dict):
    trades, uni, shocks = make_trades(p)
    bars = compute_bars(trades)
    alerts = compute_alerts(bars)
    files = {
        "trades.csv.gz": csv_bytes(trades, float_format="%.4f"),
        "bars_expected.csv": csv_bytes(bars, float_format="%.10g"),
        "alerts_expected.csv": csv_bytes(alerts, float_format="%.10g"),
        "symbols.csv": csv_bytes(uni),
        "shocks.json": (json.dumps(shocks.to_dict("records"), indent=1) + "\n").encode(),
    }
    return files, trades, bars, alerts, shocks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=Path("data/sample"))
    ap.add_argument("--check", action="store_true", help="regenerate in memory and compare with MANIFEST.json")
    for k, v in DEFAULTS.items():
        ap.add_argument(f"--{k.replace('_', '-')}", type=type(v), default=v)
    a = ap.parse_args()
    p = {k: getattr(a, k) for k in DEFAULTS}
    files, trades, bars, alerts, shocks = build(p)
    digests = {n: sha256_bytes(b) for n, b in files.items()}   # hash of UNCOMPRESSED content

    if a.check:
        m = json.loads((a.out / "MANIFEST.json").read_text())
        bad = [n for n, d in digests.items() if m["sha256_uncompressed"].get(n) != d]
        print("MATCH" if not bad else f"MISMATCH: {bad} (numpy {np.__version__}, "
              f"manifest numpy {m['versions']['numpy']})")
        return 1 if bad else 0

    a.out.mkdir(parents=True, exist_ok=True)
    for n, raw in files.items():
        if n.endswith(".gz"):
            write_gz(a.out / n, raw)
        else:
            (a.out / n).write_bytes(raw)
    det = shock_detection(shocks, bars, alerts)
    manifest = dict(
        params=p, window_ms=WINDOW_MS, baseline_bars=BASELINE_BARS, z_threshold=Z_THRESHOLD,
        t0_utc="2026-01-05T14:30:00Z", t0_ms=T0_MS,
        rows=dict(trades=len(trades), bars=len(bars), alerts=len(alerts), shocks=len(shocks)),
        shock_detection=det, sha256_uncompressed=digests,
        versions=dict(numpy=np.__version__, pandas=pd.__version__, python=sys.version.split()[0]),
    )
    (a.out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest["rows"]), json.dumps(det))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
