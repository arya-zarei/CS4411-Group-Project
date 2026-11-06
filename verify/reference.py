"""Oracle: reference bars and alert rule.

This module is a COPY of the bar and alert logic in ``tools/gen_sample_dataset.py``. That script
is the source of truth. If one changes, change both in the same pull request, regenerate
``data/sample/`` and update ``MANIFEST.json`` (see CLAUDE.md). ``tests/test_oracle.py`` fails if
the copies drift apart.

Used by the verifier (VER-2, VER-3) and by tests as the definition of "correct".
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

WINDOW_MS = 60_000  # 1-minute tumbling windows aligned to the epoch minute
BASELINE_BARS = 30  # alert baseline: previous 30 bars
Z_THRESHOLD = 3.0  # alert if |change - mean| > 3 * std

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def load_sample_trades(sample_dir: Path = SAMPLE_DIR) -> pd.DataFrame:
    return pd.read_csv(sample_dir / "trades.csv.gz")


def load_expected_bars(sample_dir: Path = SAMPLE_DIR) -> pd.DataFrame:
    return pd.read_csv(sample_dir / "bars_expected.csv")


def load_expected_alerts(sample_dir: Path = SAMPLE_DIR) -> pd.DataFrame:
    return pd.read_csv(sample_dir / "alerts_expected.csv")


# --- begin copy of tools/gen_sample_dataset.py ------------------------------------------------
def compute_bars(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.sort_values(["event_time_ms", "trade_id"], kind="stable").copy()
    t["window_start_ms"] = (t["event_time_ms"] // WINDOW_MS) * WINDOW_MS
    t["notional"] = t["price"] * t["size"]
    g = t.groupby(["symbol", "window_start_ms"], sort=True)
    bars = g.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
        notional=("notional", "sum"),
        trade_count=("trade_id", "count"),
    ).reset_index()
    bars["vwap"] = bars["notional"] / bars["volume"]
    bars["window_end_ms"] = bars["window_start_ms"] + WINDOW_MS
    return bars.drop(columns="notional")[
        [
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
        ]
    ]


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


# --- end copy ---------------------------------------------------------------------------------
