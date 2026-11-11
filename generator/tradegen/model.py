"""Trade models: virtual (the oracle's own model), realtime, and file replay.

Every source yields ``Trade`` tuples in send order. Virtual mode calls the oracle in
``tools/gen_sample_dataset.py`` directly, so its output equals the golden dataset by construction
(GEN-1) and cannot drift from the bars and alerts the verifier expects.
"""

from __future__ import annotations

import csv
import gzip
import json
import math
import time
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import numpy as np

from tools import gen_sample_dataset as oracle

FLUSH_SYMBOL = "__FLUSH__"
FLUSH_DELAY_MS = 120_000  # two minutes after the last trade: closes every real window (PLAN.md 4.3)


class Trade(NamedTuple):
    trade_id: int
    symbol: str
    event_time_ms: int
    price: float
    size: float


def flush_trade(last: Trade | None) -> Trade:
    """The reserved trade that advances the watermark past every real window. Verification ignores it."""
    if last is None:
        return Trade(1, FLUSH_SYMBOL, oracle.T0_MS + FLUSH_DELAY_MS, 1.0, 1)
    return Trade(last.trade_id + 1, FLUSH_SYMBOL, last.event_time_ms + FLUSH_DELAY_MS, 1.0, 1)


# --- virtual -------------------------------------------------------------------------------------
def virtual_params(
    seed: int = 42,
    minutes: int = 40,
    n_symbols: int = 100,
    mean_rate_per_symbol: float = 0.25,
    shock_rate_per_min: float | None = None,
) -> dict:
    """Oracle parameters. Without ``shock_rate_per_min`` the oracle defaults are used unchanged, so
    seed 42 and 40 minutes give exactly ``data/sample``. With it (GEN-5, the continuing demo), shocks
    are spread from minute 32 (after the 30-bar warm-up) to 30 s before the end, one per symbol at most."""
    p = dict(oracle.DEFAULTS)
    p.update(seed=seed, minutes=minutes, n_symbols=n_symbols, mean_rate_per_symbol=mean_rate_per_symbol)
    if shock_rate_per_min is not None:
        start_s = p["shock_start_s"]
        end_s = max(start_s + 1, minutes * 60 - 30)
        n = round(shock_rate_per_min * (end_s - start_s) / 60)
        p.update(n_shocks=max(0, min(n, n_symbols)), shock_end_s=end_s)
    return p


def virtual_trades(params: dict) -> Iterator[Trade]:
    trades, _, _ = oracle.make_trades(params)
    cols = [trades[c].to_numpy() for c in ("trade_id", "symbol", "event_time_ms", "price", "size")]
    for tid, sym, t, price, size in zip(*cols, strict=True):
        yield Trade(int(tid), str(sym), int(t), float(price), int(size))


# --- replay --------------------------------------------------------------------------------------
def replay_trades(path: Path) -> Iterator[Trade]:
    """Replay a CSV (optionally gzipped, header trade_id,symbol,event_time_ms,price,size) or JSON lines."""
    opener = gzip.open if path.suffix == ".gz" else open
    is_jsonl = ".jsonl" in path.suffixes or ".json" in path.suffixes
    with opener(path, "rt", newline="") as f:
        if is_jsonl:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    yield Trade(
                        int(d["trade_id"]), d["symbol"], int(d["event_time_ms"]), float(d["price"]), d["size"]
                    )
        else:
            for r in csv.DictReader(f):
                size = float(r["size"])
                yield Trade(
                    int(r["trade_id"]),
                    r["symbol"],
                    int(r["event_time_ms"]),
                    float(r["price"]),
                    int(size) if size.is_integer() else size,
                )


# --- realtime ------------------------------------------------------------------------------------
class RealtimeModel:
    """Same universe as the oracle (symbols, start prices, volatilities, rate skew), but trades are
    stamped with the wall clock when created and the total rate is set by the caller. Used for latency
    and throughput experiments (E1), where only timing matters, not a known answer."""

    def __init__(self, seed: int = 42, n_symbols: int = 100):
        uni, _, _ = oracle.make_universe(n_symbols, seed, 1.0)
        self.symbols = uni["symbol"].to_numpy()
        self.weights = (uni["rate_per_s"] / uni["rate_per_s"].sum()).to_numpy()
        self.logp = np.log(uni["start_price"].to_numpy())
        # Per-trade step so that each symbol's 1-minute volatility matches its sigma at the default mix.
        self.sigma = uni["sigma_1min"].to_numpy()
        self.rng = np.random.default_rng(np.random.SeedSequence(seed).spawn(n_symbols + 3)[-1])
        self.next_id = 1

    def batch(self, n: int, rate: float) -> list[Trade]:
        now_ms = int(time.time() * 1000)
        idx = self.rng.choice(len(self.symbols), size=n, p=self.weights)
        dt_s = 1.0 / np.maximum(rate * self.weights[idx], 1e-9)  # expected gap between this symbol's trades
        steps = (
            self.rng.normal(0.0, 1.0, n) * self.sigma[idx] / math.sqrt(60.0) * np.sqrt(np.minimum(dt_s, 60))
        )
        sizes = np.clip(np.round(np.exp(self.rng.normal(math.log(100), 0.8, n))), 1, 10_000).astype(int)
        out = []
        for k in range(n):
            i = idx[k]
            self.logp[i] += steps[k]
            out.append(
                Trade(
                    self.next_id,
                    str(self.symbols[i]),
                    now_ms,
                    round(math.exp(self.logp[i]), 4),
                    int(sizes[k]),
                )
            )
            self.next_id += 1
        return out


# --- formatting ----------------------------------------------------------------------------------
def kafka_value(t: Trade) -> bytes:
    """JSON value of one Kafka message (PLAN.md 4.2)."""
    size = t.size if isinstance(t.size, int) else repr(float(t.size))
    # 4 decimals, exactly like trades.csv.gz, whenever that loses nothing (all synthetic prices);
    # full precision otherwise (live crypto prices such as 0.119812).
    price = f"{t.price:.4f}" if round(t.price, 4) == t.price else repr(float(t.price))
    return (
        f'{{"trade_id":{t.trade_id},"symbol":"{t.symbol}","event_time_ms":{t.event_time_ms},'
        f'"price":{price},"size":{size}}}'
    ).encode()


def csv_line(t: Trade) -> str:
    """One row in the format of data/sample/trades.csv.gz."""
    return f"{t.trade_id},{t.symbol},{t.event_time_ms},{t.price:.4f},{t.size}\n"


CSV_HEADER = "trade_id,symbol,event_time_ms,price,size\n"
