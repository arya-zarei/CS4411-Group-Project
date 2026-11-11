"""Live trades from Coinbase's public WebSocket feed, with capture and replay fallback (T27, LIVE-1 to 6).

Feed: Coinbase Advanced Trade, public market-data endpoint, channel ``market_trades`` (no API key).
Format as documented by Coinbase (subscribe: type, product_ids, channel; messages: channel, timestamp,
sequence_num, events[{type: snapshot|update, trades[{trade_id, product_id, price, size, side, time}]}]).
Prices and sizes may arrive as strings or numbers; both are accepted. Checked against search results
of Coinbase's docs on 2026-11-11; the docs host itself was not reachable from the development sandbox,
so spike S5 must confirm the live format on a teammate's machine.

* LIVE-1: each exchange trade becomes one trade record (PLAN.md 4.2): trade_id from this adapter's own
  counter, symbol = product id, event_time_ms = exchange time in ms, price, size (fractional).
* LIVE-2: reconnect with exponential backoff (1 s doubling to 30 s).
* LIVE-3: every trade sent is appended to a capture file (JSON lines, with the exchange's trade id).
* LIVE-4: with --offline, or when no trade has arrived for --fallback-after seconds, replay the latest
  capture instead, re-stamped to the current time so the pipeline treats it as new data.
* LIVE-6: never used in a benchmark.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from .model import Trade, replay_trades

URL = "wss://advanced-trade-ws.coinbase.com"
DEFAULT_PRODUCTS = "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD,ADA-USD,AVAX-USD,LINK-USD,LTC-USD,BCH-USD"
MAX_BACKOFF_S = 30.0
SEEN_PER_SESSION = 50_000  # exchange trade ids remembered, so snapshots after a reconnect are not resent


def iso_to_ms(ts: str) -> int:
    """'2026-11-11T20:42:27.265123456Z' -> epoch ms (handles nanoseconds and a trailing Z)."""
    ts = ts.strip().replace("Z", "+00:00")
    main, _, rest = ts.partition(".")
    if rest:
        digits = "".join(c for c in rest if c.isdigit())
        tz = rest[len(digits) :]
        ts = f"{main}.{digits[:6].ljust(6, '0')}{tz}"
    return int(datetime.fromisoformat(ts).timestamp() * 1000)


def parse_message(raw: str | bytes) -> list[dict]:
    """Exchange trades in one ``market_trades`` message (empty for any other message)."""
    msg = json.loads(raw)
    if msg.get("channel") != "market_trades":
        return []
    out = []
    for event in msg.get("events") or []:
        for t in event.get("trades") or []:
            out.append(
                {
                    "source_trade_id": str(t["trade_id"]),
                    "symbol": t["product_id"],
                    "event_time_ms": iso_to_ms(t["time"]),
                    "price": float(t["price"]),
                    "size": float(t["size"]),
                    "event_type": event.get("type"),
                }
            )
    out.sort(key=lambda r: (r["event_time_ms"], r["source_trade_id"]))  # snapshots arrive newest first
    return out


def subscribe_messages(products: list[str]) -> list[str]:
    return [
        json.dumps({"type": "subscribe", "product_ids": products, "channel": "market_trades"}),
        json.dumps({"type": "subscribe", "channel": "heartbeats"}),  # keeps quiet connections open
    ]


class LiveSource:
    """Turns exchange trades into numbered trade records, captures them, and hands them to ``emit``."""

    def __init__(self, emit: Callable[[Trade], None], capture: Path | None, start_id: int = 1):
        self.emit = emit
        self.next_id = start_id
        if capture:
            Path(capture).parent.mkdir(parents=True, exist_ok=True)
        self.capture = open(capture, "a", buffering=1) if capture else None  # noqa: SIM115
        self.seen: deque[str] = deque(maxlen=SEEN_PER_SESSION)
        self.seen_set: set[str] = set()
        self.live_trades = self.replayed_trades = self.reconnects = 0
        self.last_trade_monotonic = time.monotonic()
        self.mode = "live"

    def _remember(self, key: str) -> bool:
        if key in self.seen_set:
            return False
        if len(self.seen) == self.seen.maxlen:
            self.seen_set.discard(self.seen[0])
        self.seen.append(key)
        self.seen_set.add(key)
        return True

    def on_exchange_trade(self, r: dict) -> None:
        if not self._remember(f"{r['symbol']}:{r['source_trade_id']}"):
            return
        t = Trade(self.next_id, r["symbol"], r["event_time_ms"], r["price"], r["size"])
        self.next_id += 1
        self.emit(t)
        self.live_trades += 1
        self.last_trade_monotonic = time.monotonic()
        if self.capture:
            self.capture.write(json.dumps({**t._asdict(), "source_trade_id": r["source_trade_id"]}) + "\n")

    def close(self) -> None:
        if self.capture:
            self.capture.close()


async def run_live(
    source: LiveSource, products: list[str], url: str, stop_at: float, fallback_after_s: float
) -> bool:
    """Stream until ``stop_at`` (monotonic). Returns False if the feed was silent for
    ``fallback_after_s`` seconds (the caller then switches to the capture)."""
    from websockets.asyncio.client import connect

    backoff = 1.0
    while time.monotonic() < stop_at:
        try:
            async with connect(url, max_size=8 * 1024 * 1024, open_timeout=10) as ws:
                for m in subscribe_messages(products):
                    await ws.send(m)
                print(f"[coinbase] connected to {url}, subscribed to {len(products)} products", flush=True)
                backoff = 1.0
                while time.monotonic() < stop_at:
                    silent = time.monotonic() - source.last_trade_monotonic
                    if silent > fallback_after_s:
                        return False
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=min(5.0, fallback_after_s))
                    except TimeoutError:
                        continue
                    for r in parse_message(raw):
                        source.on_exchange_trade(r)
        except Exception as exc:  # noqa: BLE001  any network error: back off and reconnect (LIVE-2)
            source.reconnects += 1
            if time.monotonic() - source.last_trade_monotonic > fallback_after_s:
                print(f"[coinbase] connection lost ({exc!r}); silent for {fallback_after_s:.0f}s", flush=True)
                return False
            print(f"[coinbase] connection lost ({exc!r}); reconnecting in {backoff:.0f}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
    return True


def replay_capture(
    source: LiveSource, path: Path, stop_at: float, speed: float = 1.0, loop: bool = True
) -> None:
    """LIVE-4: replay a capture at real pace, re-stamped so it starts now, looping until ``stop_at``."""
    source.mode = "replay"
    records = list(replay_trades(path))
    if not records:
        raise RuntimeError(f"capture {path} is empty: nothing to replay")
    print(f"[coinbase] replaying {len(records)} captured trades from {path}", flush=True)
    while time.monotonic() < stop_at:
        shift = int(time.time() * 1000) - records[0].event_time_ms
        t0_wall, t0_event = time.monotonic(), records[0].event_time_ms
        for r in records:
            target = t0_wall + (r.event_time_ms - t0_event) / 1000.0 / speed
            while time.monotonic() < target and time.monotonic() < stop_at:
                time.sleep(min(0.05, target - time.monotonic()))
            if time.monotonic() >= stop_at:
                return
            source.emit(Trade(source.next_id, r.symbol, r.event_time_ms + shift, r.price, r.size))
            source.next_id += 1
            source.replayed_trades += 1
        if not loop:
            return


def latest_capture(capture_dir: Path) -> Path | None:
    files = sorted(capture_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None
