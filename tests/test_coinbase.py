"""T27: the Coinbase adapter against a local fake server that speaks the documented protocol, plus
the capture/replay fallback. The real feed cannot be reached from CI (spike S5 checks it live)."""

from __future__ import annotations

import asyncio
import json
import socket
import time

import pytest

from tradegen import cli
from tradegen import coinbase as cb


def doc_message(kind: str, trades: list[dict]) -> str:
    """A message in the shape Coinbase documents for the market_trades channel."""
    return json.dumps(
        {
            "channel": "market_trades",
            "client_id": "",
            "timestamp": "2026-11-11T20:42:27.265Z",
            "sequence_num": 0,
            "events": [{"type": kind, "trades": trades}],
        }
    )


def trade(tid, product="BTC-USD", price="65000.12", size="0.0015", t="2026-11-11T20:42:27.265123456Z"):
    return {"trade_id": tid, "product_id": product, "price": price, "size": size, "side": "BUY", "time": t}


def test_iso_times_with_nanoseconds_and_z():
    assert cb.iso_to_ms("2026-11-11T20:42:27.265123456Z") == cb.iso_to_ms("2026-11-11T20:42:27.265Z")
    assert cb.iso_to_ms("2026-11-11T20:42:27Z") % 1000 == 0


def test_parse_message_maps_documented_fields():
    msg = doc_message(
        "snapshot",
        [
            trade("2", t="2026-11-11T20:42:28.000Z"),
            trade("1", price=65000.5, size=0.25, t="2026-11-11T20:42:27.000Z"),
        ],
    )
    rows = cb.parse_message(msg)
    assert [r["source_trade_id"] for r in rows] == ["1", "2"]  # sorted oldest first
    assert rows[0]["price"] == 65000.5 and rows[0]["size"] == 0.25  # numbers accepted
    assert rows[1]["price"] == 65000.12 and rows[1]["symbol"] == "BTC-USD"  # strings accepted
    assert cb.parse_message(json.dumps({"channel": "heartbeats", "events": []})) == []


def test_subscribe_messages():
    subs = [json.loads(m) for m in cb.subscribe_messages(["BTC-USD", "ETH-USD"])]
    assert subs[0] == {"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD"], "channel": "market_trades"}
    assert subs[1]["channel"] == "heartbeats"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_live_stream_reconnects_and_drops_repeated_snapshot_trades(tmp_path):
    from websockets.asyncio.server import serve

    connections = []

    async def handler(ws):
        sub = json.loads(await ws.recv())
        assert sub["channel"] == "market_trades"
        connections.append(sub["product_ids"])
        await ws.send(
            doc_message(
                "snapshot",
                [trade("11", t="2026-11-11T20:00:00.100Z"), trade("10", t="2026-11-11T20:00:00.000Z")],
            )
        )
        await ws.send(doc_message("update", [trade("12", t="2026-11-11T20:00:01.000Z")]))
        if len(connections) == 1:
            return  # drop the first connection: the client must reconnect (LIVE-2)
        await ws.send(doc_message("update", [trade("13", t="2026-11-11T20:00:02.000Z")]))
        await asyncio.sleep(10)

    emitted = []
    capture = tmp_path / "captures" / "cb.jsonl"

    async def scenario():
        port = free_port()
        async with serve(handler, "127.0.0.1", port):
            src = cb.LiveSource(emitted.append, capture)
            await cb.run_live(
                src, ["BTC-USD"], f"ws://127.0.0.1:{port}", time.monotonic() + 4, fallback_after_s=10
            )
            src.close()
            return src

    src = asyncio.run(scenario())
    assert len(connections) == 2 and src.reconnects == 1  # the dropped connection was re-established
    assert [t.trade_id for t in emitted] == [1, 2, 3, 4]  # the adapter's own counter
    assert [t.event_time_ms % 10_000 for t in emitted] == [0, 100, 1000, 2000]  # 10, 11, 12, then 13
    lines = [json.loads(x) for x in capture.read_text().splitlines()]
    assert [x["source_trade_id"] for x in lines] == ["10", "11", "12", "13"]  # LIVE-3


def test_silent_feed_falls_back_to_the_capture(tmp_path):
    capture = tmp_path / "cb.jsonl"
    capture.write_text(
        "\n".join(
            json.dumps(
                {
                    "trade_id": i,
                    "symbol": "ETH-USD",
                    "event_time_ms": 1_000_000 + 50 * i,
                    "price": 3000.0,
                    "size": 0.1,
                }
            )
            for i in range(1, 6)
        )
        + "\n"
    )
    emitted = []
    src = cb.LiveSource(emitted.append, None)
    url = f"ws://127.0.0.1:{free_port()}"  # nothing listens there
    ok = asyncio.run(cb.run_live(src, ["ETH-USD"], url, time.monotonic() + 30, fallback_after_s=1.0))
    assert ok is False and src.reconnects >= 1
    before = int(time.time() * 1000)
    cb.replay_capture(src, capture, time.monotonic() + 5, loop=False)
    assert len(emitted) == 5 and src.replayed_trades == 5
    assert emitted[0].event_time_ms >= before - 1000  # re-stamped to now (LIVE-4)
    assert emitted[4].event_time_ms - emitted[0].event_time_ms == 200  # spacing kept


def test_cli_offline_replays_capture(tmp_path):
    capture = tmp_path / "cb.jsonl"
    capture.write_text(
        "".join(
            json.dumps(
                {
                    "trade_id": i,
                    "symbol": "BTC-USD",
                    "event_time_ms": 5_000 + 10 * i,
                    "price": 1.5,
                    "size": 0.2,
                }
            )
            + "\n"
            for i in range(1, 21)
        )
    )
    out = tmp_path / "out.csv"
    code = cli.main(
        [
            "--mode",
            "live-coinbase",
            "--offline",
            "--replay-file",
            str(capture),
            "--duration",
            "1.5",
            "--dump-csv",
            str(out),
        ]
    )
    rows = out.read_text().splitlines()
    assert code == 0 and rows[0].startswith("trade_id") and len(rows) > 20  # looped at least once
    assert rows[1].split(",")[1] == "BTC-USD"


def test_cli_rejects_nothing_extra_for_live_mode():
    a = cli.parse_args(["--mode", "live-coinbase"])
    assert a.duration is None and a.fallback_after == 30.0
    assert cli.parse_args(["--mode", "realtime"]).duration == 60.0


@pytest.mark.parametrize("text", ["", "\n"])
def test_empty_capture_is_an_error(tmp_path, text):
    p = tmp_path / "empty.jsonl"
    p.write_text(text)
    with pytest.raises(RuntimeError):
        cb.replay_capture(cb.LiveSource(lambda t: None, None), p, time.monotonic() + 1)
