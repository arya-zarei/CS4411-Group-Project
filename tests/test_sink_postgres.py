"""Integration tests for the PostgreSQL sink (T11/T12), run against the real database in a throwaway
schema. Skipped when PostgreSQL is not reachable (for example in CI without the stack).

Feeds the golden bars to ``sink.write_batch`` one closed minute at a time, the way Spark emits them,
and checks the alerts table, idempotency on replay, ``written_at`` stability, and insert-mode duplicates.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
import pytest

import sink
from verify import reference as ref
from verify import verify as ver

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("PG_DSN", "postgresql://pipeline:change-me-locally@localhost:5432/market")
INIT_SQL = (Path(__file__).resolve().parent.parent / "infra" / "postgres" / "init.sql").read_text()


@pytest.fixture()
def conn():
    try:
        c = psycopg.connect(DSN, connect_timeout=3)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    schema = f"t_{uuid.uuid4().hex[:10]}"
    with c.transaction():
        c.execute(f"CREATE SCHEMA {schema}")
        c.execute(f"SET search_path TO {schema}")
        c.execute(INIT_SQL)
    c.execute(f"SET search_path TO {schema}")
    c.commit()
    yield c
    c.rollback()
    c.execute(f"DROP SCHEMA {schema} CASCADE")
    c.commit()
    c.close()


def batches_by_minute(bars: pd.DataFrame):
    for _, g in bars.groupby("window_start_ms", sort=True):
        yield [
            {
                "symbol": r.symbol,
                "window_start": sink.ms_to_ts(r.window_start_ms),
                "window_end": sink.ms_to_ts(r.window_end_ms),
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": float(r.volume),
                "vwap": r.vwap,
                "trade_count": int(r.trade_count),
            }
            for r in g.itertuples(index=False)
        ]


@pytest.fixture(scope="module")
def golden_bars():
    return ref.compute_bars(ref.load_sample_trades())


def _verify(conn, mode):
    bars, alerts = ver.read_actual(conn, mode)
    exp_bars = ref.compute_bars(ref.load_sample_trades())
    return ver.compare_bars(exp_bars, bars), ver.compare_alerts(ref.compute_alerts(exp_bars), alerts)


def test_minute_by_minute_upsert_gives_golden_bars_and_alerts(conn, golden_bars):
    for batch in batches_by_minute(golden_bars):
        sink.write_batch(conn, batch, "upsert")
    b, a = _verify(conn, "upsert")
    assert b["ok"] and b["matched"] == 4000, b
    assert a["ok"] and a["matched"] == 23, a


def test_whole_dataset_in_one_batch(conn, golden_bars):
    sink.write_batch(conn, [b for batch in batches_by_minute(golden_bars) for b in batch], "upsert")
    b, a = _verify(conn, "upsert")
    assert b["ok"] and a["ok"]


def test_replay_is_idempotent_and_keeps_written_at(conn, golden_bars):
    batches = list(batches_by_minute(golden_bars))
    for batch in batches:
        sink.write_batch(conn, batch, "upsert")
    before = conn.execute("SELECT symbol, window_start, written_at FROM bars ORDER BY 1, 2").fetchall()
    alerts_before = conn.execute("SELECT * FROM alerts ORDER BY 1, 2").fetchall()
    for batch in batches[-5:]:  # replay the last batches, as after a crash
        sink.write_batch(conn, batch, "upsert")
    assert (
        conn.execute("SELECT symbol, window_start, written_at FROM bars ORDER BY 1, 2").fetchall() == before
    )
    assert conn.execute("SELECT * FROM alerts ORDER BY 1, 2").fetchall() == alerts_before


def test_insert_mode_replay_creates_visible_duplicates(conn, golden_bars):
    batches = list(batches_by_minute(golden_bars))
    for batch in batches:
        sink.write_batch(conn, batch, "insert")
    b, a = _verify(conn, "insert")
    assert b["ok"] and a["ok"]  # no replay yet: correct
    sink.write_batch(conn, batches[-1], "insert")  # replay one batch
    b, _ = _verify(conn, "insert")
    assert not b["ok"] and b["duplicate_rows"] == len(batches[-1])


def test_bad_sink_mode_is_rejected(conn):
    with pytest.raises(ValueError):
        sink.write_batch(conn, [], "merge")
