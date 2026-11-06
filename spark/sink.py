"""PostgreSQL sink for the bars job: one transaction per micro-batch (PLAN.md 5.5).

1. Write the batch's bars: upsert on (symbol, window_start) in ``upsert`` mode, plain insert into
   ``bars_plain`` in ``insert`` mode (Experiment 4 baseline, SPK-11).
2. Evaluate the alert rule for those bars against each symbol's previous bars, read back from the
   table that already contains the batch, and write alerts the same way.
3. Commit. ``written_at`` is set by the database on first insert only; the update path never touches
   it, so bar latency stays valid after a replay.

Plain Python (3.10) and psycopg; no Spark imports, so it can be tested on its own.
"""

from datetime import datetime, timezone

from alert_rule import BASELINE_BARS, evaluate

BAR_COLS = (
    "symbol",
    "window_start",
    "window_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)
ALERT_COLS = (
    "symbol",
    "window_start",
    "window_end",
    "close",
    "log_return",
    "baseline_mean",
    "baseline_std",
    "z_score",
)

TABLES = {
    "upsert": {"bars": "bars", "alerts": "alerts", "bar_history": "bars"},
    # insert mode reads history de-duplicated, so duplicates in bars_plain do not change the alert maths
    "insert": {
        "bars": "bars_plain",
        "alerts": "alerts_plain",
        "bar_history": "(SELECT DISTINCT ON (symbol, window_start) symbol, window_start, window_end, close "
        "FROM bars_plain ORDER BY symbol, window_start, id)",
    },
}


def ms_to_ts(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _insert_sql(table, cols, mode):
    names = ", ".join(cols)
    params = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({names}) VALUES ({params})"
    if mode == "upsert":
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("symbol", "window_start"))
        sql += f" ON CONFLICT (symbol, window_start) DO UPDATE SET {updates}"
    return sql


# For each symbol in the batch: the BASELINE_BARS + 1 bars before the batch's first window, plus every
# bar from the first to the last window of the batch. Enough history for every batch bar's baseline.
HISTORY_SQL = """
WITH req AS (
  SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::timestamptz[]) AS r(symbol, min_ws, max_ws)
)
SELECT req.symbol, h.window_start, h.window_end, h.close
FROM req CROSS JOIN LATERAL (
  (SELECT window_start, window_end, close FROM {src} b
     WHERE b.symbol = req.symbol AND b.window_start < req.min_ws
     ORDER BY b.window_start DESC LIMIT {before})
  UNION ALL
  (SELECT window_start, window_end, close FROM {src} b
     WHERE b.symbol = req.symbol AND b.window_start BETWEEN req.min_ws AND req.max_ws)
) h
ORDER BY req.symbol, h.window_start
"""


def alerts_for_batch(cur, bars, mode):
    """Alert rows for the given batch bars (dicts with BAR_COLS), using history from the database."""
    if not bars:
        return []
    by_symbol = {}
    for b in bars:
        by_symbol.setdefault(b["symbol"], set()).add(b["window_start"])
    symbols = sorted(by_symbol)
    cur.execute(
        HISTORY_SQL.format(src=TABLES[mode]["bar_history"], before=BASELINE_BARS + 1),
        (symbols, [min(by_symbol[s]) for s in symbols], [max(by_symbol[s]) for s in symbols]),
    )
    history = {}
    for symbol, ws, we, close in cur.fetchall():
        history.setdefault(symbol, []).append((ws, we, close))
    out = []
    for symbol, rows in history.items():
        wanted = by_symbol[symbol]
        idx = [i for i, r in enumerate(rows) if r[0] in wanted]
        for i, log_return, mean, std, z in evaluate([r[2] for r in rows], idx):
            ws, we, close = rows[i]
            out.append(
                dict(
                    symbol=symbol,
                    window_start=ws,
                    window_end=we,
                    close=close,
                    log_return=log_return,
                    baseline_mean=mean,
                    baseline_std=std,
                    z_score=z,
                )
            )
    return out


def write_batch(conn, bars, mode="upsert"):
    """Write one micro-batch. ``bars``: dicts with BAR_COLS (timestamps as aware datetimes).
    Returns (bars_written, alerts_written). Everything happens in one transaction."""
    if mode not in TABLES:
        raise ValueError(f"SINK_MODE must be upsert or insert, not {mode!r}")
    t = TABLES[mode]
    with conn.transaction(), conn.cursor() as cur:
        if bars:
            cur.executemany(
                _insert_sql(t["bars"], BAR_COLS, mode), [tuple(b[c] for c in BAR_COLS) for b in bars]
            )
        alerts = alerts_for_batch(cur, bars, mode)
        if alerts:
            cur.executemany(
                _insert_sql(t["alerts"], ALERT_COLS, mode), [tuple(a[c] for c in ALERT_COLS) for a in alerts]
            )
    return len(bars), len(alerts)
