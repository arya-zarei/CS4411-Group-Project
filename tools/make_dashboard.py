"""Generates infra/grafana/dashboards/pipeline.json (T20, PLAN.md 5.7). Edit this file, not the JSON.

  uv run python tools/make_dashboard.py

Time-series panels use the wall-clock arrival time of each row (bars.written_at, query_progress.ts),
so the live demo scrolls in real time even though virtual-mode bars carry January 2026 timestamps.
Tables and the alerts-per-minute chart show the bars' own (virtual) minutes.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "infra" / "grafana" / "dashboards" / "pipeline.json"
DS = {"type": "grafana-postgresql-datasource", "uid": "pg"}


def target(sql: str, ref: str = "A", fmt: str = "table") -> dict:
    return {
        "refId": ref,
        "datasource": DS,
        "editorMode": "code",
        "rawQuery": True,
        "format": fmt,
        "rawSql": sql,
    }


_ids = iter(range(1, 100))


def panel(kind: str, title: str, x: int, y: int, w: int, h: int, targets: list, **extra) -> dict:
    p = {
        "id": next(_ids),
        "type": kind,
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
    }
    p.update(extra)
    return p


def stat(title, sql, x, y, w=4, unit="none", decimals=0, description="", text=False):
    fields = "/.*/" if text else ""  # a stat panel shows only numeric fields unless told otherwise
    return panel(
        "stat", title, x, y, w, 4, [target(sql)], description=description,
        fieldConfig={"defaults": {"unit": unit, "decimals": decimals, "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": [{"color": "blue", "value": None}]}},
                     "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": fields, "values": False},
                 "colorMode": "value", "graphMode": "none", "textMode": "value_and_name", "justifyMode": "auto"},
    )  # fmt: skip


def timeseries(title, sql, x, y, w, h, unit="none", description=""):
    return panel(
        "timeseries", title, x, y, w, h, [target(sql)], description=description,
        fieldConfig={"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never"}},
                     "overrides": []},
        options={"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                 "tooltip": {"mode": "multi", "sort": "none"}},
    )  # fmt: skip


def table(title, sql, x, y, w, h, description="", decimals_by_name=None, widths=None):
    overrides = [
        {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "decimals", "value": d}]}
        for name, d in (decimals_by_name or {}).items()
    ]
    overrides += [
        {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "custom.width", "value": px}]}
        for name, px in (widths or {}).items()
    ]
    return panel(
        "table", title, x, y, w, h, [target(sql)], description=description,
        fieldConfig={"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": overrides},
        options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False, "reducer": ["sum"], "fields": ""}},
    )  # fmt: skip


panels = [
    # --- row 1: headline numbers (DSH-5 freshness) ---
    stat("Bars written", 'SELECT count(*) AS "bars" FROM bars', 0, 0),
    stat("Alerts", 'SELECT count(*) AS "alerts" FROM alerts', 4, 0),
    stat("Symbols", 'SELECT count(DISTINCT symbol) AS "symbols" FROM bars', 8, 0),
    stat(
        "Latest bar (event time)",
        "SELECT to_char(max(window_start) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS \"window (UTC)\" FROM bars",
        12,
        0,
        w=4,
        text=True,
        description="Start of the newest 1-minute window written. Virtual mode runs ahead of the wall clock.",
    ),  # fmt: skip
    stat(
        "Seconds since last bar",
        'SELECT extract(epoch FROM now() - max(written_at)) AS "seconds" FROM bars',
        16,
        0,
        w=4,
        unit="s",
        decimals=1,
        description="Wall-clock time since the newest bar row was written (DSH-5).",
    ),  # fmt: skip
    stat(
        "Kafka lag (records)",
        'SELECT lag_total AS "lag" FROM query_progress ORDER BY id DESC LIMIT 1',
        20,
        0,
        w=4,
        description="Newest offset in `trades` minus the offset Spark's latest batch processed (SPK-8).",
    ),  # fmt: skip
    # --- row 2: DSH-1 candlestick + VWAP for $symbol ---
    panel(
        "candlestick",
        "$symbol: 1-minute bars and VWAP",
        0,
        4,
        16,
        11,
        [
            target(
                "SELECT written_at AS time, open, high, low, close, vwap, volume FROM bars "
                "WHERE symbol = '$symbol' AND $__timeFilter(written_at) ORDER BY written_at",
                fmt="table",
            )
        ],
        description="x axis: when each bar was written (wall clock). VWAP is drawn as a line. "
        "Fixed to the last 5 minutes: at 30x a wider range packs the candles too tightly to see.",
        timeFrom="5m",
        fieldConfig={
            "defaults": {"custom": {"lineWidth": 2, "fillOpacity": 0}},
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "vwap"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "orange"}}],
                }
            ],
        },
        options={
            "mode": "candles+volume",
            "candleStyle": "candles",
            "colorStrategy": "open-close",
            "fields": {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"},
            "colors": {"up": "green", "down": "red"},
            "includeAllFields": True,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
        },
    ),  # fmt: skip
    # --- DSH-3 alerts table ---
    table(
        "Alerts (newest first)",
        "SELECT to_char(window_start AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS \"window (UTC)\", symbol, "
        'round(z_score::numeric, 1) AS z, round((log_return * 100)::numeric, 2) AS "return %" '
        "FROM alerts ORDER BY window_start DESC, symbol LIMIT 100",
        16,
        4,
        8,
        11,
        description="Bars whose log return is more than 3 standard deviations from the previous 30 (DSH-3).",
        widths={"window (UTC)": 110, "symbol": 80, "z": 70},
    ),  # fmt: skip
    # --- row 3: DSH-4 alerts per minute, DSH-5 throughput ---
    panel(
        "barchart",
        "Alerts per minute (last 60 event-time minutes)",
        0,
        15,
        8,
        8,
        [
            target(
                "SELECT to_char(m AT TIME ZONE 'UTC', 'HH24:MI') AS minute, coalesce(a.n, 0) AS alerts "
                "FROM generate_series((SELECT max(window_start) FROM bars) - interval '59 minutes', "
                "(SELECT max(window_start) FROM bars), interval '1 minute') AS m "
                "LEFT JOIN (SELECT window_start, count(*) AS n FROM alerts GROUP BY 1) a ON a.window_start = m ORDER BY m"
            )
        ],
        options={
            "orientation": "vertical",
            "xField": "minute",
            "showValue": "never",
            "barWidth": 0.9,
            "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
            "xTickLabelSpacing": 100,
            "tooltip": {"mode": "single"},
        },
        fieldConfig={"defaults": {"color": {"mode": "fixed", "fixedColor": "red"}}, "overrides": []},
    ),  # fmt: skip
    timeseries(
        "Input and processed rows per second",
        'SELECT ts AS time, input_rows_per_s AS "input rows/s", processed_rows_per_s AS "processed rows/s" '
        "FROM query_progress WHERE $__timeFilter(ts) ORDER BY ts",
        8,
        15,
        8,
        8,
        unit="short",
    ),  # fmt: skip
    timeseries(
        "Batch duration and Kafka lag",
        'SELECT ts AS time, batch_duration_ms AS "batch duration (ms)", lag_total AS "lag (records)" '
        "FROM query_progress WHERE $__timeFilter(ts) ORDER BY ts",
        16,
        15,
        8,
        8,
        unit="short",
    ),  # fmt: skip
    # --- row 4: DSH-2 latest bars ---
    table(
        "Latest bar per symbol",
        "SELECT DISTINCT ON (symbol) symbol, to_char(window_start AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS \"window (UTC)\", "
        "round(open::numeric, 4) AS open, round(high::numeric, 4) AS high, round(low::numeric, 4) AS low, "
        "round(close::numeric, 4) AS close, round(vwap::numeric, 4) AS vwap, volume, trade_count AS trades "
        "FROM bars ORDER BY symbol, window_start DESC",
        0,
        23,
        24,
        10,
        description="The newest 1-minute bar of every symbol (DSH-2).",
        decimals_by_name={c: 4 for c in ("open", "high", "low", "close", "vwap")},
    ),  # fmt: skip
]

dashboard = {
    "uid": "pipeline",
    "title": "Market-data pipeline",
    "description": "Bars, alerts and pipeline health from PostgreSQL (PLAN.md 5.7). Generated by tools/make_dashboard.py.",
    "tags": ["kafka", "spark", "market-data"],
    "timezone": "browser",
    "editable": False,
    "graphTooltip": 1,
    "refresh": "5s",  # DSH-6
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["2s", "5s", "10s", "30s", "1m"]},
    "schemaVersion": 41,
    "version": 1,
    "templating": {
        "list": [
            {
                "name": "symbol",
                "label": "Symbol",
                "type": "query",
                "datasource": DS,
                "query": "SELECT DISTINCT symbol FROM bars ORDER BY 1",
                "definition": "SELECT DISTINCT symbol FROM bars ORDER BY 1",
                "refresh": 2,
                "sort": 1,
                "includeAll": False,
                "multi": False,
                "current": {},
                "options": [],
            }
        ]
    },
    "annotations": {"list": []},
    "panels": panels,
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
    print(f"wrote {OUT} ({len(panels)} panels)")
