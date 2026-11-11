"""Latency collectors and percentile tables (PLAN.md 5.9, T23).

* Pass-through: Kafka's LogAppendTime on `passthrough_out` minus the trade's creation time
  (`event_time_ms`, wall clock in realtime mode). Everything runs on one machine and one clock.
* Bars: time from when a bar can be finalized (window end + watermark) to when its row first
  appears in PostgreSQL (`written_at`, set on first insert only).

Latency is only meaningful for runs in realtime mode.
"""

from __future__ import annotations

import threading
import time

import numpy as np
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, TopicPartition

from bench.kafka_admin import BOOTSTRAP

PERCENTILES = (50, 95, 99)
_KEY = b'"event_time_ms":'


def summarize(values_ms) -> dict:
    """Median, p95, p99 (the proposal's statistics) plus count, mean, min and max, in milliseconds."""
    a = np.asarray(values_ms, dtype=float)
    if a.size == 0:
        return {"count": 0}
    out = {
        "count": int(a.size),
        "mean_ms": float(a.mean()),
        "min_ms": float(a.min()),
        "max_ms": float(a.max()),
    }
    for p, v in zip(PERCENTILES, np.percentile(a, PERCENTILES), strict=True):
        out[f"p{p}_ms"] = float(v)
    return out


def event_time_ms(value: bytes) -> int:
    """Fast path for the generator's JSON: read event_time_ms without a full JSON parse."""
    i = value.index(_KEY) + len(_KEY)
    j = i
    while value[j : j + 1].isdigit():
        j += 1
    return int(value[i:j])


class PassthroughCollector:
    """Consumes `passthrough_out` from its current end and records one latency per record."""

    def __init__(self, topic: str = "passthrough_out", bootstrap: str = BOOTSTRAP):
        self.topic = topic
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"latency-{time.time_ns()}",
                "enable.auto.commit": False,
                "fetch.wait.max.ms": 10,
            }
        )
        self.latencies: list[int] = []
        self.created: list[int] = []  # each record's creation time, to drop a warm-up period
        self.wrong_timestamp_type = 0
        self.records = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> PassthroughCollector:
        md = self.consumer.list_topics(self.topic, timeout=10).topics[self.topic]
        parts = []
        for p in md.partitions:
            _, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, p), timeout=10)
            parts.append(TopicPartition(self.topic, p, high))
        self.consumer.assign(parts)  # only records written after this point
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            for msg in self.consumer.consume(10_000, timeout=0.2):
                if msg.error():
                    continue
                ts_type, ts = msg.timestamp()
                if ts_type != TIMESTAMP_LOG_APPEND_TIME:
                    self.wrong_timestamp_type += 1
                    continue
                created = event_time_ms(msg.value())
                self.latencies.append(ts - created)
                self.created.append(created)
                self.records += 1

    def stop(self, drain_s: float = 3.0) -> dict:
        time.sleep(drain_s)
        self._stop.set()
        self._thread.join(timeout=10)
        self.consumer.close()
        out = summarize(self.latencies)
        out["wrong_timestamp_type"] = self.wrong_timestamp_type
        return out


BAR_LATENCY_SQL = """
SELECT extract(epoch FROM (written_at - (window_end + make_interval(secs => %s)))) * 1000
FROM bars WHERE window_end >= to_timestamp(%s / 1000.0)
"""


def bar_latencies(conn, watermark_ms: int = 10_000, since_ms: int = 0) -> np.ndarray:
    """Bar latency in ms for bars whose window ended at or after `since_ms` (skip warm-up)."""
    with conn.cursor() as cur:
        cur.execute(BAR_LATENCY_SQL, (watermark_ms / 1000.0, since_ms))
        return np.array([float(r[0]) for r in cur.fetchall()])
