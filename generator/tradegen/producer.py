"""Kafka producer with delivery tracking.

* Every send registers a delivery callback. Counts of acked and failed sends are always kept.
* With an ack log (GEN-2), each trade's outcome is appended to ``acks.csv`` in batches:
  ``trade_id,acked,partition,offset,error,ack_ms``. That file is the ground truth for loss detection
  (VER-1); ``ack_ms`` (wall clock when the delivery report arrived) gives the write stall in
  Experiment 2. Delivery reports are served every ``POLL_EVERY`` sends, so ack_ms is accurate to
  about POLL_EVERY / rate seconds.
* When the local queue is full (GEN-3) the producer waits for deliveries instead of dropping trades,
  and counts how often it had to wait.
"""

from __future__ import annotations

import time
from functools import partial
from pathlib import Path

from confluent_kafka import KafkaError, Producer

from .model import Trade, kafka_value

ACK_LOG_FLUSH_EVERY = 20_000
POLL_EVERY = 50


class TradeProducer:
    def __init__(self, bootstrap: str, topic: str, conf: dict, ack_log: Path | None = None):
        self.topic = topic
        self.conf = {"bootstrap.servers": bootstrap, **conf}
        self.producer = Producer(self.conf)
        self.attempted = self.acked = self.failed = 0
        self.backpressure_waits = 0
        self.first_error: str | None = None
        self._ack_file = open(ack_log, "w", buffering=1 << 20) if ack_log else None  # noqa: SIM115
        if self._ack_file:
            self._ack_file.write("trade_id,acked,partition,offset,error,ack_ms\n")
        self._ack_rows: list[str] = []

    # delivery callbacks run inside poll()/flush() on this thread
    def _count(self, err: KafkaError | None, _msg) -> None:
        if err is None:
            self.acked += 1
        else:
            self.failed += 1
            self.first_error = self.first_error or str(err)

    def _log(self, trade_id: int, err: KafkaError | None, msg) -> None:
        self._count(err, msg)
        now_ms = int(time.time() * 1000)
        if err is None:
            self._ack_rows.append(f"{trade_id},1,{msg.partition()},{msg.offset()},,{now_ms}\n")
        else:
            self._ack_rows.append(f"{trade_id},0,,,{err.name()},{now_ms}\n")
        if len(self._ack_rows) >= ACK_LOG_FLUSH_EVERY:
            self._flush_ack_rows()

    def _flush_ack_rows(self) -> None:
        if self._ack_file and self._ack_rows:
            self._ack_file.writelines(self._ack_rows)
            self._ack_rows.clear()

    def send(self, t: Trade) -> None:
        cb = partial(self._log, t.trade_id) if self._ack_file else self._count
        key = t.symbol.encode()
        value = kafka_value(t)
        while True:
            try:
                self.producer.produce(self.topic, value=value, key=key, on_delivery=cb)
                break
            except BufferError:  # local queue full: wait for deliveries, never drop (GEN-3)
                self.backpressure_waits += 1
                self.producer.poll(0.05)
        self.attempted += 1
        if self.attempted % POLL_EVERY == 0:
            self.producer.poll(0)

    def close(self, timeout_s: float = 120.0) -> int:
        """Wait for outstanding deliveries. Returns the number still undelivered (0 is good)."""
        remaining = self.producer.flush(timeout_s)
        self._flush_ack_rows()
        if self._ack_file:
            self._ack_file.close()
        return remaining


def wait_until(target_wall_s: float) -> None:
    """Sleep until a wall-clock time, in short steps so the caller can keep polling the producer."""
    delay = target_wall_s - time.monotonic()
    if delay > 0.001:
        time.sleep(min(delay, 0.05))
