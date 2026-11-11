"""VER-1: did Kafka keep every acknowledged trade exactly once? (PLAN.md 5.8, Experiment 2)

Compares the producer's per-trade ack log (tradegen --log-trades: trade_id,acked,partition,offset,error)
with what is actually in the topic, read from the beginning:

* lost:       acknowledged by Kafka but not in the topic
* duplicated: in the topic more than once (for example a producer retry without idempotence)
* unacked_but_present: the producer saw a failure, yet the record was written (informational;
  possible when an ack times out after the write succeeded)
* write_stall: the longest gap between delivery reports (Experiment 2's "time writes are stalled")

  python -m verify.kafka_layer --acks runs/<run>/acks.csv [--topic trades] [--report out.json]

``read_topic`` is also used by the bar verifier to compute expected bars from the topic's actual
contents (VER-2 b), which separates Kafka-layer loss from Spark or sink errors.
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, TopicPartition

DEFAULT_BOOTSTRAP = "localhost:29092,localhost:29093,localhost:29094"
MAX_EXAMPLES = 10


def read_topic(
    topic: str = "trades", bootstrap: str = DEFAULT_BOOTSTRAP, timeout_s: float = 300
) -> pd.DataFrame:
    """Every record currently in the topic (up to the end offsets at call time), parsed.
    Columns: trade_id, symbol, event_time_ms, price, size, partition, offset."""
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{time.time_ns()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        ends, assign = {}, []
        for p in parts:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            ends[p] = high
            if high > low:
                assign.append(TopicPartition(topic, p, low))
        c.assign(assign)
        rows = []
        remaining = {tp.partition for tp in assign}
        deadline = time.monotonic() + timeout_s
        while remaining and time.monotonic() < deadline:
            for msg in c.consume(10_000, timeout=1.0):
                if msg.error():
                    continue
                p = msg.partition()
                if msg.offset() >= ends[p]:
                    remaining.discard(p)
                    continue
                d = json.loads(msg.value())
                rows.append(
                    (d["trade_id"], d["symbol"], d["event_time_ms"], d["price"], d["size"], p, msg.offset())
                )
                if msg.offset() == ends[p] - 1:
                    remaining.discard(p)
        if remaining:
            raise TimeoutError(f"could not read partitions {sorted(remaining)} of {topic} to the end")
    finally:
        c.close()
    cols = ["trade_id", "symbol", "event_time_ms", "price", "size", "partition", "offset"]
    return pd.DataFrame(rows, columns=cols)


def write_stall(acks: pd.DataFrame) -> dict | None:
    """Experiment 2's "time writes are stalled", from the ``ack_ms`` column (tradegen ack logs from
    2026-11-11 on). Reported per partition, because a broker crash stalls only some partitions with
    acks=1 (those the dead broker led) but every partition with acks=all (the dead broker is in every
    in-sync replica set until Kafka removes it); a single gap across all partitions would hide the
    acks=1 stall behind the partitions that kept going.

    max_partition_gap_ms: the longest gap between consecutive acks within any one partition (headline).
    max_gap_ms:           the longest gap between any two consecutive delivery reports."""
    if "ack_ms" not in acks or acks["ack_ms"].isna().all() or len(acks) < 2:
        return None
    t = acks["ack_ms"].dropna().astype("int64").sort_values().to_numpy()
    gaps = t[1:] - t[:-1]
    i = int(gaps.argmax())
    out = {"max_gap_ms": int(gaps[i]), "gap_start_ms": int(t[i]), "gap_end_ms": int(t[i + 1])}
    acked = acks[(acks["acked"] == 1) & acks["partition"].notna()]
    per_partition, worst = {}, None
    for part, g in acked.groupby("partition"):
        pt = g["ack_ms"].astype("int64").sort_values().to_numpy()
        if len(pt) < 2:
            continue
        pg = pt[1:] - pt[:-1]
        j = int(pg.argmax())
        per_partition[int(part)] = int(pg[j])
        if worst is None or pg[j] > worst[1]:
            worst = (int(part), int(pg[j]), int(pt[j]), int(pt[j + 1]))
    if worst:
        out.update(
            max_partition_gap_ms=worst[1],
            worst_partition=worst[0],
            partition_gap_start_ms=worst[2],
            partition_gap_end_ms=worst[3],
            per_partition_max_gap_ms=per_partition,
        )
    return out


def compare(acks: pd.DataFrame, topic_ids) -> dict:
    """Pure comparison of an ack log with the trade ids found in the topic (unit-tested)."""
    counts = collections.Counter(int(t) for t in topic_ids)
    acked = set(acks.loc[acks["acked"] == 1, "trade_id"].astype(int))
    failed = set(acks.loc[acks["acked"] != 1, "trade_id"].astype(int))
    present = set(counts)
    lost = sorted(acked - present)
    duplicated = {t: n for t, n in counts.items() if n > 1}
    unacked_present = sorted(failed & present)
    return {
        "sent": int(len(acks)),
        "acked": len(acked),
        "failed": len(failed),
        "records_in_topic": int(sum(counts.values())),
        "distinct_in_topic": len(present),
        "lost": len(lost),
        "duplicated_ids": len(duplicated),
        "duplicate_records": int(sum(n - 1 for n in duplicated.values())),
        "unacked_but_present": len(unacked_present),
        "not_in_ack_log": len(present - acked - failed),
        "write_stall": write_stall(acks),
        "examples": {
            "lost": lost[:MAX_EXAMPLES],
            "duplicated": sorted(duplicated)[:MAX_EXAMPLES],
            "unacked_but_present": unacked_present[:MAX_EXAMPLES],
        },
        "ok": not lost and not duplicated,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--acks", type=Path, required=True, help="ack log written by tradegen --log-trades")
    ap.add_argument("--topic", default="trades")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--report", type=Path)
    a = ap.parse_args(argv)
    acks = pd.read_csv(a.acks)
    topic = read_topic(a.topic, a.bootstrap)
    report = compare(acks, topic["trade_id"])
    report.update(topic=a.topic, ack_log=str(a.acks), status="PASS" if report["ok"] else "FAIL")
    if a.report:
        a.report.parent.mkdir(parents=True, exist_ok=True)
        a.report.write_text(json.dumps(report, indent=2) + "\n")
    print(
        f"{report['status']}: {report['acked']} acked, {report['records_in_topic']} records in {a.topic}; "
        f"lost {report['lost']}, duplicated {report['duplicated_ids']} ids "
        f"({report['duplicate_records']} extra records), failed {report['failed']} "
        f"(of which present {report['unacked_but_present']})"
        + (
            f"; longest ack gap in one partition {report['write_stall'].get('max_partition_gap_ms')} ms"
            if report["write_stall"]
            else ""
        )
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
