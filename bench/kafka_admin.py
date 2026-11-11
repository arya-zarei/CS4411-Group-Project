"""Kafka helpers for the harness: recreate topics, find partition leaders, read end offsets.

Talks to the cluster from the host through the EXTERNAL listeners (localhost:29092-29094).
Topic settings mirror infra/kafka/create-topics.sh (PLAN.md 5.2); tests check they agree.
"""

from __future__ import annotations

import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:29092,localhost:29093,localhost:29094"
DAY_MS = 86_400_000

TOPIC_CONFIGS = {
    "trades": {"min.insync.replicas": "2", "retention.ms": str(DAY_MS)},
    "passthrough_out": {
        "min.insync.replicas": "2",
        "retention.ms": str(DAY_MS),
        "message.timestamp.type": "LogAppendTime",
    },
    "perf_test": {"min.insync.replicas": "1", "retention.ms": "3600000"},
}
REPLICATION = {"trades": 3, "passthrough_out": 3, "perf_test": 1}


def admin(bootstrap: str = BOOTSTRAP) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def recreate_topics(partitions: dict[str, int], bootstrap: str = BOOTSTRAP, timeout_s: float = 60) -> None:
    """Delete and recreate topics with the given partition counts. The caller must also wipe any Spark
    checkpoint that read these topics (risk R8), or Spark fails on offsets that no longer exist."""
    a = admin(bootstrap)
    names = list(partitions)
    existing = set(a.list_topics(timeout=10).topics)
    to_delete = [n for n in names if n in existing]
    if to_delete:
        for f in a.delete_topics(to_delete, operation_timeout=30).values():
            f.result()
        deadline = time.monotonic() + timeout_s
        while set(to_delete) & set(a.list_topics(timeout=10).topics):
            if time.monotonic() > deadline:
                raise TimeoutError(f"topics not deleted: {to_delete}")
            time.sleep(0.5)
    new = [NewTopic(n, partitions[n], REPLICATION[n], config=TOPIC_CONFIGS[n]) for n in names]
    deadline = time.monotonic() + timeout_s
    while True:  # creation right after deletion can briefly fail with TOPIC_ALREADY_EXISTS
        try:
            for f in a.create_topics(new, operation_timeout=30).values():
                f.result()
            break
        except Exception as exc:  # noqa: BLE001
            if time.monotonic() > deadline:
                raise
            if "already exists" not in str(exc).lower() and "marked for deletion" not in str(exc).lower():
                raise
            time.sleep(1)
    wait_for_leaders(names, bootstrap, timeout_s)


def wait_for_leaders(names: list[str], bootstrap: str = BOOTSTRAP, timeout_s: float = 60) -> None:
    a = admin(bootstrap)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        md = a.list_topics(timeout=10).topics
        if all(
            n in md and md[n].partitions and all(p.leader >= 0 for p in md[n].partitions.values())
            for n in names
        ):
            return
        time.sleep(0.5)
    raise TimeoutError(f"no leaders for {names}")


def partition_count(topic: str, bootstrap: str = BOOTSTRAP) -> int:
    return len(admin(bootstrap).list_topics(topic, timeout=10).topics[topic].partitions)


def leader_of(topic: str, partition: int = 0, bootstrap: str = BOOTSTRAP) -> int:
    """Broker id leading a partition (E2 kills it). Broker id n runs in container kafka-n."""
    return admin(bootstrap).list_topics(topic, timeout=10).topics[topic].partitions[partition].leader


def end_offsets(topic: str, bootstrap: str = BOOTSTRAP) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "bench-offsets", "enable.auto.commit": False})
    try:
        n = len(c.list_topics(topic, timeout=10).topics[topic].partitions)
        out = {}
        for p in range(n):
            _, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            out[p] = high
        return out
    finally:
        c.close()


def total_end_offset(topic: str, bootstrap: str = BOOTSTRAP) -> int:
    return sum(end_offsets(topic, bootstrap).values())
