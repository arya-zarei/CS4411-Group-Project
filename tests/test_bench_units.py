"""Unit tests for M2 code that needs no Docker: Kafka-layer verifier, throughput rule (D5), harness
config, perf-test parsing, latency helpers, dashboard generation."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bench import kafka_admin
from bench.latency import event_time_ms, summarize
from bench.runner import EXPERIMENTS, PERF_TEST_LINE, load_config
from bench.throughput import keeps_up, search
from verify.kafka_layer import compare, write_stall

ROOT = Path(__file__).resolve().parent.parent


# --- VER-1 ---------------------------------------------------------------------------------------
def acks_frame(rows):
    return pd.DataFrame(rows, columns=["trade_id", "acked", "partition", "offset", "error", "ack_ms"])


def test_ver1_clean_lost_duplicated_and_unacked():
    acks = acks_frame(
        [(1, 1, 0, 0, None, 1000), (2, 1, 0, 1, None, 1001), (3, 0, None, None, "TIMED_OUT", 1002)]
    )
    assert compare(acks, [1, 2])["ok"]
    r = compare(acks, [1])  # trade 2 acked but missing
    assert not r["ok"] and r["lost"] == 1 and r["examples"]["lost"] == [2]
    r = compare(acks, [1, 2, 2])
    assert not r["ok"] and r["duplicated_ids"] == 1 and r["duplicate_records"] == 1
    r = compare(acks, [1, 2, 3])  # a failed send that was written anyway is reported, not an error
    assert r["ok"] and r["unacked_but_present"] == 1


def test_write_stall_is_reported_per_partition():
    # partition 0 stalls for 5 s; partition 1 keeps acking every 100 ms
    rows = [(i, 1, 1, i, None, 1000 + 100 * i) for i in range(100)]
    rows += [(1000, 1, 0, 0, None, 1000), (1001, 1, 0, 1, None, 6000)]
    w = write_stall(acks_frame(rows))
    assert w["max_partition_gap_ms"] == 5000 and w["worst_partition"] == 0
    assert w["max_gap_ms"] < 5000  # the stall is invisible across all partitions
    assert w["per_partition_max_gap_ms"] == {0: 5000, 1: 100}


def test_write_stall_needs_ack_times():
    old = pd.DataFrame({"trade_id": [1, 2], "acked": [1, 1], "partition": [0, 0], "offset": [0, 1]})
    assert write_stall(old) is None


# --- D5 sustained throughput ---------------------------------------------------------------------
def test_keeps_up_with_bounded_lag():
    samples = [(t, 500 + (100 if t % 2 else 0)) for t in range(60)]
    assert keeps_up(samples, rate=1000)["keeps_up"]


def test_growing_lag_does_not_keep_up():
    samples = [(t, 200.0 * t) for t in range(60)]  # 200 records/s growth > 1% of 1,000/s
    r = keeps_up(samples, rate=1000)
    assert not r["keeps_up"] and r["slope_records_per_s"] > r["slope_limit"]


def test_lag_ending_above_first_window_fails():
    samples = [(t, 100.0 if t < 50 else 5000.0) for t in range(60)]
    assert not keeps_up(samples, rate=1_000_000)["keeps_up"]  # slope limit huge; the end lag decides


def test_too_few_samples():
    assert not keeps_up([(0, 0)], rate=1000)["keeps_up"]


def test_search_steps_up_then_bisects_once():
    capacity = 7000
    r = search([1000, 5000, 10000, 20000], lambda rate: {"keeps_up": rate <= capacity})
    assert [s["rate"] for s in r["steps"]] == [1000, 5000, 10000, 7500]
    assert r["sustained_rate"] == 5000 and r["limited_by"] == "spark"


def test_search_stops_when_the_generator_is_the_limit():
    r = search([1000, 5000], lambda rate: {"keeps_up": True, "generator_limited": rate > 1000})
    assert r["sustained_rate"] == 1000 and r["limited_by"] == "generator"


def test_search_reaching_ladder_top():
    r = search([1000, 2000], lambda rate: {"keeps_up": True})
    assert r["sustained_rate"] == 2000 and r["limited_by"] == "ladder_top"


# --- latency helpers -----------------------------------------------------------------------------
def test_event_time_fast_path_matches_json():
    v = b'{"trade_id":1,"symbol":"SYM078","event_time_ms":1767623400023,"price":81.3873,"size":25}'
    assert event_time_ms(v) == json.loads(v)["event_time_ms"]


def test_summarize_percentiles():
    s = summarize(list(range(1, 101)))
    assert s["count"] == 100 and s["p50_ms"] == pytest.approx(50.5) and s["p99_ms"] == pytest.approx(99.01)
    assert summarize([]) == {"count": 0}


# --- harness configuration -----------------------------------------------------------------------
def test_perf_test_summary_line_is_parsed():
    line = (
        "50000 records sent, 5293.245818 records/sec (1.01 MB/sec), 5312.65 ms avg latency, "
        "8582.00 ms max latency, 5502 ms 50th, 8017 ms 95th, 8555 ms 99th, 8577 ms 99.9th."
    )
    m = PERF_TEST_LINE.search(line)
    assert m and float(m["rps"]) == pytest.approx(5293.245818) and m["p99"] == "8555"


@pytest.mark.parametrize("name", sorted(EXPERIMENTS))
def test_experiment_configs_load(name):
    for dry in (False, True):
        params, settings = load_config(name, dry)
        ids = [s["id"] for s in settings]
        assert len(ids) == len(set(ids)), "setting ids must be unique (they name result folders)"
        assert params.get("repeats", 3) == 3, "the proposal requires three runs per setting"
        assert all(s.get("repeats", 3) >= 3 for s in settings), "never fewer than three runs"
    _, dry_settings = load_config(name, True)
    assert any(not s.get("skip") for s in dry_settings), "every experiment needs a dry run"


def test_continuous_mode_partitions_capped_at_cores():
    _, settings = load_config("e1b", False)
    for s in settings:
        if s["mode"] == "continuous":
            assert s["partitions"] <= 4  # spike S3: local[4] cannot run more continuous tasks


def test_harness_topic_settings_match_kafka_init():
    script = (ROOT / "infra" / "kafka" / "create-topics.sh").read_text()
    for topic, configs in kafka_admin.TOPIC_CONFIGS.items():
        block = script[script.index(f"--topic {topic}") :].split("\n\n")[0]
        assert f"--replication-factor {kafka_admin.REPLICATION[topic]}" in block, topic
        for key, value in configs.items():
            assert f"{key}={value}" in block, (topic, key)


# --- dashboard (T20) -----------------------------------------------------------------------------
def test_dashboard_json_is_generated_from_the_script():
    committed = (ROOT / "infra" / "grafana" / "dashboards" / "pipeline.json").read_text()
    import importlib.util

    spec = importlib.util.spec_from_file_location("make_dashboard", ROOT / "tools" / "make_dashboard.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert json.dumps(mod.dashboard, indent=2) + "\n" == committed, (
        "run: uv run python tools/make_dashboard.py"
    )


def test_dashboard_panels_cover_dsh_requirements():
    d = json.loads((ROOT / "infra" / "grafana" / "dashboards" / "pipeline.json").read_text())
    types = [p["type"] for p in d["panels"]]
    assert "candlestick" in types and "barchart" in types and types.count("table") >= 2  # DSH-1..4
    assert d["refresh"] in ("2s", "5s")  # DSH-6
    for p in d["panels"]:
        assert p["datasource"]["uid"] == "pg" and p["targets"][0]["rawSql"].strip()
    sql = " ".join(p["targets"][0]["rawSql"] for p in d["panels"])
    assert "query_progress" in sql and "lag_total" in sql  # DSH-5
    assert re.search(r"\$symbol", sql) and d["templating"]["list"][0]["name"] == "symbol"
    provider = yaml.safe_load(
        (ROOT / "infra" / "grafana" / "provisioning" / "dashboards" / "dashboards.yml").read_text()
    )
    assert provider["providers"][0]["options"]["path"] == "/var/lib/grafana/dashboards"
