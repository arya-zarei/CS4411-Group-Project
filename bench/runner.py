"""Benchmark runner (PLAN.md 5.9, T24): one command per experiment, driven by a YAML config.

  uv run python -m bench.runner e2              # full run: every setting, three repeats
  uv run python -m bench.runner e2 --dry-run    # quick check: short runs, one repeat
  uv run python -m bench.runner all --dry-run

Results go to results/raw/<experiment>/<setting>/rep<k>/ (results/raw-dry/... for dry runs):
config.json (parameters, versions, git commit, machine) is written first, result.json last. A run
whose result.json exists is skipped, so an interrupted overnight run can simply be started again.
A failed run writes error.json instead and is retried next time.

Every run starts from a known state: the stack is reset (containers and volumes removed), started,
and the topics are created with the setting's partition count. Run only one experiment at a time,
on one machine, with other heavy programs closed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg
import yaml

from bench import kafka_admin as ka
from bench import stack
from bench.latency import PassthroughCollector, bar_latencies, summarize
from bench.throughput import LagSampler, keeps_up, search

ROOT = stack.ROOT
EXPERIMENTS_DIR = Path(__file__).resolve().parent / "experiments"
PG_DSN = os.environ.get("PG_DSN", "postgresql://pipeline:change-me-locally@localhost:5432/market")
WATERMARK_MS = 10_000
GOLDEN = "data/sample/trades.csv.gz"


# --- small helpers --------------------------------------------------------------------------------
def now_ms() -> int:
    """The current wall-clock time in epoch milliseconds, the unit used in the database and the logs."""
    return int(time.time() * 1000)


def pg():
    """Open an autocommit connection to the benchmark PostgreSQL."""
    return psycopg.connect(PG_DSN, autocommit=True)


def git_commit() -> str | None:
    """The commit being run, stored in each run's config.json so results can be traced to code. None if git
    is unavailable.
    """
    r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    return r.stdout.strip() or None


def py_env() -> dict:
    """The environment for child Python processes: the current one plus PYTHONPATH pointing at the
    repository and the generator package.
    """
    return {**os.environ, "PYTHONPATH": f"{ROOT}:{ROOT / 'generator'}"}


def tradegen(args: list[str], out: Path, background: bool = False):
    """Run the generator on the host (talks to Kafka through localhost:2909x)."""
    cmd = [sys.executable, "-m", "tradegen", *args, "--runs-dir", str(out), "--run-id", "gen"]
    log = open(out / "generator.log", "w")  # noqa: SIM115
    p = subprocess.Popen(cmd, cwd=ROOT, env=py_env(), stdout=log, stderr=subprocess.STDOUT, text=True)
    return p if background else wait_tradegen(p, out)


def wait_tradegen(p, out: Path, timeout_s: float = 3600) -> dict:
    """Wait for a generator process to finish and return its JSON summary. If it wrote no summary it
    failed, so the error includes the end of its log.
    """
    p.wait(timeout=timeout_s)
    summary = out / "gen" / "generator_summary.json"
    if not summary.exists():
        raise RuntimeError(
            f"generator failed (exit {p.returncode}):\n{(out / 'generator.log').read_text()[-2000:]}"
        )
    return json.loads(summary.read_text())


def run_module(module: str, *args: str) -> tuple[int, str]:
    """Run a Python module in the repository as a child process and return its exit code and combined
    output.
    """
    r = subprocess.run(
        [sys.executable, "-m", module, *args], cwd=ROOT, env=py_env(), capture_output=True, text=True
    )
    return r.returncode, (r.stdout + r.stderr).strip()


def verify_bars(out: Path, name: str, *args: str) -> dict:
    """Run the bar/alert verifier (VER-2/3/4) and return its JSON report."""
    report = out / f"verify_{name}.json"
    code, text = run_module("verify.verify", *args, "--report", str(report))
    if not report.exists():
        raise RuntimeError(f"verifier crashed:\n{text[-2000:]}")
    r = json.loads(report.read_text())
    return {
        "status": r["status"],
        "bars": {
            k: r["bars"][k]
            for k in ("expected", "matched", "missing", "extra", "duplicate_rows", "value_mismatches")
        },
        "alerts": {
            k: r["alerts"][k]
            for k in ("expected", "matched", "missing", "extra", "duplicate_rows", "value_mismatches")
        },
        "summary": text.splitlines()[-1] if text else "",
    }


def verify_kafka(out: Path) -> dict:
    """Run the Kafka-layer verifier on a run's ack log and return the fields worth keeping (acked, lost,
    duplicated, write stall). It raises if the verifier produced no report, which means it crashed.
    """
    report = out / "verify_kafka.json"
    code, text = run_module(
        "verify.kafka_layer", "--acks", str(out / "gen" / "acks.csv"), "--report", str(report)
    )
    if not report.exists():
        raise RuntimeError(f"Kafka-layer verifier crashed:\n{text[-2000:]}")
    r = json.loads(report.read_text())
    keep = (
        "acked",
        "failed",
        "records_in_topic",
        "lost",
        "duplicated_ids",
        "duplicate_records",
        "unacked_but_present",
    )
    return {"status": r["status"], **{k: r[k] for k in keep}, "write_stall": r.get("write_stall")}


def dump_virtual(params: dict, path: Path) -> None:
    """The exact trades a virtual-mode run sends (without the flush trade), for VER-2 (a)."""
    code, text = run_module("tradegen", *virtual_args(params), "--speed", "max", "--dump-csv", str(path))
    if code != 0:
        raise RuntimeError(text[-2000:])


def virtual_args(p: dict) -> list[str]:
    """Generator arguments for virtual mode from an experiment's parameters: seed (default 42), virtual
    minutes and mean rate per symbol.
    """
    return [
        "--mode", "virtual", "--seed", str(p.get("seed", 42)), "--minutes", str(p["minutes"]),
        "--mean-rate-per-symbol", str(p.get("mean_rate_per_symbol", 0.25)),
    ]  # fmt: skip


def fresh_stack(partitions: int = 6) -> None:
    """Known state: no containers, no volumes; Kafka x3, topics, PostgreSQL up and healthy."""
    stack.reset()
    stack.start_infra()
    if partitions != 6:
        ka.recreate_topics({"trades": partitions, "passthrough_out": min(partitions, 6)})


def start_spark(service: str, env: dict, checkpoint_volume: str) -> None:
    """Start a Spark job from a clean state. It stops the service, wipes its checkpoint volume, starts it
    with the given environment, and waits until the job logs that it started. A wiped checkpoint matters
    because a checkpoint from an old topic would make Spark fail on data loss.
    """
    stack.stop(service)
    stack.wipe_volume(checkpoint_volume)
    stack.up(service, env=env, wait=True)
    marker = "[bars_job] started" if service == "spark-bars" else "[passthrough] started"
    stack.wait_for_log(service, marker, 180)


def progress(conn, since_ms: int = 0) -> list[tuple]:
    """Read the Spark progress rows (batch id, input rows, batch duration, processing rate) written since a
    time.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extract(epoch FROM ts) * 1000, batch_id, num_input_rows, batch_duration_ms, "
            "processed_rows_per_s FROM query_progress WHERE ts >= to_timestamp(%s / 1000.0) ORDER BY id",
            (since_ms,),
        )
        return cur.fetchall()


def bars_lag_fn(conn):
    """Kafka end offsets of `trades` minus the offsets of Spark's latest completed batch."""

    def lag() -> float:
        """Records Spark has not processed yet: Kafka's end offsets for the topic minus the offsets in
        Spark's latest progress row.
        """
        with conn.cursor() as cur:
            cur.execute("SELECT end_offsets FROM query_progress ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
        processed = sum(int(v) for parts in (row[0] or {}).values() for v in parts.values()) if row else 0
        return ka.total_end_offset("trades") - processed

    return lag


def passthrough_lag() -> float:
    """Input records not yet in the output topic (one output record per input record)."""
    return ka.total_end_offset("trades") - ka.total_end_offset("passthrough_out")


def generator_limited(summary: dict, rate: float) -> bool:
    """True if the generator managed less than 95% of the requested rate. Such a step says more about the
    generator than about Spark, so the result is marked.
    """
    return summary.get("achieved_rate_per_s", 0) < 0.95 * rate


def realtime_args(rate: float, duration: float) -> list[str]:
    """Generator arguments for a realtime run at a rate for some seconds, without the closing flush trade
    (the run is measured, not verified).
    """
    return ["--mode", "realtime", "--rate", str(rate), "--duration", str(duration), "--no-flush-trade"]


PERF_TEST_LINE = re.compile(
    r"(?P<records>\d+) records sent, (?P<rps>[\d.]+) records/sec \((?P<mbps>[\d.]+) MB/sec\), "
    r"(?P<avg>[\d.]+) ms avg latency, (?P<max>[\d.]+) ms max latency, (?P<p50>\d+) ms 50th, "
    r"(?P<p95>\d+) ms 95th, (?P<p99>\d+) ms 99th"
)


# --- E1a: producer batching (reproduction a, Kreps et al. Figure 4) ------------------------------
def run_e1a(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 1a. Recreate the one-partition test topic and run Kafka's producer performance tool with
    the setting's batch size and linger, then parse the final summary line of its output (the interval
    lines have no percentiles) into throughput and latency numbers.
    """
    stack.start_infra()  # no reset needed: perf_test is recreated, nothing else is involved
    ka.recreate_topics({"perf_test": 1})
    props = [
        f"acks={p.get('acks', 1)}",
        f"batch.size={setting['batch_size']}",
        f"linger.ms={setting['linger_ms']}",
    ]
    cmd = [
        "exec", "kafka-1", "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--bootstrap-server", "localhost:9092", "--topic", "perf_test",
        "--num-records", str(p["num_records"]), "--record-size", str(p["record_size"]),
        "--throughput", "-1", "--command-property", *props,
    ]  # fmt: skip
    text = stack.docker(*cmd)
    (out / "perf_test.log").write_text(text)
    m = PERF_TEST_LINE.search(text)  # the final summary line (interval lines lack the percentiles)
    if not m:
        raise RuntimeError(f"unexpected kafka-producer-perf-test output:\n{text[-1500:]}")
    return {
        "records": int(m["records"]),
        "records_per_s": float(m["rps"]),
        "mb_per_s": float(m["mbps"]),
        "avg_latency_ms": float(m["avg"]),
        "max_latency_ms": float(m["max"]),
        "p50_latency_ms": float(m["p50"]),
        "p95_latency_ms": float(m["p95"]),
        "p99_latency_ms": float(m["p99"]),
        "batch_size_bytes": setting["batch_size"],
        "batch_records_approx": round(setting["batch_size"] / p["record_size"], 1),
        "linger_ms": setting["linger_ms"],
        "producer_props": props,
    }


# --- E1b: pass-through, continuous versus micro-batch (reproduction b, Armbrust et al. Figure 7) -
def passthrough_step(setting: dict, p: dict, out: Path, rate: float) -> dict:
    """One E1b measurement at one input rate. It recreates the topics, starts the pass-through job, and
    runs the generator while a collector records each record's latency and a sampler records the lag.
    Latencies from the warm-up period are dropped. The result has the latency summary, whether Spark
    kept up, and whether the generator reached the rate.
    """
    env = {"PT_MODE": setting["mode"], "PT_TRIGGER_INTERVAL": setting.get("trigger", "100 milliseconds")}
    ka.recreate_topics({"trades": setting["partitions"], "passthrough_out": setting["partitions"]})
    start_spark("spark-passthrough", env, "passthrough-checkpoint")
    time.sleep(p.get("settle_s", 10))
    collector = PassthroughCollector().start()
    sampler = LagSampler(passthrough_lag).start()
    step_dir = out / f"rate-{int(rate)}"
    step_dir.mkdir(exist_ok=True)
    t_start = now_ms()
    gen = tradegen(realtime_args(rate, p["duration_s"]), step_dir)
    samples = sampler.stop()
    collector.stop(drain_s=3)
    warm = t_start + p["warmup_s"] * 1000  # discard JVM and connection warm-up (measured on 2026-11-11)
    lat = [
        lat for lat, created in zip(collector.latencies, collector.created, strict=True) if created >= warm
    ]
    stack.stop("spark-passthrough")
    return {
        "latency": summarize(lat),
        **keeps_up(samples, rate),
        "achieved_rate": gen["achieved_rate_per_s"],
        "generator_limited": generator_limited(gen, rate),
    }


def run_e1b(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 1b. Start from a clean stack, then either search for the highest rate that keeps up
    (micro-batch) or measure each rate in the list once (continuous latency curve).
    """
    fresh_stack(setting["partitions"])
    if setting.get("search_throughput"):
        return search(setting["rates"], lambda r: passthrough_step(setting, p, out, r))
    return {"steps": [{"rate": r, **passthrough_step(setting, p, out, r)} for r in setting["rates"]]}


# --- E1c: bars pipeline, partitions and trigger interval -----------------------------------------
def bars_step(setting: dict, p: dict, out: Path, rate: float) -> dict:
    """One bars-pipeline measurement at one input rate (E1c and the E4 cost runs). It recreates the topic,
    empties the tables, starts the bars job, runs the generator, waits for the last windows to be
    written, then computes bar latency after warm-up, batch durations and whether Spark kept up.
    """
    ka.recreate_topics({"trades": setting["partitions"]})
    with pg() as c:
        c.execute("TRUNCATE bars, alerts, bars_plain, alerts_plain, query_progress")
    env = {
        "TRIGGER_INTERVAL": setting["trigger"],
        "STARTING_OFFSETS": "latest",
        "SINK_MODE": setting.get("sink", "upsert"),
    }
    start_spark("spark-bars", env, "spark-checkpoint")
    time.sleep(p.get("settle_s", 10))
    step_dir = out / f"rate-{int(rate)}"
    step_dir.mkdir(exist_ok=True)
    with pg() as conn:
        sampler = LagSampler(bars_lag_fn(conn)).start()
        t_start = now_ms()
        gen = tradegen(realtime_args(rate, p["duration_s"]), step_dir)
        samples = sampler.stop()
        time.sleep(WATERMARK_MS / 1000 + 5)  # let the last finished windows be written
        table = "bars_plain" if setting.get("sink") == "insert" else "bars"
        lat = bar_latencies_table(conn, table, since_ms=t_start + p["warmup_s"] * 1000)
        prog = progress(conn, t_start)
    stack.stop("spark-bars")
    durations = [r[3] for r in prog if r[3] is not None and r[2]]
    return {
        "bar_latency": summarize(lat),
        **keeps_up(samples, rate),
        "batches": len(prog),
        "batch_duration_ms": summarize(durations),
        "achieved_rate": gen["achieved_rate_per_s"],
        "generator_limited": generator_limited(gen, rate),
    }


def bar_latencies_table(conn, table: str, since_ms: int) -> np.ndarray:
    """Bar latencies (time written minus window end minus watermark) from the chosen table. The
    plain-insert table needs its own query, because it can hold several rows per bar.
    """
    if table == "bars":
        return bar_latencies(conn, WATERMARK_MS, since_ms)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extract(epoch FROM (written_at - (window_end + interval '10 seconds'))) * 1000 "
            "FROM bars_plain WHERE window_end >= to_timestamp(%s / 1000.0)",
            (since_ms,),
        )
        return np.array([float(r[0]) for r in cur.fetchall()])


def run_e1c(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 1c. Start from a clean stack with the setting's partition count and search for the
    highest input rate the bars job sustains.
    """
    fresh_stack(setting["partitions"])
    return search(setting["rates"], lambda r: bars_step(setting, p, out, r))


# --- E2: broker crash ----------------------------------------------------------------------------
def run_e2(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 2, broker crash. Run a paced virtual stream, and halfway through kill the broker that
    leads partition 0. After the generator finishes, bring the broker back and check three things: the
    Kafka layer (nothing acknowledged was lost), the bars against the generator's trades, and the bars
    against what the topic holds. The result includes the write stall and whether it overlaps the kill.
    """
    fresh_stack()
    start_spark("spark-bars", {}, "spark-checkpoint")
    wall_s = p["minutes"] * 60 / p["speed"]
    args = [*virtual_args(p), "--speed", str(p["speed"]), "--acks", str(setting["acks"]), "--log-trades"]
    if "idempotence" in setting:
        args += ["--idempotence", str(setting["idempotence"]).lower()]
    gen_proc = tradegen(args, out, background=True)
    t0 = time.time()
    time.sleep(p["kill_at_fraction"] * wall_s)
    leader = ka.leader_of("trades", 0)
    t_kill = now_ms()
    stack.kill(f"kafka-{leader}")
    gen = wait_tradegen(gen_proc, out)
    stack.up(f"kafka-{leader}")  # bring it back so the topic can be read in full
    ka.wait_for_leaders(["trades"])
    kafka = verify_kafka(out)
    dump_virtual(p, out / "trades.csv")
    bars_a = verify_bars(out, "a", "--trades", str(out / "trades.csv"), "--wait", "300")
    bars_b = verify_bars(out, "b", "--from-topic")  # Spark versus what Kafka actually holds
    stall = kafka.get("write_stall") or {}
    return {
        "killed_broker": f"kafka-{leader}",
        "kill_at_s": round((t_kill / 1000) - t0, 1),
        "write_stall_ms": stall.get("max_partition_gap_ms"),  # longest ack gap within one partition
        "write_stall_any_ack_ms": stall.get("max_gap_ms"),  # longest gap with no ack on any partition
        "stall_partitions": stall.get("per_partition_max_gap_ms"),
        "stall_overlaps_kill": bool(stall.get("partition_gap_start_ms"))
        and stall["partition_gap_start_ms"] <= t_kill + 5000
        and stall["partition_gap_end_ms"] >= t_kill,
        "kafka": kafka,
        "bars_vs_generator": bars_a,
        "bars_vs_topic": bars_b,
        "generator": gen,
    }


# --- E3: Spark crash ------------------------------------------------------------------------------
def recovery_times(conn, t_kill: int, t_restart: int, timeout_s: float = 300) -> dict:
    """Kill to the first batch committed after the restart, and to the first new bar written."""
    deadline = time.monotonic() + timeout_s
    first_batch = first_bar = None
    while time.monotonic() < deadline and (first_batch is None or first_bar is None):
        with conn.cursor() as cur:
            if first_batch is None:
                cur.execute(
                    "SELECT extract(epoch FROM min(ts)) * 1000 FROM query_progress "
                    "WHERE ts > to_timestamp(%s / 1000.0)",
                    (t_restart,),
                )
                v = cur.fetchone()[0]
                first_batch = int(v) if v is not None else None
            if first_bar is None:
                cur.execute(
                    "SELECT extract(epoch FROM min(written_at)) * 1000 FROM bars "
                    "WHERE written_at > to_timestamp(%s / 1000.0)",
                    (t_kill,),
                )
                v = cur.fetchone()[0]
                first_bar = int(v) if v is not None else None
        time.sleep(0.5)
    return {
        "down_s": round((t_restart - t_kill) / 1000, 3),
        "kill_to_first_batch_s": round((first_batch - t_kill) / 1000, 3) if first_batch else None,
        "kill_to_first_bar_s": round((first_bar - t_kill) / 1000, 3) if first_bar else None,
        "restart_to_first_batch_s": round((first_batch - t_restart) / 1000, 3) if first_batch else None,
    }


def run_e3(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 3, Spark crash. Run either a deterministic replay (kill after a fixed batch) or a paced
    live stream (kill halfway), restart Spark on the same checkpoint at once, measure how long it takes
    to produce new output, and verify that the final bars are exact.
    """
    fresh_stack()
    with pg() as conn:
        if setting["mode"] == "deterministic":
            gen = tradegen(["--mode", "replay", "--file", GOLDEN, "--speed", "max", "--log-trades"], out)
            env = {"MAX_OFFSETS_PER_TRIGGER": str(p["max_offsets_per_trigger"])}
            start_spark("spark-bars", env, "spark-checkpoint")
            while not any(r[1] >= p["kill_after_batch"] for r in progress(conn)):
                time.sleep(0.2)
            t_kill = now_ms()
            stack.kill("spark-bars")
        else:
            env = {}
            start_spark("spark-bars", env, "spark-checkpoint")
            wall_s = p["minutes"] * 60 / p["speed"]
            gen_proc = tradegen(
                [*virtual_args(p), "--speed", str(p["speed"]), "--log-trades"], out, background=True
            )
            time.sleep(p["kill_at_fraction"] * wall_s)
            t_kill = now_ms()
            stack.kill("spark-bars")
        time.sleep(p.get("down_s", 0))
        t_restart = now_ms()
        stack.up("spark-bars", env=env, wait=False)  # same checkpoint volume: Spark resumes from it
        rec = recovery_times(conn, t_kill, t_restart)
        if setting["mode"] != "deterministic":
            gen = wait_tradegen(gen_proc, out)
            dump_virtual(p, out / "trades.csv")
            trades = str(out / "trades.csv")
        else:
            trades = GOLDEN
    bars = verify_bars(out, "a", "--trades", trades, "--wait", "300")
    return {"mode": setting["mode"], **rec, "bars": bars, "generator": gen}


# --- E4: exactly-once cost -------------------------------------------------------------------------
def run_e4(setting: dict, p: dict, out: Path) -> dict:
    """Experiment 4, exactly-once cost. In a crash run, Spark is set to kill itself after a batch commits
    to the database and before Spark records it. After the restart, the bars are verified and duplicate
    rows are counted, which is the difference between plain inserts and upserts. A cost run is a normal
    crash-free measurement at one fixed rate.
    """
    fresh_stack()
    sink = setting["sink"]
    if setting["kind"] == "crash":  # deterministic replay, crash after batch N commits (SPK-10)
        gen = tradegen(["--mode", "replay", "--file", GOLDEN, "--speed", "max", "--log-trades"], out)
        env = {"SINK_MODE": sink, "MAX_OFFSETS_PER_TRIGGER": str(p["max_offsets_per_trigger"]),
               "FAULT_AFTER_BATCH": str(p["fault_after_batch"])}  # fmt: skip
        start_spark("spark-bars", env, "spark-checkpoint")
        deadline = time.monotonic() + 300
        while stack.is_running("spark-bars"):
            if time.monotonic() > deadline:
                raise TimeoutError("the SPK-10 fault never fired")
            time.sleep(0.5)
        crash = {
            "exit_code": stack.exit_code("spark-bars"),
            "fired": "FAULT_AFTER_BATCH" in stack.logs("spark-bars"),
        }
        stack.up(
            "spark-bars", env=env, wait=False
        )  # restart; the marker in the checkpoint stops a second crash
        bars = verify_bars(out, "a", "--trades", GOLDEN, "--sink", sink, "--wait", "300")
        return {
            "sink": sink,
            "crash": crash,
            "duplicate_bar_rows": bars["bars"]["duplicate_rows"],
            "duplicate_alert_rows": bars["alerts"]["duplicate_rows"],
            "bars": bars,
            "generator": gen,
        }
    # crash-free cost at a fixed input rate (same rate for both sinks)
    step = bars_step({"partitions": 6, "trigger": "1 second", "sink": sink}, p, out, p["rate"])
    return {"sink": sink, "rate": p["rate"], **step}


EXPERIMENTS = {"e1a": run_e1a, "e1b": run_e1b, "e1c": run_e1c, "e2": run_e2, "e3": run_e3, "e4": run_e4}
ORDER = ["e2", "e3", "e4", "e1a", "e1b", "e1c"]  # PLAN.md 5.9: RQ2 and RQ3 first if time runs short


# --- driver ----------------------------------------------------------------------------------------
def load_config(name: str, dry: bool) -> tuple[dict, list[dict]]:
    """Read an experiment's YAML. In a dry run the short-run overrides replace the full values, for the
    whole experiment and for each setting.
    """
    cfg = yaml.safe_load((EXPERIMENTS_DIR / f"{name}.yaml").read_text())
    params = {**cfg.get("params", {}), **(cfg.get("dry_run", {}) if dry else {})}
    settings = cfg["settings"]
    if dry:
        for s in settings:
            s.update(s.get("dry_run", {}))
    return params, settings


def run_experiment(
    name: str, dry: bool, repeats: int | None, only: str | None, results: Path
) -> tuple[list, int]:
    """Run every setting of an experiment, the configured number of times (E2 acks=1 asks for more). A run
    that already has result.json is skipped, so an interrupted experiment resumes where it stopped. Each
    run writes config.json first, then result.json, or error.json if it failed; one failure does not
    stop the others.
    """
    params, settings = load_config(name, dry)
    repeats = repeats or params.get("repeats", 3)
    rows, errors = [], 0
    for s in settings:
        if (only and only not in s["id"]) or (dry and s.get("skip")):
            continue
        n = 1 if dry else s.get("repeats", repeats)  # a setting may ask for more runs (E2 acks=1)
        for rep in range(1, n + 1):
            out = results / name / s["id"] / f"rep{rep}"
            if (out / "result.json").exists():
                print(f"[{name}] {s['id']} rep{rep}: done already, skipping")
                rows.append(json.loads((out / "result.json").read_text()))
                continue
            out.mkdir(parents=True, exist_ok=True)
            (out / "error.json").unlink(missing_ok=True)
            config = {
                "experiment": name, "setting": s, "repeat": rep, "dry_run": dry,
                "params": {**params, **s.get("params", {})},
                "git_commit": git_commit(), "started_at": datetime.now(UTC).isoformat(),
                "machine": stack.machine_info(),
            }  # fmt: skip
            (out / "config.json").write_text(json.dumps(config, indent=2) + "\n")
            print(f"[{name}] {s['id']} rep{rep}: running", flush=True)
            t0 = time.monotonic()
            try:
                result = EXPERIMENTS[name](s, {**params, **s.get("params", {})}, out)
            except Exception as exc:  # noqa: BLE001  record and continue with the next run
                (out / "error.json").write_text(
                    json.dumps({"error": repr(exc), "traceback": traceback.format_exc()}, indent=2)
                )
                print(f"[{name}] {s['id']} rep{rep}: ERROR {exc!r}", flush=True)
                errors += 1
                continue
            result = {
                "experiment": name,
                "setting": s["id"],
                "repeat": rep,
                "elapsed_s": round(time.monotonic() - t0, 1),
                **result,
            }
            (out / "result.json").write_text(json.dumps(result, indent=2, default=float) + "\n")
            print(
                f"[{name}] {s['id']} rep{rep}: done in {result['elapsed_s']}s | {headline(result)}",
                flush=True,
            )
            rows.append(result)
    return rows, errors


def headline(r: dict) -> str:
    """One line per run for the console."""
    if "records_per_s" in r:
        return f"{r['records_per_s']:.0f} records/s, {r['mb_per_s']:.2f} MB/s"
    if "sustained_rate" in r:
        return f"sustained {r['sustained_rate']} records/s (limited by {r['limited_by']})"
    if "kafka" in r:
        k = r["kafka"]
        return (
            f"lost {k['lost']}, duplicated {k['duplicated_ids']}, stall {r['write_stall_ms']} ms; "
            f"bars vs generator {r['bars_vs_generator']['status']}, vs topic {r['bars_vs_topic']['status']}"
        )
    if "kill_to_first_batch_s" in r:
        return (
            f"recovery {r['kill_to_first_batch_s']} s to first batch, "
            f"{r['kill_to_first_bar_s']} s to first bar; {r['bars']['status']}"
        )
    if "duplicate_bar_rows" in r:
        return f"{r['sink']}: duplicate bar rows {r['duplicate_bar_rows']}; {r['bars']['status']}"
    if "steps" in r:
        return "; ".join(
            f"{s['rate']:.0f}/s p50 {s.get('latency', {}).get('p50_ms', float('nan')):.0f} ms"
            for s in r["steps"]
        )
    if "bar_latency" in r:
        p50 = r["bar_latency"].get("p50_ms")
        return f"{r['sink']} at {r['rate']}/s: bar latency p50 {p50} ms, keeps up {r['keeps_up']}"
    return ""


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. It builds the images unless told not to, runs the chosen experiment (or
    all of them in order), prints how many runs succeeded and failed, tears the stack down, and exits
    non-zero if any run failed.
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("experiment", choices=[*EXPERIMENTS, "all"])
    ap.add_argument(
        "--dry-run", action="store_true", help="short runs, one repeat, results under results/raw-dry"
    )
    ap.add_argument("--repeats", type=int, help="override the config (default 3; 1 for dry runs)")
    ap.add_argument("--only", help="run only settings whose id contains this text")
    ap.add_argument("--no-build", action="store_true", help="reuse the images already built")
    ap.add_argument("--results", type=Path, help="results directory (default results/raw or results/raw-dry)")
    a = ap.parse_args(argv)
    results = a.results or ROOT / ("results/raw-dry" if a.dry_run else "results/raw")
    if not a.no_build:
        stack.build("generator", "spark-bars", "spark-passthrough")
    names = ORDER if a.experiment == "all" else [a.experiment]
    failed = 0
    for name in names:
        rows, errors = run_experiment(
            name, a.dry_run, a.repeats or (1 if a.dry_run else None), a.only, results
        )
        failed += errors
        print(f"[{name}] {len(rows)} run(s) with results, {errors} failed (see error.json)")
    stack.reset()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
