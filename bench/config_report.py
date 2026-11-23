"""Write results/CONFIGURATION.md: the Kafka, Spark and producer configuration of every experiment.

Everything is read from the files that drive the runs (``docker-compose.yml``, ``infra/kafka/create-topics.sh``,
the generator's defaults and ``bench/experiments/*.yaml``), so the document cannot drift from what ran.
A test fails if the committed file differs from a fresh render.

    uv run python -m bench.config_report            # rewrite results/CONFIGURATION.md
    uv run python -m bench.config_report --check    # exit 1 if it is out of date
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT))

from tradegen import cli  # noqa: E402

OUT = ROOT / "results" / "CONFIGURATION.md"
EXPERIMENTS = ["e1a", "e1b", "e1c", "e2", "e3", "e4"]
DEFAULT_RE = re.compile(r"\$\{[A-Z_]+:-([^}]*)\}")


def resolve(value) -> str:
    """Replace ``${VAR:-default}`` by its default: the value used when nothing overrides it."""
    return DEFAULT_RE.sub(lambda m: m.group(1), str(value))


def compose() -> dict:
    """Read the services section of docker-compose.yml."""
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]


def md_table(header: list[str], rows: list[list]) -> str:
    """Render a Markdown table from a header and rows."""
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def topic_settings() -> list[list]:
    """(topic, partitions, RF, configs) read from create-topics.sh."""
    text = (ROOT / "infra" / "kafka" / "create-topics.sh").read_text().replace("\\\n", " ")
    rows = []
    for m in re.finditer(r"create --topic (\S+) --partitions (\S+) --replication-factor (\d+)([^\n]*)", text):
        parts = m.group(2).strip('"')
        if parts.startswith("$"):  # a shell variable: its default is set at the top of the script
            d = re.search(rf"{re.escape(parts[1:])}=.*?:-(?:\$\{{\w+:-)?(\d+)", text)
            parts = f"{d.group(1)} by default, set per experiment" if d else "same as `trades`"
        cfg = ", ".join(re.findall(r"--config (\S+)", m.group(4)))
        rows.append([f"`{m.group(1)}`", parts, m.group(3), cfg])
    return rows


def broker_section(svc: dict) -> str:
    """The Kafka broker settings from the compose file (image and environment, without the per-node ids),
    as a table.
    """
    env = svc["kafka-1"]["environment"]
    shared = {
        k: resolve(v)
        for k, v in env.items()
        if k not in {"KAFKA_NODE_ID", "KAFKA_LISTENERS", "KAFKA_ADVERTISED_LISTENERS", "CLUSTER_ID"}
    }
    rows = [[f"`{k}`", f"`{v}`"] for k, v in shared.items()]
    image = resolve(svc["kafka-1"]["image"])
    return (
        f"Image `{image}`; three nodes (`kafka-1` to `kafka-3`), each broker and controller (KRaft combined mode).\n\n"
        + md_table(["Setting (docker-compose.yml)", "Value"], rows)
    )


def spark_section(svc: dict) -> str:
    """The Spark settings from the compose file: for each job its spark-submit options, environment and
    container limits, with ${VAR:-default} replaced by the default.
    """
    bars, pt = svc["spark-bars"], svc["spark-passthrough"]
    rows = []
    for name, s in (("bars job (`spark-bars`)", bars), ("pass-through job (`spark-passthrough`)", pt)):
        cmd = [resolve(c) for c in s["command"] if c.startswith("--")]
        env = {k: resolve(v) for k, v in s["environment"].items() if k not in {"HOME", "PG_DSN"}}
        rows.append([name, "`" + " ".join(cmd) + "`"])
        rows.append(["", "environment: " + ", ".join(f"`{k}={v}`" for k, v in env.items() if v != "")])
        rows.append(
            ["", f"container: {s['cpus']} CPUs, memory limit `{resolve(s['mem_limit'])}`, `init: true`"]
        )
    image = resolve(bars["build"]["args"]["SPARK_IMAGE"])
    connector = resolve(bars["build"]["args"]["SPARK_KAFKA_CONNECTOR"])
    return (
        f"Image built `FROM {image}` with the connector `{connector}` baked in. Source options set in the "
        "jobs (not environment-dependent): `failOnDataLoss=true`, `subscribe` to one topic, checkpoints on a "
        "named volume, bars output in append mode through `foreachBatch`.\n\n"
        + md_table(["Job", "Configuration"], rows)
    )


def producer_section() -> str:
    """The generator's producer settings, taken from the generator's own argument parser, for acks=all and
    for acks=1, so the document shows what the code really uses.
    """
    conf = cli.producer_conf(cli.parse_args(["--mode", "virtual"]))
    conf_acks1 = cli.producer_conf(cli.parse_args(["--mode", "virtual", "--acks", "1"]))

    def show(v):
        """Format a setting value, writing booleans in lower case as Kafka does."""
        return str(v).lower() if isinstance(v, bool) else str(v)

    rows = [[f"`{k}`", f"`{show(v)}`", f"`{show(conf_acks1[k])}`"] for k, v in conf.items()]
    return (
        "Generator (`confluent-kafka`, librdkafka) defaults from `generator/tradegen/cli.py`:\n\n"
        + md_table(["Producer property", "acks=all (default)", "acks=1"], rows)
    )


def load(name: str) -> tuple[dict, list[dict]]:
    """Read an experiment's YAML and return its shared parameters and its list of settings."""
    cfg = yaml.safe_load((ROOT / "bench" / "experiments" / f"{name}.yaml").read_text())
    return cfg.get("params", {}), cfg["settings"]


def fmt(v) -> str:
    """Format a number with thousands separators when it has five digits or more, and anything else as
    text.
    """
    return f"{v:,}" if isinstance(v, int) and not isinstance(v, bool) and abs(v) >= 10000 else str(v)


def settings_table(columns: list[tuple[str, str]], settings: list[dict], defaults: dict) -> str:
    """A table with one row per setting and the chosen columns. A value missing from the setting falls back
    to the experiment's shared parameters, and lists such as input rates are joined with commas.
    """
    rows = []
    for s in settings:
        row = [f"`{s['id']}`"]
        for _, key in columns:
            v = s.get(key, defaults.get(key, "-"))
            row.append(", ".join(fmt(x) for x in v) if isinstance(v, list) else fmt(v))
        rows.append(row)
    return md_table(["Setting", *[c for c, _ in columns]], rows)


def experiment_sections() -> str:
    """Write the description and table for each of the six experiments from their YAML parameters."""
    out = []
    p, s = load("e1a")
    out.append(
        "### E1a: producer batching (reproduction a)\n\n"
        "Kafka's own `kafka-producer-perf-test.sh` runs inside `kafka-1` against topic `perf_test` "
        "(1 partition, RF 1, min ISR 1; the other topics are not involved). "
        f"{fmt(p['num_records'])} records of {p['record_size']} bytes, `--throughput -1`, `acks={p['acks']}`, "
        "no Spark. Topic recreated before each setting; the producer and the broker share the machine.\n\n"
        + settings_table([("batch.size (bytes)", "batch_size"), ("linger.ms", "linger_ms")], s, p)
    )
    p, s = load("e1b")
    out.append(
        "### E1b: continuous versus micro-batch (reproduction b)\n\n"
        "Pass-through job (`spark-passthrough`, `trades` to `passthrough_out`, no state), generator in "
        "`realtime` mode with `acks=all` and idempotence on. `trades` and `passthrough_out` have as many "
        "partitions as the setting says (RF 3, min ISR 2). Continuous mode: `trigger(continuous='1 second')` "
        "(the checkpoint interval); micro-batch: `processingTime` trigger. `startingOffsets=latest`.\n\n"
        + settings_table(
            [
                ("mode", "mode"),
                ("partitions", "partitions"),
                ("trigger", "trigger"),
                ("input rates (records/s)", "rates"),
            ],
            s,
            {**p, "trigger": "n/a (continuous)"},
        )
        + f"\n\nRun length per rate: {p['duration_s']} s (continuous curve points use "
        f"{s[0].get('params', {}).get('duration_s', p['duration_s'])} s), warm-up discarded {p['warmup_s']} s "
        f"({s[0].get('params', {}).get('warmup_s', p['warmup_s'])} s for continuous), "
        "three repeats; micro-batch searches for the highest rate that keeps up (decision D5)."
    )
    p, s = load("e1c")
    out.append(
        "### E1c: partitions and trigger interval (RQ1)\n\n"
        "The real bars job (`spark-bars`, `local[4]`, watermark `10 seconds`, `SINK_MODE=upsert`, "
        "`spark.sql.shuffle.partitions=4`), micro-batch with a `processingTime` trigger. Generator in `realtime` "
        "mode, `acks=all`, idempotence on, `--no-flush-trade`. `trades` has the partition count of the setting "
        "(RF 3, min ISR 2), `startingOffsets=latest`, PostgreSQL truncated and Kafka topic recreated before each "
        "step.\n\n"
        + settings_table(
            [("partitions", "partitions"), ("trigger", "trigger"), ("input rates (records/s)", "rates")], s, p
        )
        + f"\n\nEach step: {p['duration_s']} s, first {p['warmup_s']} s discarded from latency, three repeats."
    )
    p, s = load("e2")
    out.append(
        "### E2: broker crash (RQ2)\n\n"
        "Bars job (defaults above), `trades` with 6 partitions (RF 3, min ISR 2, unclean election off), "
        "`startingOffsets=earliest`. Generator in `virtual` mode: seed 42, "
        f"{p['minutes']} virtual minutes at speed {p['speed']}x, {p['mean_rate_per_symbol']} trades per symbol "
        f"per virtual second, per-trade acknowledgement log. At {int(p['kill_at_fraction'] * 100)} % of the "
        "run the harness runs `docker kill` on the broker leading partition 0 and restarts it afterwards.\n\n"
        + settings_table(
            [("acks", "acks"), ("idempotence", "idempotence"), ("kills (repeats)", "repeats")],
            [{**x, "idempotence": str(x.get("idempotence", x["acks"] == "all")).lower()} for x in s],
            p,
        )
        + "\n\nWith `acks=1` the producer cannot enable idempotence (Kafka rejects the combination), so a "
        "producer retry can duplicate a trade."
    )
    p, s = load("e3")
    out.append(
        "### E3: Spark crash (RQ2)\n\n"
        "Bars job (defaults above), `trades` with 6 partitions, generator `acks=all`, idempotence on. The Spark "
        "container is sent `SIGKILL` at "
        f"{int(p['kill_at_fraction'] * 100)} % of the stream (live) or after batch {p['kill_after_batch']} "
        "(deterministic) and restarted at once on the same checkpoint volume.\n\n"
        + md_table(
            ["Setting", "Input", "Spark settings"],
            [
                ["`live`", f"virtual mode, seed 42, {p['minutes']} min at {p['speed']}x, paced", "defaults"],
                [
                    "`deterministic`",
                    "golden dataset preloaded (`data/sample`, 60,163 trades), replay at maximum speed",
                    f"`MAX_OFFSETS_PER_TRIGGER={p['max_offsets_per_trigger']}`, `STARTING_OFFSETS=earliest`",
                ],
            ],
        )
    )
    p, s = load("e4")
    out.append(
        "### E4: cost of exactly-once output (RQ3)\n\n"
        "Bars job (defaults above); `SINK_MODE` selects the table and write statement (`insert`: plain "
        "`INSERT` into `bars_plain`/`alerts_plain`; `upsert`: `INSERT ... ON CONFLICT DO UPDATE` into "
        "`bars`/`alerts`). `crash` runs replay the golden dataset with "
        f"`MAX_OFFSETS_PER_TRIGGER={p['max_offsets_per_trigger']}` and `FAULT_AFTER_BATCH={p['fault_after_batch']}` "
        "(Spark kills its own JVM after the database commit and before recording the batch). `cost` runs are "
        f"crash-free, realtime at {fmt(p['rate'])} records/s for {p['duration_s']} s, warm-up {p['warmup_s']} s.\n\n"
        + settings_table([("kind", "kind"), ("sink", "sink")], s, p)
    )
    return "\n\n".join(out)


def render() -> str:
    """Build the whole document text from the compose file, topic script, generator defaults and experiment
    definitions.
    """
    svc = compose()
    return (
        "\n\n".join(
            [
                "# Configuration of every experiment",
                "Generated by `uv run python -m bench.config_report` from `docker-compose.yml`, "
                "`infra/kafka/create-topics.sh`, `generator/tradegen/cli.py` and `bench/experiments/*.yaml`; "
                "do not edit by hand (a test fails when this file is out of date). Every run also stores its own "
                "parameters, git commit and machine in `config.json` under `results/raw/` (git-ignored; the "
                "summaries are in `summary.csv`). Machine and versions: [MACHINE.md](MACHINE.md).",
                "## 1. Settings shared by all experiments",
                "### Kafka brokers\n\n" + broker_section(svc),
                "Page cache and log flushing are Kafka's defaults (no `log.flush.*` override): durability comes "
                "from replication, not from `fsync`. Broker failure detection uses the KRaft defaults "
                "(`broker.session.timeout.ms` 9,000, heartbeat 2,000), which is what the 9 s write stall in E2 "
                "measures.",
                "### Topics\n\n" + md_table(["Topic", "Partitions", "RF", "Config"], topic_settings()),
                "### Spark\n\n" + spark_section(svc),
                "### Producer\n\n" + producer_section(),
                "## 2. Per experiment",
                experiment_sections(),
                "## 3. Where each value lives\n\nExperiment parameters: `bench/experiments/*.yaml`; stack: "
                "`docker-compose.yml`; topics: `infra/kafka/create-topics.sh`; producer: `generator/tradegen/cli.py`; "
                "Spark options: `spark/bars_job.py`, `spark/passthrough_job.py`. Environment variables such as "
                "`TRIGGER_INTERVAL`, `SINK_MODE` and `MAX_OFFSETS_PER_TRIGGER` are set per step by `bench/runner.py`.",
            ]
        ).rstrip()
        + "\n"
    )


def main(argv: list[str] | None = None) -> int:
    """Write results/CONFIGURATION.md, or with --check compare it with a fresh render and exit 1 if it is
    out of date.
    """
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if results/CONFIGURATION.md is out of date")
    a = ap.parse_args(argv)
    text = render()
    if a.check:
        if not OUT.exists() or OUT.read_text() != text:
            print("results/CONFIGURATION.md is out of date: run `uv run python -m bench.config_report`")
            return 1
        return 0
    OUT.write_text(text)
    print(f"wrote {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
