"""Guards for the infrastructure files: pins agree, and PLAN.md Kafka settings stay set."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = (ROOT / "docker-compose.yml").read_text()


def _env_example() -> dict[str, str]:
    out = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


@pytest.mark.parametrize(
    "var", ["KAFKA_IMAGE", "SPARK_IMAGE", "SPARK_KAFKA_CONNECTOR", "POSTGRES_IMAGE", "GRAFANA_IMAGE"]
)
def test_compose_default_equals_env_example(var):
    pinned = _env_example()[var]
    defaults = re.findall(r"\$\{" + var + r":-([^}]+)\}", COMPOSE)
    assert defaults, f"{var} has no default in docker-compose.yml"
    assert set(defaults) == {pinned}


def test_pins_have_exact_versions():
    for var in ("KAFKA_IMAGE", "SPARK_IMAGE", "POSTGRES_IMAGE", "GRAFANA_IMAGE"):
        tag = _env_example()[var].split(":", 1)[1]
        assert re.match(r"\d+\.\d+(\.\d+)?(-|$)", tag), f"{var} tag {tag!r} is not an exact version"
        assert tag != "latest"


def test_kafka_durability_settings():
    cfg = yaml.safe_load(COMPOSE)
    env = cfg["x-kafka-common"]["environment"]
    assert str(env["KAFKA_DEFAULT_REPLICATION_FACTOR"]) == "3"
    assert str(env["KAFKA_MIN_INSYNC_REPLICAS"]) == "2"
    assert str(env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"]) == "3"
    assert str(env["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"]) == "3"
    assert str(env["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"]) == "2"
    assert str(env["KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE"]).lower() == "false"


def test_three_brokers_with_healthchecks():
    services = yaml.safe_load(COMPOSE)["services"]
    for n in (1, 2, 3):
        assert f"kafka-{n}" in services
    for name in ("postgres", "grafana"):
        assert "healthcheck" in services[name]


def test_init_sql_defines_plan_tables():
    sql = (ROOT / "infra" / "postgres" / "init.sql").read_text()
    for table in ("bars", "alerts", "bars_plain", "alerts_plain", "query_progress"):
        assert re.search(rf"CREATE TABLE {table}\b", sql), table
    assert sql.count("PRIMARY KEY (symbol, window_start)") == 2


def test_pipeline_services_exist_and_checkpoint_survives_kills():
    services = yaml.safe_load(COMPOSE)["services"]
    assert "generator" in services and "spark-bars" in services
    assert any(v.startswith("spark-checkpoint:") for v in services["spark-bars"]["volumes"])
    assert "spark-checkpoint" in yaml.safe_load(COMPOSE)["volumes"]
    assert str(services["spark-bars"]["cpus"]) == "4"  # decision D2: local[4] with a 4-CPU limit


def test_spark_code_keeps_python_310_compatible_imports():
    # the official Spark image runs Python 3.10: no datetime.UTC (3.11+) in spark/
    for path in (ROOT / "spark").glob("*.py"):
        assert "from datetime import UTC" not in path.read_text(), path
