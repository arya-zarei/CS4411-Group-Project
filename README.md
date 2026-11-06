# Real-Time Market-Data Pipeline with Apache Kafka and Spark Structured Streaming

Group 6, COMPSCI 4411/9538 (Databases 2): Arya Zarei, Rawad Alharastani, Mark Samwaiel.

A case study of a real-time market-data pipeline: a seeded trade generator publishes trades for 100 symbols to a 3-broker Kafka cluster, a PySpark Structured Streaming job computes 1-minute bars and 3-sigma alerts into PostgreSQL, and Grafana shows them live. Scripted experiments measure latency, throughput, crash recovery and the cost of exactly-once output, and reproduce two published results.

> **Status: milestone M0 (foundation) done.** Kafka (3 brokers), PostgreSQL and Grafana start healthy with one command, and a minimal Spark job reads the `trades` topic. The generator, bars job, alerts and dashboard come in M1 and M2. See [PLAN.md](PLAN.md).

## Contents

| Path | What |
|---|---|
| [PLAN.md](PLAN.md) | Full build plan, requirements, milestones, task list |
| [CLAUDE.md](CLAUDE.md) | Conventions for Claude Code sessions |
| [docs/proposal/proposal.pdf](docs/proposal/proposal.pdf) | The confirmed project proposal |
| [data/sample/](data/sample/) | Golden dataset: trades plus expected bars and alerts |
| [tools/gen_sample_dataset.py](tools/gen_sample_dataset.py) | Reference generator and oracle |

## Quickstart

Requires Docker with about 10 to 12 GB of memory allocated (use `make up LITE=1` on smaller machines), and [uv](https://docs.astral.sh/uv/) for the Python tooling.

```bash
make up          # build and start Kafka x3, topics, PostgreSQL, Grafana; waits until healthy
make topics      # show the topics: replication factor 3, min ISR 2
make test        # unit tests (oracle versus the golden dataset, config checks)
make smoke       # minimal Spark job that prints records from the trades topic
make reset       # stop everything and delete all volumes
make help        # all targets
```

Grafana: http://localhost:3000 (viewer without login, admin/admin to edit). PostgreSQL: `localhost:5432`, user `pipeline`, database `market`. Kafka from the host: `localhost:29092`, `29093`, `29094`.

Image versions are pinned in `docker-compose.yml` (same values as `.env.example`). Copy `.env.example` to `.env` only to override something.

**Behind a TLS-inspecting proxy** (some university or company networks), the Spark image build cannot download packages. Pass the proxy's CA certificate: `EXTRA_CA_FILE=$PWD/.local/proxy-ca.pem make smoke` (the file must be inside the project folder; `.local/` is git-ignored).

## Reproducing the golden dataset

```bash
uv sync
uv run python tools/gen_sample_dataset.py --out data/sample --check   # prints MATCH
```

## License

To be chosen by the group (see PLAN.md decision D8).
