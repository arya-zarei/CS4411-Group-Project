# Real-Time Market-Data Pipeline with Apache Kafka and Spark Structured Streaming

Group 6, COMPSCI 4411/9538 (Databases 2): Arya Zarei, Rawad Alharastani, Mark Samwaiel.

A case study of a real-time market-data pipeline: a seeded trade generator publishes trades for 100 symbols to a 3-broker Kafka cluster, a PySpark Structured Streaming job computes 1-minute bars and 3-sigma alerts into PostgreSQL, and Grafana shows them live. Scripted experiments measure latency, throughput, crash recovery and the cost of exactly-once output, and reproduce two published results.

> **Status: foundation stage.** The application is built according to [PLAN.md](PLAN.md). Quickstart below is filled in as milestones land.

## Contents

| Path | What |
|---|---|
| [PLAN.md](PLAN.md) | Full build plan, requirements, milestones, task list |
| [CLAUDE.md](CLAUDE.md) | Conventions for Claude Code sessions |
| [docs/proposal/proposal.pdf](docs/proposal/proposal.pdf) | The confirmed project proposal |
| [data/sample/](data/sample/) | Golden dataset: trades plus expected bars and alerts |
| [tools/gen_sample_dataset.py](tools/gen_sample_dataset.py) | Reference generator and oracle |

## Quickstart (to be completed in milestone M2)

```bash
cp .env.example .env
docker compose up --build
# Grafana: http://localhost:3000
```

Requires Docker with about 10 to 12 GB of memory allocated.

## Reproducing the golden dataset

```bash
pip install numpy pandas
python tools/gen_sample_dataset.py --out data/sample --check   # prints MATCH
```

## License

To be chosen by the group (see PLAN.md decision D8).
