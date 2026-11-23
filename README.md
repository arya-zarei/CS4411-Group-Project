# Kafka and Spark market-data pipeline

COMPSCI 4411/9538 (Databases 2), Group 6: Arya Zarei, Rawad Alharastani, Mark Samwaiel.

A trade generator publishes trades for 100 symbols to a 3-broker Kafka cluster. A PySpark Structured
Streaming job turns them into 1-minute bars (open, high, low, close, VWAP) and 3-sigma alerts, writes
both to PostgreSQL, and Grafana shows them live. A set of scripted experiments measures latency,
throughput, crash recovery and the cost of exactly-once output, and every run is checked against an
exact expected answer. The report is in `report/`.

## What you need

- Docker with Compose v2, and 10 to 12 GB of memory for Docker (use `make up LITE=1` if you have less)
- [uv](https://docs.astral.sh/uv/) (installs the pinned Python and packages)
- `make`

## Run it

```bash
uv sync          # Python tools, pinned in uv.lock
make up          # builds and starts Kafka x3, PostgreSQL, Grafana, Spark and the generator
```

Open http://localhost:3000 (Grafana, no login). The generator replays a synthetic day at 30 times real
speed, so the first bars appear after about a minute and the first alerts after about 80 seconds.
Other addresses: Spark UI http://localhost:4040, PostgreSQL `localhost:5432` (user `pipeline`, database
`market`), Kafka `localhost:29092` to `29094`.

```bash
make down        # stop, keep the data
make reset       # stop and delete all data and volumes
make help        # list every target
```

Behind a TLS-inspecting proxy, build with its CA certificate (the file must be inside this folder):
`EXTRA_CA_FILE=$PWD/.local/proxy-ca.pem make up`.

## Check that it is correct

```bash
make test        # unit tests, plus PostgreSQL integration tests when a database is running
make golden      # replays data/sample through Kafka and Spark, then compares every bar and alert
                 # with the expected answer; ends with PASS
```

`make golden` builds the images and takes a few minutes. To check the pipeline while a broker and Spark
are killed mid-run: `KILL_SPARK_AT=15 KILL_BROKER_AT=35 scripts/live_check.sh`.

## Run the experiments

```bash
make bench-dry   # a short run of every experiment, to check that they work
make bench-e2    # one experiment: e1a, e1b, e1c, e2, e3 or e4
make bench-all   # everything, three runs per setting, about 12 hours; safe to stop and resume
uv run python -m bench.summarize   # tables and figures from the raw runs
```

Raw output goes to `results/raw/<experiment>/<setting>/rep<k>/` (not in the repository). The committed
summary is `results/summary.csv`, and the figures are in `results/figures/`. Run the experiments with
nothing else using the machine.

## What is where

| Path | Contents |
|---|---|
| `docker-compose.yml`, `docker-compose.lite.yml` | The whole stack; the lite file lowers memory use |
| `generator/tradegen/` | Trade generator: seeded virtual mode, realtime mode, file replay, and a live Coinbase mode |
| `spark/` | `bars_job.py` (bars and alerts), `sink.py` (PostgreSQL upserts), `alert_rule.py`, `progress_listener.py`, `passthrough_job.py` (benchmarks) |
| `verify/` | `reference.py` (the expected bars and alerts), `verify.py` (compares a run with them), `kafka_layer.py` (checks the Kafka topic against what the producer sent) |
| `tools/` | `gen_sample_dataset.py` (defines the trade model, the bars and the alert rule), `make_dashboard.py` |
| `data/sample/` | Golden dataset: 60,163 trades, the expected 4,000 bars and 23 alerts, and a checksum manifest |
| `bench/` | Experiment runner, metric collectors, `summarize.py`; the experiments are `bench/experiments/*.yaml` |
| `infra/` | Kafka topic script, PostgreSQL schema, Grafana dashboard and data source, Dockerfiles |
| `scripts/` | `golden.sh`, `live_check.sh`, `fresh_clone_check.sh`, `make_bundle.sh`, `bundle_check.sh` |
| `tests/` | Unit and integration tests |
| `results/` | `summary.csv` and `figures/` |
| `report/` | The final report in LaTeX (`main.tex`), with its figures and bibliography |

## How it fits together

1. `tradegen` sends each trade to the Kafka topic `trades` (6 partitions, replication factor 3, minimum
   in-sync replicas 2), keyed by symbol, with `acks=all` and an idempotent producer.
2. `spark/bars_job.py` reads the topic, groups trades into 1-minute event-time windows per symbol with a
   10-second watermark, and writes each micro-batch to PostgreSQL in one transaction, using
   `INSERT ... ON CONFLICT DO UPDATE`, so a replayed batch after a crash changes nothing.
3. An alert fires when a bar's log return is more than 3 standard deviations from the mean of the
   previous 30 bars of that symbol (`spark/alert_rule.py`).
4. Grafana reads `bars`, `alerts` and `query_progress` from PostgreSQL.

`tools/gen_sample_dataset.py` is the reference for the trade model, the bars and the alert rule.
`verify/reference.py` and `spark/alert_rule.py` are copies of its logic, and the tests fail if they differ.
If you change the rule, change all three and regenerate the dataset:
`uv run python tools/gen_sample_dataset.py --out data/sample`.

## Live market data

`make demo-live` runs the pipeline on live Coinbase trades (needs internet access that allows
WebSockets). It saves a capture, and `make demo-offline` replays that capture with no network. After a
live run, `uv run python -m verify.verify --from-topic` checks the output against the topic's contents.

## Versions

Kafka 4.4.0, Spark 4.2.1 (PySpark), PostgreSQL 18.6, Grafana 13.3.0; Python 3.13 on the host. All pinned
in `docker-compose.yml` and `uv.lock`.

## License

MIT, see `LICENSE`.
