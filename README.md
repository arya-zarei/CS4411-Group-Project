# Real-Time Market-Data Pipeline with Apache Kafka and Spark Structured Streaming

Group 6, COMPSCI 4411/9538 (Databases 2): Arya Zarei, Rawad Alharastani, Mark Samwaiel.

A case study of a real-time market-data pipeline: a seeded trade generator publishes trades for 100 symbols to a 3-broker Kafka cluster, a PySpark Structured Streaming job computes 1-minute bars and 3-sigma alerts into PostgreSQL, and Grafana shows them live. Scripted experiments measure latency, throughput, crash recovery and the cost of exactly-once output, and reproduce two published results.

> **Status: milestones M0 and M1 done.** The full pipeline runs end to end: generator, 3-broker Kafka, Spark bars and alerts, PostgreSQL, verifier. `make golden` passes from a clean reset, and the output stays exactly correct through a Spark crash and a broker crash. The dashboard and the benchmark harness come in M2. See [PLAN.md](PLAN.md).

## Milestones

| | Milestone | Dates | Gate | Status |
|---|---|---|---|---|
| ✅ | **M0 Foundation** | Nov 6 to 8 | G0: all containers healthy, topics exist, `make test` passes | Done Nov 6 |
| ✅ | **M1 End-to-end bars** | Nov 9 to 14 | G1: `make golden` passes from a clean `make reset` | Done Nov 6 |
| ⬜ | M2 Output and measurement | Nov 15 to 21 | G2 (feature freeze): every experiment has a working one-minute dry run | |
| ⬜ | M3 Experiments | Nov 22 to 28 | G3: `results/summary.csv` and figures committed | |
| ⬜ | M4 Freeze | Nov 29 to 30 | G4: `v1.0` tag pushed | |

Tasks, with acceptance criteria, are GitHub issues (`T00` to `T42`); details in [PLAN.md](PLAN.md) section 9. Decisions and deviations: [docs/decisions.md](docs/decisions.md).

## Contents

| Path | What |
|---|---|
| [PLAN.md](PLAN.md) | Full build plan, requirements, milestones, task list |
| [CLAUDE.md](CLAUDE.md) | Conventions for Claude Code sessions |
| [docs/proposal/proposal.pdf](docs/proposal/proposal.pdf) | The confirmed project proposal |
| [docs/decisions.md](docs/decisions.md) | Every decision and deviation, with reasons |
| [generator/tradegen/](generator/tradegen/) | Trade generator: virtual, realtime and replay modes, per-trade ack log |
| [spark/](spark/) | Spark bars and alerts job, PostgreSQL sink, progress listener |
| [verify/](verify/) | Oracle (`reference.py`) and verifier (`verify.py`) |
| [scripts/](scripts/) | `golden.sh` (gate G1) and `live_check.sh` (streaming and fault checks) |
| [infra/](infra/) | Kafka topics, PostgreSQL schema, Grafana provisioning, Dockerfiles |
| [data/sample/](data/sample/) | Golden dataset: trades plus expected bars and alerts |
| [tools/gen_sample_dataset.py](tools/gen_sample_dataset.py) | Reference generator and oracle (source of truth) |

## Quickstart

Requires Docker with about 10 to 12 GB of memory allocated (use `make up LITE=1` on smaller machines), and [uv](https://docs.astral.sh/uv/) for the Python tooling (`uv sync` once).

```bash
make up          # build and start everything: Kafka x3, PostgreSQL, Grafana, Spark, and the generator
make golden      # gate G1: reset, replay the golden dataset through Kafka and Spark, verify (PASS/FAIL)
make test        # unit and integration tests (oracle, generator, alert rule, verifier, sink)
make reset       # stop everything and delete all volumes
make help        # all targets
```

`make up` starts the live demo. The generator runs in virtual mode at 30x (decision D3): 480 virtual minutes in 16 wall minutes, with price shocks after the 30-bar warm-up, so alerts start after about 80 seconds. Bars and alerts land in PostgreSQL (`bars`, `alerts`); `query_progress` has one row per Spark batch. The Grafana dashboard comes in M2 (T20).

Checking correctness of a run:

```bash
make golden                                          # golden dataset, ends with PASS or FAIL
scripts/live_check.sh                                # streaming run (60 virtual minutes at 60x), then verify
KILL_SPARK_AT=15 KILL_BROKER_AT=35 scripts/live_check.sh   # same, with a Spark crash and a broker crash
```

Endpoints: Grafana http://localhost:3000 (viewer without login, admin/admin to edit), Spark UI http://localhost:4040, PostgreSQL `localhost:5432` (user `pipeline`, database `market`), Kafka from the host `localhost:29092`, `29093`, `29094`.

Image versions are pinned in `docker-compose.yml` (same values as `.env.example`). Copy `.env.example` to `.env` only to override something. The application code is copied into the images, so rebuild after changing it (`make up` and `make golden` do).

**Behind a TLS-inspecting proxy** (some university or company networks), image builds cannot download packages. Pass the proxy's CA certificate: `EXTRA_CA_FILE=$PWD/.local/proxy-ca.pem make up` (the file must be inside the project folder; `.local/` is git-ignored).

## Reproducing the golden dataset

```bash
uv sync
uv run python tools/gen_sample_dataset.py --out data/sample --check   # prints MATCH
```

## License

To be chosen by the group (see PLAN.md decision D8).
