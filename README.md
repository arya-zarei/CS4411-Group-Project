# Real-Time Market-Data Pipeline with Apache Kafka and Spark Structured Streaming

Group 6, COMPSCI 4411/9538 (Databases 2): Arya Zarei, Rawad Alharastani, Mark Samwaiel.

A seeded trade generator publishes trades for 100 symbols to a 3-broker Kafka cluster (KRaft, RF 3,
min ISR 2); a PySpark Structured Streaming job computes 1-minute bars (open, high, low, close, VWAP)
and 3-sigma alerts into PostgreSQL; Grafana shows them live. All of it starts with one
`docker compose up`. Scripted experiments measure latency, throughput, crash recovery and the cost of
exactly-once output, reproduce two published results (Kreps et al. 2011, Fig. 4; Armbrust et al.
2018, Fig. 7), and every run is checked bar by bar against an oracle.

> **Status: built, measured and verified (M0 to M4; the `v1.0` tag is waiting to be pushed).** `make golden` passes; all experiments ran
> three times per setting (`results/`). Open items that need the group: the live Coinbase run and a
> teammate's quickstart (T27, T28), a second-machine check (T40), a license (D8), and confirming the
> alert rule (D1). See [Status and open items](#status-and-open-items).

## Results at a glance

Medians over three runs, on one 4-vCPU, 16 GB machine. Full tables: [results/RESULTS.md](results/RESULTS.md);
every number: [results/summary.csv](results/summary.csv); machine and versions: [results/MACHINE.md](results/MACHINE.md).

| Question | Finding | Figure |
|---|---|---|
| Reproduction (a): producer batching | 19,798 records/s unbatched, 525,376 with 50-message batches (26x; the paper: about 8x). Shape reproduced. | [e1a](results/figures/e1a_producer_batching.png) |
| Reproduction (b): continuous vs micro-batch | Continuous: p50 43 to 83 ms up to 40,000 records/s, saturating at 80,000 (p50 1.8 s). Micro-batch sustained at least 100,000 records/s (our input limit) at p50 0.3 to 0.6 s. Shape reproduced. | [e1b](results/figures/e1b_continuous_vs_microbatch.png) |
| RQ1: partitions and trigger | 1 partition sustains about 75,000 records/s; 3 and 6 partitions at least 90,000 (our input limit). Bar latency p50 is 2 to 3.5 s; the best trigger depends on load: 500 ms at 30,000 records/s (1.7 s), 1 s at 60,000 (4.9 s), because shorter triggers only add overhead once a batch takes longer than the interval; 5 s is slowest (7 to 9 s). | [throughput](results/figures/e1c_sustained_throughput.png), [latency 30k](results/figures/e1c_bar_latency_30k.png), [60k](results/figures/e1c_bar_latency_60k.png) |
| RQ2: broker crash | No acknowledged trade lost in 12 leader kills. acks=all stalls every partition for about 9 s until the dead broker is fenced; acks=1 stalls only its partitions (about 10 s). | [e2](results/figures/e2_broker_crash.png) |
| RQ2: Spark crash | First new bar 12.8 s (replay) to 14.6 s (live) after a SIGKILL; output exact in all runs. | [e3](results/figures/e3_spark_recovery.png) |
| RQ3: exactly-once cost | A crash between the database commit and Spark's commit left 300 duplicate rows with plain inserts, 0 with upserts, every time; no measurable cost at 2,000 records/s. | [e4](results/figures/e4_exactly_once.png) |

## Quickstart

Requires Docker with 10 to 12 GB of memory (or `make up LITE=1`) and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/arya-zarei/CS4411-Group-Project.git && cd CS4411-Group-Project
uv sync          # Python tools (pinned in uv.lock)
make up          # Kafka x3, PostgreSQL, Grafana, Spark and the generator; then open http://localhost:3000
make golden      # replay the golden dataset through Kafka and Spark and verify every bar and alert: PASS
make help        # every target
```

`make up` runs the demo: the generator at 30x virtual speed (480 virtual minutes in 16 minutes) with
price shocks after the 30-bar warm-up, so alerts appear after about 80 seconds. The dashboard needs no
login. Other endpoints: Spark UI http://localhost:4040; PostgreSQL `localhost:5432` (user `pipeline`,
database `market`); Kafka `localhost:29092` to `29094`. `make reset` stops everything and deletes all
data. Behind a TLS-inspecting proxy, pass its CA: `EXTRA_CA_FILE=$PWD/.local/proxy-ca.pem make up`
(the file must be inside the project folder).

## Reproducing everything

| Goal | Command |
|---|---|
| The golden dataset | `uv run python tools/gen_sample_dataset.py --out data/sample --check` prints `MATCH` |
| Whole-pipeline correctness | `make golden`; with crashes: `KILL_SPARK_AT=15 KILL_BROKER_AT=35 scripts/live_check.sh` |
| All experiments (about 12 hours, resumable) | `make bench-all`; or one: `make bench-e2`; quick check: `make bench-dry` |
| Tables and figures from the raw runs | `uv run python -m bench.summarize` |
| A fresh clone, end to end | `scripts/fresh_clone_check.sh` |
| Tests | `make test` (unit and PostgreSQL integration tests; CI runs them on every push) |

Every run writes `results/raw/<experiment>/<setting>/rep<k>/` with its configuration, versions, git
commit and machine (git-ignored; the summaries are committed). Details: [docs/runbook.md](docs/runbook.md).

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | How Kafka and Spark Structured Streaming work and integrate; failure behaviour with evidence; design trade-offs |
| [results/RESULTS.md](results/RESULTS.md) | Every measured result |
| [docs/decisions.md](docs/decisions.md) | Every decision, deviation and finding, dated, with reasons |
| [docs/runbook.md](docs/runbook.md) | Running the demo, the checks and each experiment; recovering a broken stack |
| [docs/report/report-draft.md](docs/report/report-draft.md) | Draft of the final report, mapped to the evaluation rubric (to port to the course's LaTeX template) |
| [docs/presentation.md](docs/presentation.md) | 15-minute presentation plan and demo script |
| [PLAN.md](PLAN.md) | Requirements, milestones and tasks |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Workflow and the checks to run before pushing |
| [docs/proposal/proposal.pdf](docs/proposal/proposal.pdf) | The approved proposal |

## Repository

| Path | What |
|---|---|
| [generator/tradegen/](generator/tradegen/) | Trade generator: virtual, realtime, replay and live Coinbase modes; per-trade acknowledgement log |
| [spark/](spark/) | Bars and alerts job, PostgreSQL sink, progress listener, pass-through job |
| [verify/](verify/) | Oracle (`reference.py`), bar and alert verifier, Kafka-layer verifier |
| [bench/](bench/) | Experiment harness (`experiments/*.yaml`), collectors, summarizer |
| [scripts/](scripts/) | `golden.sh`, `live_check.sh`, `fresh_clone_check.sh` |
| [infra/](infra/) | Kafka topics, PostgreSQL schema, Grafana dashboard and data source, Dockerfiles |
| [data/sample/](data/sample/) | Golden dataset (60,163 trades) with expected bars, alerts and a SHA-256 manifest |
| [tools/](tools/) | The oracle and dataset generator (`gen_sample_dataset.py`), dashboard generator |
| [results/](results/) | Summary, figures, machine record |

## Milestones

| | Milestone | Gate | Status |
|---|---|---|---|
| ✅ | **M0 Foundation** | G0: all containers healthy, topics exist, `make test` passes | Done Nov 6 |
| ✅ | **M1 End-to-end bars** | G1: `make golden` passes from a clean `make reset` | Done Nov 6 |
| ✅ | **M2 Output and measurement** | G2: every experiment has a working dry run | Done Nov 11 (T27 live run and T28 teammate check open) |
| ✅ | **M3 Experiments** | G3: `results/summary.csv` and figures committed | Done Nov 12 |
| 🟡 | **M4 Freeze** | G4: `v1.0` tag pushed | Fresh clone and demo rehearsal (with crashes) pass. Open: push the `v1.0` tag (a maintainer; see below), T40 on a second machine |

## Status and open items

These need a person or a different machine; everything else is done and tested.

| Item | Why it is open | How to close it |
|---|---|---|
| T27 / S5: 10 minutes on live Coinbase trades | The development environment cannot open WebSockets; the adapter is tested against a protocol-faithful fake feed and offline from a capture | `make demo-live` for 10 minutes, then `uv run python -m verify.verify --from-topic` |
| T34: capture a live session for the demo fallback | Same | `make demo-live` writes `runs/captures/coinbase-live.jsonl` |
| T28 / T40: quickstart and fresh clone on a teammate's machine | Needs someone who did not write the code, and a second machine | `scripts/fresh_clone_check.sh`, then comment on the issue |
| D1: the alert's "price change" = log return of the close | Group decision | Confirm in the group chat |
| D8: license | Group decision | Add a `LICENSE` file (MIT is the usual choice) |
| Tag `v1.0` (gate G4) | The development environment cannot push tags | `git fetch && git tag -a v1.0 -m "v1.0" origin/main && git push origin v1.0`, or GitHub, Releases, Draft a new release |

Known limitations (also in the report): one machine, so no network partitions and no multi-node
Spark; synthetic data for every measurement; security features described, not measured; virtual time
compression magnifies failover skew relative to the watermark (docs/decisions.md, 2026-11-12); the
benchmark VM changed CPU between experiments, and results are only compared within one machine
(results/MACHINE.md).
