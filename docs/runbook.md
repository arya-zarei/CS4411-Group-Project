# Runbook

How to run the pipeline, the checks and the experiments, and how to recover a broken stack.
Everything runs from the repository root. Python tools run on the host with `uv run`; the pipeline
runs in Docker. One-time setup: `uv sync`.

## Demo

| Command | What it does |
|---|---|
| `make up` | Everything, with the synthetic generator at 30x: alerts start after about 80 seconds. |
| `make demo-live` | Live Coinbase trades instead of the generator. Captures to `runs/captures/`; if the feed is silent for 30 s it replays the newest capture. |
| `make demo-offline` | No network: replays the newest capture in `runs/captures/`, re-stamped to now. |
| `make reset` | Stop everything and delete all data (Kafka, PostgreSQL, Spark checkpoints). |

Dashboard: http://localhost:3000 (no login). Spark UI: http://localhost:4040.

Demo crashes (show that output stays correct):

```bash
docker kill spark-bars && docker compose up -d spark-bars      # Spark crash, restart from checkpoint
docker kill kafka-2                                             # broker crash; writes continue (RF 3, min ISR 2)
docker compose up -d kafka-2                                    # bring it back
```

## Is the output correct?

| Command | Checks |
|---|---|
| `make golden` | Gate G1: replay `data/sample` through Kafka and Spark; bars and alerts must equal the expected files. |
| `scripts/live_check.sh` | A paced 60x stream; `KILL_SPARK_AT=15 KILL_BROKER_AT=35 scripts/live_check.sh` adds both crashes. |
| `uv run python -m verify.verify --trades FILE` | Bars and alerts in PostgreSQL versus the oracle on the trades the generator sent (VER-2 a, VER-3). |
| `uv run python -m verify.verify --from-topic` | The same versus what is actually in Kafka (VER-2 b); use for live data. |
| `uv run python -m verify.kafka_layer --acks runs/<run>/acks.csv` | Every acknowledged trade is in Kafka exactly once (VER-1). |

## Experiments

```bash
make bench-dry                              # every experiment, short runs, one repeat (about 20 minutes)
make bench-e2                               # one experiment, three runs per setting
make bench-all                              # everything, in priority order E2, E3, E4, E1a, E1b, E1c
make bench-e1c BENCH_ARGS="--only p6-1s"    # one setting
```

Settings are in `bench/experiments/<name>.yaml`. Results: `results/raw/<experiment>/<setting>/rep<k>/`
(`config.json`, `result.json`, verifier reports, generator ack log). A run that already has
`result.json` is skipped, so an interrupted `make bench-all` can simply be started again; a failed run
leaves `error.json` and is retried. Delete a run's folder to repeat it.

| Experiment | Question | Typical full-run time |
|---|---|---|
| E2 | Broker crash: acks=1 versus acks=all, lost and duplicated trades, write stall | about 5 min per run |
| E3 | Spark crash: recovery time and correctness (live and deterministic replay) | about 5 min per run |
| E4 | Exactly-once cost: insert versus upsert, duplicates after a crash, crash-free cost | 2 to 7 min per run |
| E1a | Producer batching (Kreps et al. Figure 4) | about 1 min per run |
| E1b | Pass-through latency, continuous versus micro-batch (Armbrust et al. Figure 7) | about 30 min per run |
| E1c | Bars pipeline: partitions and trigger interval versus throughput and latency | 30 to 60 min per run |

Rules for benchmark runs (PLAN.md 5.9): one machine for all runs, close other heavy programs, never
run two experiments at once, and give Docker 10 to 12 GB. Each run records its machine; the summary
lists which runs ran where, so a change of hardware cannot go unnoticed.

Turning raw runs into the committed results:

```bash
uv run python -m bench.summarize      # results/summary.csv, RESULTS.md, MACHINE.md, figures/*.png
```

Long runs on a machine that may restart: the runner is resumable, so start the same command again.
To keep a run going without a terminal, use `nohup make bench-all > bench.log 2>&1 &` and never start
a second copy while one runs (two experiments at once corrupt both).

## Reproducibility check (T40)

```bash
scripts/fresh_clone_check.sh               # clones the public repository and follows the README:
                                           # uv sync, lint, oracle check, tests, make golden
REF=v1.0 scripts/fresh_clone_check.sh      # the same for the tagged release
```

## Recovering a broken stack

| Symptom | Fix |
|---|---|
| Anything odd after a crash test | `make reset`, then start again. |
| Spark exits with `failOnDataLoss` / offsets errors | The topic was recreated but the checkpoint was not: `make reset` (the harness always wipes both together). |
| Dashboard empty | Is the generator or `coinbase` running (`make ps`)? Is the time range "last 15 minutes"? |
| `kafka-init` fails | A broker is not healthy yet: `make logs`, then `make up` again. |
| Image build cannot download packages | Behind a TLS-inspecting proxy: `EXTRA_CA_FILE=$PWD/.local/proxy-ca.pem make up` (README). |
| Docker Hub "429 Too Many Requests" while building | Anonymous pull limit: wait a few minutes or `docker login`. `BUILD=0 make golden` reuses built images. |
| Out of memory | `make up LITE=1` (smaller heaps; not for benchmark runs). |
