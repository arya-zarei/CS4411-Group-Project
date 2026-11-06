# CLAUDE.md

Instructions for Claude Code sessions in this repository.

## What this project is

Course project (COMPSCI 4411/9538, Databases 2), Group 6: a market-data pipeline built on Apache Kafka (3 brokers, KRaft) and Spark Structured Streaming (PySpark), writing 1-minute bars and alerts to PostgreSQL, shown in Grafana. It is also a benchmark and fault-injection study that answers three research questions.

## Read first, in this order

1. `docs/proposal/proposal.pdf`: the confirmed proposal. It is the authority.
2. `PLAN.md`: requirements, architecture, milestones, task list (section 9), open decisions (section 13).
3. `docs/decisions.md`: every decision and deviation so far (create it if missing).

## Rules

- **Never change a value the proposal fixes** without asking the user: 100 symbols, 1-minute bars (open, high, low, close, VWAP), alert = more than 3 standard deviations from the average over the previous 30 bars, 3 Kafka brokers with replication factor 3 and min ISR 2, one `docker compose up`, PySpark, PostgreSQL, Grafana, three runs per setting, median/p95/p99 latency, one 16 GB machine.
- **The oracle wins.** `tools/gen_sample_dataset.py` defines the trade model, the bars and the alert rule. `verify/reference.py` is a copy of its logic. If one changes, change both in the same PR and regenerate `data/sample/` (`python tools/gen_sample_dataset.py --out data/sample`), and update `MANIFEST.json`.
- **Do not guess versions, image tags, option names or API details.** Check current official documentation (Kafka, Spark, Postgres, Grafana, Coinbase) before using them. The proposal names Kafka 4.4, Spark 4.2, PostgreSQL 18 and Grafana 13. If a tag does not exist, propose the nearest one and ask.
- **Correctness before speed.** A benchmark number is worthless if `make verify` does not pass for that run.
- **Determinism.** Seeded generator, fixed virtual start time, `__FLUSH__` trade to close the last windows. Do not add wall-clock dependence to correctness paths.
- **No secrets, no course handouts, no rubric files** in the repo. It is public.
- Do not commit run outputs (`runs/`, `results/raw/`) or files over 5 MB.

## Working style

- Small commits, one branch per task (`t12-alerts`), one pull request per task, task IDs (T00 to T42) in titles.
- Before claiming a task done, demonstrate its "Done when" criterion from `PLAN.md` section 9 by running it, and paste the command and output summary in the PR.
- When you deviate from `PLAN.md`, say so in the PR and add a line to `docs/decisions.md`.
- Keep memory in mind: the target machine has 16 GB total. Do not raise heap sizes or add services without checking the budget in `PLAN.md` section 7.2.
- Python: one pinned version, lock file committed, `ruff` for lint and format, `pytest` for tests.
- This machine has Docker; run things rather than reasoning about whether they would work.

## Common commands (create them as the Makefile grows)

```
make up          # docker compose up --build -d
make down
make reset       # remove containers and volumes
make logs
make test        # unit tests, including oracle versus data/sample
make golden      # replay data/sample through Kafka and Spark, then verify
make verify
make demo
make bench-<name>   # e2 | e3 | e4 | e1a | e1b | e1c
```

## Attribution

If you create commits or pull requests, follow the attribution lines supplied by your environment.
