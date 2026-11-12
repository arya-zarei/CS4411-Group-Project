# Contributing

## Workflow

1. One branch per task, named after its issue: `t30-e2-runs`, `t41-runbook`.
2. Small commits. Open a pull request to `main` with the task id in the title (`T30: ...`).
3. Paste the task's *Done when* evidence (command and the end of its output) into the pull request.
4. CI must be green (ruff, tests including PostgreSQL integration tests, compose validation).
5. Deviation from `PLAN.md` or the proposal? Say so in the pull request and add a line to
   `docs/decisions.md`. Never change a value the proposal fixes without the group agreeing.

## Before pushing

```bash
make lint        # ruff check and format check
make test        # unit and integration tests (start PostgreSQL first: docker compose up -d --wait postgres)
make golden      # if you touched the generator, the Spark job, the sink or the verifier
```

## Rules that keep results valid

- `tools/gen_sample_dataset.py` is the oracle. `verify/reference.py` and `spark/alert_rule.py` must
  agree with it; tests enforce that. Change all of them together and regenerate `data/sample/`.
- The application code is copied into the Docker images: rebuild (`make build`) after changing it.
- Benchmarks: one machine for all runs, one experiment at a time, `make bench-<name>`; never edit
  files under `results/raw/` by hand. Regenerate summaries with `uv run python -m bench.summarize`.
- The repository is public: no secrets, no course handouts or rubrics, no files over 5 MB.

More: [docs/runbook.md](docs/runbook.md), [CLAUDE.md](CLAUDE.md).
