#!/usr/bin/env bash
# Does the submission zip work as a reviewer would use it? Unzips it into an empty folder (no .git,
# nothing from the development machine) and follows the README: uv sync, lint, oracle check, tests,
# then `make golden` (builds the images, replays the golden dataset, verifies every bar and alert).
# It stops and removes this project's containers and volumes (make reset) first.
#
#   scripts/bundle_check.sh [dist/cs4411-group6-pipeline.zip]
#   QUICK=1 scripts/bundle_check.sh        # skip make golden (no Docker needed)
set -euo pipefail

ZIP="$(realpath "${1:-dist/cs4411-group6-pipeline.zip}")"
DIR="${DIR:-$(mktemp -d)}"
step() { printf '\n== [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

step "unzip $ZIP into $DIR"
unzip -q "$ZIP" -d "$DIR"
cd "$DIR"/*/
[[ ! -d .git ]] || { echo "the zip must not contain .git" >&2; exit 1; }
extra=$(find . -name '*.md' -not -path './.venv/*' -not -path './README.md' | head -3)
[[ -z "$extra" ]] || { echo "the zip must hold no Markdown but README.md: $extra" >&2; exit 1; }
for f in README.md LICENSE Makefile docker-compose.yml uv.lock data/sample/MANIFEST.json report/main.tex results/summary.csv; do
  [[ -f "$f" ]] || { echo "missing $f" >&2; exit 1; }
done

if [[ -n "${EXTRA_CA_FILE:-}" ]]; then   # behind a TLS-inspecting proxy (README): the CA must be inside the folder
  mkdir -p .local && cp "$EXTRA_CA_FILE" .local/proxy-ca.pem && export EXTRA_CA_FILE="$PWD/.local/proxy-ca.pem"
fi

step "README: uv sync"
uv sync --frozen --quiet

step "lint, oracle check, tests"
uv run --quiet ruff check . && uv run --quiet ruff format --check .
uv run --quiet python tools/gen_sample_dataset.py --out data/sample --check
docker compose --profile smoke --profile bench --profile live config -q
uv run --quiet pytest -q

if [[ "${QUICK:-0}" != "1" ]]; then
  step "make golden"
  make golden
fi

step "PASS: the zip works ($DIR)"
