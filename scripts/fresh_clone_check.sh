#!/usr/bin/env bash
# T40: does a fresh clone work, following only the README? Run it on a second machine (or a clean
# folder) to check reproducibility. Ends with PASS (exit 0) or the first failing step.
#
#   scripts/fresh_clone_check.sh                     # clones https://github.com/arya-zarei/CS4411-Group-Project
#   REF=v1.0 scripts/fresh_clone_check.sh            # a tag or branch
#   REPO=/path/to/local/clone scripts/fresh_clone_check.sh
#
# It stops and removes this project's containers and volumes (make reset) before the golden run.
set -euo pipefail

REPO="${REPO:-https://github.com/arya-zarei/CS4411-Group-Project.git}"
REF="${REF:-main}"
DIR="${DIR:-$(mktemp -d)/clone}"

step() { printf '\n== [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

step "clone $REPO ($REF) into $DIR"
git clone --quiet --branch "$REF" "$REPO" "$DIR"
cd "$DIR"
git log --oneline -1

if [[ -n "${EXTRA_CA_FILE:-}" ]]; then   # behind a TLS-inspecting proxy (README): the CA must be inside the clone
  mkdir -p .local && cp "$EXTRA_CA_FILE" .local/proxy-ca.pem && export EXTRA_CA_FILE="$PWD/.local/proxy-ca.pem"
fi

step "README: uv sync"
uv sync --frozen --quiet

step "lint and tests"
uv run --quiet ruff check . && uv run --quiet ruff format --check .
uv run --quiet python tools/gen_sample_dataset.py --out data/sample --check
docker compose --profile smoke --profile bench --profile live config -q
uv run --quiet pytest -q

step "make golden (builds the images from this clone, replays the golden dataset, verifies)"
make golden

step "PASS: fresh clone of $REF works ($DIR)"
