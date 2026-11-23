#!/usr/bin/env bash
# Builds the submission zip: every file tracked in git at REF (default HEAD) except the Markdown files
# other than README.md.
# It has no .git folder, no .venv, no run outputs and no secrets (those are git-ignored), and runs
# from the unzipped folder exactly like a clone (README quickstart).
#
#   scripts/make_bundle.sh                 # dist/cs4411-group6-pipeline.zip
#   REF=v1.0 scripts/make_bundle.sh        # a tag or commit
#   WITH_RAW=1 scripts/make_bundle.sh      # also dist/cs4411-group6-raw-results.zip (every run's raw output)
#
# Check the result with scripts/bundle_check.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

REF="${REF:-HEAD}"
NAME=cs4411-group6-pipeline
mkdir -p dist
rm -f "dist/$NAME.zip"

if [[ -n "$(git status --porcelain)" && "$REF" == "HEAD" ]]; then
  echo "warning: uncommitted changes are NOT in the zip (it contains the committed $REF)" >&2
fi
git archive --format=zip --prefix="$NAME/" -o "dist/$NAME.zip" "$REF"

# The submission has one Markdown file, the README. Notes, plans and generated .md tables stay in the
# repository only (the report, report/main.tex, carries what a reader needs).
unzip -Z1 "dist/$NAME.zip" | grep -E '\.md$' | grep -v -x "$NAME/README.md" | xargs -d '\n' zip -q -d "dist/$NAME.zip"

# Guards: the repo is public, so no handouts or rubrics; the project says no file over 5 MB.
if unzip -Z1 "dist/$NAME.zip" | grep -i -E 'rubric|handout|\.local/|\.env$|\.pem$'; then
  echo "error: a file that must not be shared is in the zip" >&2; exit 1
fi
big=$(unzip -Z "dist/$NAME.zip" | awk '/^-/ && $4 > 5*1024*1024 {print $NF, $4}')
[[ -z "$big" ]] || { echo "error: file over 5 MB in the zip: $big" >&2; exit 1; }

echo "dist/$NAME.zip  $(du -h "dist/$NAME.zip" | cut -f1)  $(unzip -Z1 "dist/$NAME.zip" | grep -vc '/$') files  ($(git rev-parse --short "$REF"))"

if [[ "${WITH_RAW:-0}" == "1" ]]; then
  rm -f "dist/$NAME-raw-results.zip"
  zip -q -r "dist/$NAME-raw-results.zip" results/raw results/raw-dry -x '*.pyc'
  echo "dist/$NAME-raw-results.zip  $(du -h "dist/$NAME-raw-results.zip" | cut -f1)  (unzip into the project folder)"
fi
