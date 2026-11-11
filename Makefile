# Targets are added as the project grows (see PLAN.md INF-7). Run `make help`.
COMPOSE ?= docker compose
LITE    ?= 0
ifeq ($(LITE),1)
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.lite.yml
endif

.DEFAULT_GOAL := help
.PHONY: help up build down reset logs ps topics test lint fmt smoke golden verify requirements dashboard \
	demo-live demo-offline \
	bench-e1a bench-e1b bench-e1c bench-e2 bench-e3 bench-e4 bench-all bench-dry

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'
	@echo "  bench-<e1a|e1b|e1c|e2|e3|e4|all>  Run an experiment: 3 runs per setting, resumable (BENCH_ARGS=--dry-run)"
	@echo "  (add LITE=1 to use docker-compose.lite.yml on machines with less RAM)"

up:  ## Build and start everything (live demo: generator at 30x, Spark, Grafana); wait until healthy
	$(COMPOSE) up --build -d --wait

build:  ## Build the generator and Spark images
	$(COMPOSE) build generator spark-bars

down:  ## Stop containers, keep volumes
	$(COMPOSE) down

reset:  ## Stop containers and DELETE all volumes (Kafka data, PostgreSQL, Spark checkpoint)
	$(COMPOSE) --profile smoke --profile bench --profile live down -v --remove-orphans

logs:  ## Follow logs of all services
	$(COMPOSE) logs -f --tail=100

ps:  ## Show service status
	$(COMPOSE) ps -a

topics:  ## Describe Kafka topics (replication factor, min ISR, leaders)
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

test:  ## Unit tests, including the oracle versus data/sample
	uv run pytest -q

lint:  ## ruff lint and format check
	uv run ruff check . && uv run ruff format --check .

fmt:  ## ruff format
	uv run ruff format .

golden:  ## Gate G1: reset, replay data/sample through Kafka and Spark, verify (PASS or FAIL)
	COMPOSE="$(COMPOSE)" ./scripts/golden.sh

TRADES ?= data/sample/trades.csv.gz
verify:  ## Verify PostgreSQL bars and alerts against TRADES (default: the golden sample)
	uv run python -m verify.verify --trades $(TRADES) $(VERIFY_ARGS)

DEMO_SERVICES := kafka-1 kafka-2 kafka-3 postgres grafana spark-bars
demo-live:  ## Live demo on Coinbase trades (T27); captures to runs/captures/, replays one if the feed is silent
	$(COMPOSE) --profile live up --build -d --wait $(DEMO_SERVICES) coinbase

demo-offline:  ## Live demo without network: replay the newest capture in runs/captures/ (LIVE-4)
	$(COMPOSE) --profile live up --build -d --wait $(DEMO_SERVICES)
	$(COMPOSE) --profile live run -d --rm --no-deps --name coinbase-offline coinbase \
	  --mode=live-coinbase --offline --capture-dir=/runs/captures --acks=all

dashboard:  ## Regenerate infra/grafana/dashboards/pipeline.json from tools/make_dashboard.py
	uv run python tools/make_dashboard.py

# One experiment (or all of them, in PLAN.md 5.9 priority order): three runs per setting, resumable.
# Explicit targets, because make never applies pattern rules to .PHONY targets.
bench-e1a bench-e1b bench-e1c bench-e2 bench-e3 bench-e4 bench-all:
	uv run python -m bench.runner $(@:bench-%=%) $(BENCH_ARGS)

bench-dry:  ## Dry run of every experiment: short runs, one repeat, results/raw-dry/
	uv run python -m bench.runner all --dry-run $(BENCH_ARGS)

requirements:  ## Re-export infra/generator/requirements.txt from uv.lock
	uv export --frozen --no-dev --no-hashes --no-header --no-emit-project --format requirements-txt -o infra/generator/requirements.txt

smoke:  ## T04: minimal Spark job reads the trades topic and prints to the console
	$(COMPOSE) --profile smoke up --build spark-smoke
