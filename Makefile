# Targets are added as the project grows (see PLAN.md INF-7). Run `make help`.
COMPOSE ?= docker compose
LITE    ?= 0
ifeq ($(LITE),1)
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.lite.yml
endif

.DEFAULT_GOAL := help
.PHONY: help up down reset logs ps topics test lint fmt smoke

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'
	@echo "  (add LITE=1 to use docker-compose.lite.yml on machines with less RAM)"

up:  ## Build and start the stack, wait until healthy
	$(COMPOSE) up --build -d --wait

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

smoke:  ## T04: minimal Spark job reads the trades topic and prints to the console
	$(COMPOSE) --profile smoke up --build spark-smoke
