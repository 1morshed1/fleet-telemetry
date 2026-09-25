# Fleet Telemetry — developer Makefile
#
# Requires: Go on PATH (this repo was built with go at /usr/local/go/bin),
# Docker + Docker Compose, and `buf` (go install github.com/bufbuild/buf/cmd/buf@latest)
# for regenerating protobuf code.

GO ?= go
COMPOSE ?= docker compose

# Simulator knobs (override on the command line: make simulate VEHICLES=5000)
VEHICLES ?= 1000
INGEST_ADDR ?= localhost:50051

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---- Code ----

.PHONY: proto
proto: ## Regenerate Go code from .proto files (needs buf)
	buf lint
	buf generate

.PHONY: tidy
tidy: ## Sync go.mod/go.sum
	$(GO) mod tidy

.PHONY: build
build: ## Build all service binaries into ./bin
	$(GO) build -o bin/simulator ./cmd/simulator
	$(GO) build -o bin/ingestion ./cmd/ingestion
	$(GO) build -o bin/processor ./cmd/processor
	$(GO) build -o bin/dashboard ./cmd/dashboard

.PHONY: test
test: ## Run unit tests
	$(GO) test ./...

.PHONY: vet
vet: ## Static analysis
	$(GO) vet ./...

## ---- Infra / run ----

.PHONY: up
up: ## Start infra + services via docker-compose
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop everything
	$(COMPOSE) down

.PHONY: infra
infra: ## Start only infrastructure (kafka, db, redis, prometheus, grafana)
	$(COMPOSE) up -d redpanda timescaledb redis prometheus grafana

.PHONY: logs
logs: ## Tail all service logs
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## Show running containers
	$(COMPOSE) ps

## ---- Simulator ----

.PHONY: simulate
simulate: ## Run the simulator locally (VEHICLES=n INGEST_ADDR=host:port)
	$(GO) run ./cmd/simulator -vehicles=$(VEHICLES) -addr=$(INGEST_ADDR)

## ---- Cleanup ----

.PHONY: clean
clean: ## Remove built binaries
	rm -rf bin/
