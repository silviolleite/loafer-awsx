# loafer-awsx examples tooling.
#
# Convenience targets for running the loafer-awsx examples against a local
# MiniStack instance provisioned with the Terraform config in ./terraform.
#
# All targets are meant to be run from this directory (examples/). The Go module
# lives at the repository root, so the example programs are run as relative
# packages, e.g. `go run ./basic`.

# Tooling
GO             := go
DOCKER_COMPOSE := docker compose

# Terraform lives in the terraform/ subdirectory and ships the docker-compose.yml
# used to start MiniStack.
TERRAFORM_DIR  := terraform
COMPOSE_FILE   := $(TERRAFORM_DIR)/docker-compose.yml

# MiniStack gateway, and how long `up` waits for its healthcheck (seconds).
MINISTACK_URL          := http://localhost:4566
MINISTACK_WAIT_TIMEOUT := 60

# Observability stack (OTel Collector + Jaeger) used by the middleware example.
OBSERVABILITY_COMPOSE_FILE := middleware/docker-compose.yml
JAEGER_UI                  := http://localhost:16686

.PHONY: help up down down-clean provision destroy \
        observability-up observability-down observability-down-clean \
        run-basic run-fifo run-fifo-scheduled-retry run-localscheduler run-typed run-middleware run-producer run-all

# help is the default goal: it parses the `##` comments after each target name
# and prints them as a self-documented target list (natsx/awsx style).
.DEFAULT_GOAL := help

help: ## Show this help
	@echo "loafer-awsx examples - available targets:"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Typical flow: make up && make provision && make run-basic"

# --- MiniStack --------------------------------------------------------------

up: ## Start MiniStack and wait until its healthcheck passes
	@echo "Starting MiniStack and waiting for its healthcheck..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --wait --wait-timeout $(MINISTACK_WAIT_TIMEOUT)
	@echo "MiniStack is ready at $(MINISTACK_URL)"

down: ## Stop MiniStack (keeps the persisted state volume)
	@echo "Stopping MiniStack..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down

down-clean: ## Stop MiniStack and remove its persisted state volume
	@echo "Stopping MiniStack and removing volumes..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v

# --- Observability (OTel Collector + Jaeger) --------------------------------
# Tracing backend for the middleware example. Independent of MiniStack; bring
# it up alongside `make up` when you want to view spans in the Jaeger UI.

observability-up: ## Start the OTel Collector + Jaeger tracing backend for the middleware example
	@echo "Starting OTel Collector + Jaeger..."
	@$(DOCKER_COMPOSE) -f $(OBSERVABILITY_COMPOSE_FILE) up -d
	@echo "Jaeger UI available at $(JAEGER_UI)"

observability-down: ## Stop the OTel Collector + Jaeger tracing backend
	@echo "Stopping OTel Collector + Jaeger..."
	@$(DOCKER_COMPOSE) -f $(OBSERVABILITY_COMPOSE_FILE) down

observability-down-clean: ## Stop the tracing backend and remove its volumes
	@echo "Stopping OTel Collector + Jaeger and removing volumes..."
	@$(DOCKER_COMPOSE) -f $(OBSERVABILITY_COMPOSE_FILE) down -v

# --- Terraform --------------------------------------------------------------

provision: ## Provision the SQS queues, SNS topics and subscriptions (terraform apply)
	@echo "Provisioning resources with Terraform..."
	@cd $(TERRAFORM_DIR) && terraform init -input=false
	@cd $(TERRAFORM_DIR) && terraform apply -auto-approve

destroy: ## Tear down the Terraform-managed resources (terraform destroy)
	@echo "Destroying Terraform-managed resources..."
	@cd $(TERRAFORM_DIR) && terraform destroy -auto-approve

# --- Examples ---------------------------------------------------------------
# Consumer examples block until interrupted (Ctrl+C); the producer runs once and
# exits.

run-basic: ## Run the basic standard-queue consumer example
	@echo "Running basic example (Ctrl+C to stop)..."
	@$(GO) run ./basic

run-fifo: ## Run the FIFO PerGroupID consumer example
	@echo "Running fifo example (Ctrl+C to stop)..."
	@$(GO) run ./fifo

run-fifo-scheduled-retry: ## Run the FIFO Scheduled Retry consumer example
	@echo "Running fifo-scheduled-retry example (Ctrl+C to stop)..."
	@$(GO) run ./fifo-scheduled-retry

run-localscheduler: ## Run the local EventBridge Scheduler simulator (local demo only; fires retry schedules)
	@echo "Running local scheduler simulator (Ctrl+C to stop; local demo only)..."
	@$(GO) run ./localscheduler

run-typed: ## Run the typed handler consumer example
	@echo "Running typed example (Ctrl+C to stop)..."
	@$(GO) run ./typed

run-middleware: ## Run the observability middleware consumer example (metrics on :9090, traces to Jaeger)
	@echo "Running middleware example (Ctrl+C to stop; metrics on :9090/metrics, traces at $(JAEGER_UI))..."
	@echo "Tip: run 'make observability-up' first to view spans in the Jaeger UI."
	@$(GO) run ./middleware

run-producer: ## Run the SNS producer example (publishes then exits)
	@echo "Running producer example..."
	@$(GO) run ./producer

# run-all publishes messages with the producer example. The consumer examples
# block until interrupted, so they cannot be chained sequentially; run-all
# instead exercises the publishing path end to end. Start a consumer (e.g.
# `make run-basic`) in another terminal first to observe delivery.
run-all: ## Run the producer example (helper; consumers block, run them individually)
	@echo "Running the producer example (consumers block, so run them individually)..."
	@$(MAKE) run-producer
