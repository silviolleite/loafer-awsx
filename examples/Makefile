# loafer-awsx examples tooling.
#
# Convenience targets for running the loafer-awsx examples against a local
# LocalStack instance provisioned with the Terraform config in ./terraform.
#
# All targets are meant to be run from this directory (examples/). The Go module
# lives at the repository root, so the example programs are run as relative
# packages, e.g. `go run ./basic`.

# Tooling
GO             := go
DOCKER_COMPOSE := docker compose

# Terraform lives in the terraform/ subdirectory and ships the docker-compose.yml
# used to start LocalStack.
TERRAFORM_DIR  := terraform
COMPOSE_FILE   := $(TERRAFORM_DIR)/docker-compose.yml

# LocalStack gateway used for the readiness health check (matches the defaults
# in terraform/docker-compose.yml and terraform/variables.tf).
LOCALSTACK_URL := http://localhost:4566

.PHONY: help up down down-clean provision destroy \
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

# --- LocalStack -------------------------------------------------------------

up: ## Start LocalStack and wait for SQS + SNS to be ready
	@echo "Starting LocalStack..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d
	@echo "Waiting for LocalStack (SQS + SNS) to be ready..."
	@until curl -s $(LOCALSTACK_URL)/_localstack/health | grep -q '"sqs": "\(running\|available\)"'; do sleep 1; done
	@until curl -s $(LOCALSTACK_URL)/_localstack/health | grep -q '"sns": "\(running\|available\)"'; do sleep 1; done
	@echo "LocalStack is ready at $(LOCALSTACK_URL)"

down: ## Stop LocalStack (keeps the data volume)
	@echo "Stopping LocalStack..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down

down-clean: ## Stop LocalStack and remove its data volume
	@echo "Stopping LocalStack and removing volumes..."
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v

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

run-localscheduler: ## Run the local EventBridge Scheduler simulator (LocalStack demo only; fires retry schedules)
	@echo "Running local scheduler simulator (Ctrl+C to stop; LocalStack demo only)..."
	@$(GO) run ./localscheduler

run-typed: ## Run the typed handler consumer example
	@echo "Running typed example (Ctrl+C to stop)..."
	@$(GO) run ./typed

run-middleware: ## Run the observability middleware consumer example (metrics on :9090)
	@echo "Running middleware example (Ctrl+C to stop; metrics on :9090/metrics)..."
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
