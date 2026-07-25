# loafer-awsx build tooling.
#
# Module path: github.com/silviolleite/loafer-awsx (module lives at the
# repository root). All Go commands run in the current directory.

# Tooling
GO            := go

# Project settings
MODULE        := $(shell $(GO) list -m)
PKGS          := $(shell $(GO) list ./... | grep -Ev 'examples|fake')
GOPATH_BIN    := $(shell $(GO) env GOPATH)/bin

# Coverage artifacts
COVER_TMP     := cover.out.tmp
COVER_FINAL   := cover.out
COVER_TOTAL   := geral.out

# Integration test settings
COMPOSE_FILE  := docker-compose.integration.yml
LOCALSTACK_URL := http://localhost:4566

# Tool versions
GOLANGCI_LINT_VERSION := v2.12.2
LEFTHOOK_VERSION      := v1.4.8

.PHONY: all clean format lint test cover test-bench test-integration test-chaos configure check \
        check-vuln setup-dev \
        install-goimports install-golangci install-fieldalignment install-lefthook \
        install-mockery install-rapid install-govulncheck update-dependencies mocks

all: lint test

format:
	@echo "Formatting code..."
	@goimports -local $(MODULE) -w -l .
	@fieldalignment -fix ./... || true

lint: format
	@echo "Running linters..."
	@golangci-lint run --allow-parallel-runners --max-same-issues 0 --config .golangci.yml ./...

test: clean
	@echo "Running tests with race detection..."
	@$(GO) test -timeout 2m -race -covermode=atomic -coverprofile=$(COVER_TOTAL) $(PKGS)
	@$(GO) tool cover -func=$(COVER_TOTAL)

cover:
	@echo "Generating filtered test coverage..."
	@$(GO) test -covermode=count -coverprofile=$(COVER_TMP) ./...
	@grep -Ev 'examples|fake' $(COVER_TMP) > $(COVER_FINAL)
	@$(GO) tool cover -func=$(COVER_FINAL)

test-bench: clean
	@echo "Running benchmarks..."
	@$(GO) test -bench=. -benchtime=5s -count=1 -benchmem $(PKGS)

test-integration:
	@echo "Starting LocalStack..."
	@docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for LocalStack (SQS + SNS) to be ready..."
	@until curl -s $(LOCALSTACK_URL)/_localstack/health | grep -q '"sqs": "\(running\|available\)"'; do sleep 1; done
	@until curl -s $(LOCALSTACK_URL)/_localstack/health | grep -q '"sns": "\(running\|available\)"'; do sleep 1; done
	@$(GO) test -tags=integration -race -v ./consumer/... ./broker/... -run Integration \
		|| (docker compose -f $(COMPOSE_FILE) down -v && exit 1)
	@docker compose -f $(COMPOSE_FILE) down -v

clean:
	@echo "Cleaning test cache..."
	@$(GO) clean -testcache
	@rm -f $(COVER_TMP) $(COVER_FINAL) $(COVER_TOTAL)

mocks:
	@echo "Generating mocks..."
	@mockery

configure: install-goimports install-fieldalignment install-golangci install-lefthook install-mockery install-rapid install-govulncheck setup-dev
	@echo "Installing lefthook hooks..."
	@lefthook install

install-goimports:
	@echo "Installing goimports..."
	@$(GO) install golang.org/x/tools/cmd/goimports@latest

install-fieldalignment:
	@echo "Installing fieldalignment..."
	@$(GO) install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest

install-golangci:
	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(GOPATH_BIN) $(GOLANGCI_LINT_VERSION)

install-lefthook:
	@echo "Installing lefthook $(LEFTHOOK_VERSION)..."
	@$(GO) install github.com/evilmartians/lefthook@$(LEFTHOOK_VERSION)

install-mockery:
	@echo "Installing mockery..."
	@$(GO) install github.com/vektra/mockery/v3@latest

install-rapid:
	@echo "Installing rapid..."
	@$(GO) get pgregory.net/rapid

install-govulncheck:
	@echo "Installing govulncheck..."
	@$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

update-dependencies:
	@echo "Updating dependencies..."
	@$(GO) get -t -u ./...
	@$(GO) mod tidy

check: lint test check-vuln

check-vuln:
	@echo "Running govulncheck..."
	@govulncheck ./...

test-chaos: clean
	@echo "Running chaos/stress suite..."
	@GOMAXPROCS=1 $(GO) test ./... -race -count=30 -shuffle=on -timeout 15m

setup-dev:
	@echo "Installing npm/commitlint dev dependencies..."
	@npm install
	@echo "Registering git hooks via lefthook..."
	@lefthook install
