# ===== Redis Watcher Makefile =================================================

BINARY        ?= redis-watcher
PKG           ?= ./...
CMD_PKG       ?= ./cmd/redis-watcher
BIN_DIR       ?= bin
COVER_PROFILE ?= coverage.out

VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
  -X main.version=$(VERSION) \
  -X main.commit=$(COMMIT) \
  -X main.date=$(BUILD_DATE)

GO        ?= go
GOFLAGS   ?= -trimpath
GOLINT    ?= golangci-lint
DOCKER    ?= docker

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN{FS=":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	     /^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: tidy
tidy: ## Run go mod tidy.
	$(GO) mod tidy

.PHONY: deps
deps: ## Download module dependencies.
	$(GO) mod download

.PHONY: fmt
fmt: ## Format source files.
	$(GO) fmt $(PKG)
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w -local github.com/hatamiarash7/redis-watcher .

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet $(PKG)

.PHONY: lint
lint: ## Run golangci-lint.
	$(GOLINT) run ./...

.PHONY: test
test: ## Run unit tests with race detector.
	$(GO) test -race -count=1 -timeout=60s $(PKG)

.PHONY: test-cover
test-cover: ## Run unit tests with coverage profile.
	$(GO) test -race -count=1 -timeout=60s -covermode=atomic -coverprofile=$(COVER_PROFILE) $(PKG)
	$(GO) tool cover -func=$(COVER_PROFILE) | tail -n 1

.PHONY: test-integration
test-integration: ## Run integration tests (requires running Redis).
	$(GO) test -race -count=1 -tags=integration -timeout=2m ./test/integration/...

.PHONY: build
build: ## Build the binary into ./bin/.
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)

.PHONY: install
install: ## Install the binary into GOPATH.
	$(GO) install $(GOFLAGS) -ldflags '$(LDFLAGS)' $(CMD_PKG)

.PHONY: run
run: build ## Build then run with config.yaml.
	./$(BIN_DIR)/$(BINARY) --config config.yaml

.PHONY: docker
docker: ## Build the Docker image.
	$(DOCKER) build \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -t $(BINARY):$(VERSION) -t $(BINARY):latest .

.PHONY: docker-compose-up
docker-compose-up: ## Start the docker-compose stack (Redis + watcher).
	$(DOCKER) compose up -d --build

.PHONY: docker-compose-down
docker-compose-down: ## Stop the docker-compose stack.
	$(DOCKER) compose down -v

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR) $(COVER_PROFILE) coverage.html

.PHONY: ci
ci: vet lint test ## Run the checks executed in CI.
