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

GO             ?= go
GOFLAGS        ?= -trimpath
GOLINT         ?= golangci-lint
DOCKER         ?= docker
DOCKER_CONTEXT ?= .
IMAGE          ?= $(BINARY)
IMAGE_TAG      ?= $(VERSION)

.DEFAULT_GOAL := help

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development

.PHONY: deps
deps: ## Download module dependencies
	$(GO) mod download

.PHONY: tidy
tidy: ## Tidy module dependencies
	$(GO) mod tidy

.PHONY: build
build: ## Build the binary (default: ./bin/)
	mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)

.PHONY: install
install: ## Install the binary with go install
	$(GO) install $(GOFLAGS) -ldflags '$(LDFLAGS)' $(CMD_PKG)

.PHONY: run
run: build ## Build then run with config.yaml
	./$(BIN_DIR)/$(BINARY) --config config.yaml

##@ Quality

.PHONY: fmt
fmt: ## Format source files
	$(GO) fmt $(PKG)
	$(GO) run golang.org/x/tools/cmd/goimports@latest -w -local github.com/hatamiarash7/redis-watcher .

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKG)

.PHONY: lint
lint: ## Run golangci-lint
	$(GOLINT) run ./...

.PHONY: test
test: ## Run unit tests with race detector
	$(GO) test -race -count=1 -timeout=60s $(PKG)

.PHONY: test-cover
test-cover: ## Run unit tests with coverage profile
	$(GO) test -race -count=1 -timeout=60s -covermode=atomic -coverprofile=$(COVER_PROFILE) $(PKG)
	$(GO) tool cover -func=$(COVER_PROFILE) | tail -n 1

.PHONY: test-integration
test-integration: ## Run integration tests (requires running Redis)
	$(GO) test -race -count=1 -tags=integration -timeout=2m ./test/integration/...

.PHONY: check
check: vet lint test ## Run local quality checks

.PHONY: ci
ci: check test-integration ## Run static checks and unit/integration tests

##@ Docker

.PHONY: docker
docker: ## Build the Docker image
	$(DOCKER) build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(IMAGE_TAG) -t $(IMAGE):latest $(DOCKER_CONTEXT)

.PHONY: docker-build
docker-build: docker ## Build the Docker image

.PHONY: docker-compose-up
docker-compose-up: ## Start the Docker Compose stack (Redis + watcher)
	mkdir -p ./redis-socket
	chmod 777 ./redis-socket
	$(DOCKER) compose up -d

.PHONY: docker-compose-down
docker-compose-down: ## Stop the Docker Compose stack and remove volumes
	$(DOCKER) compose down -v

##@ Cleanup

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) $(COVER_PROFILE) coverage.html
