# Makefile for the ThunderGo service. Common dev tasks.
#
# Usage:
#   make            — show help (default goal)
#   make build      — compile a static binary to ./bin/thundergo
#   make run        — build and run locally (requires .env)
#   make docker     — build the Docker image
#   make up         — docker compose up -d --build (mongo + thundergo)
#   make logs       — docker compose logs -f
#   make down       — docker compose down
#   make test       — go vet + run unit tests with race detector
#   make test-ci    — go vet + run tests 10x with race detector (for CI)
#   make vet        — go vet ./...
#   make lint       — run golangci-lint (requires golangci-lint)
#   make fmt        — run go fmt
#   make fmt-check  — check formatting without modifying
#   make gosec      — run gosec security scanner
#   make vulncheck  — run govulncheck
#   make coverage   — generate HTML coverage report (cover.html)
#   make size       — build and assert binary < 25 MB
#   make tidy       — go mod tidy
#   make clean      — remove build artifacts

GO ?= go
BINARY := bin/thundergo
IMAGE  := thundergo:latest
# F-008/F-018: inject version at link time. Falls back to "dev" when no git tag.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
# RA-F-004: pin the security-scanner tool versions instead of `@latest` so
# `make gosec` / `make vulncheck` produce reproducible results across machines
# and CI runs. Bump explicitly (with a CHANGELOG entry) when upgrading; do not
# silently track latest — a upstream release could introduce new false
# positives that break the PR gate without any code change in this repo.
GOSEC_VERSION      := v2.22.5
VULNCHECK_VERSION  := v1.5.0

# F-017: parallel recipe execution where safe.
MAKEFLAGS += -j$(shell nproc 2>/dev/null || echo 2)

# F-021: default goal is help so bare `make` is informative.
.DEFAULT_GOAL := help

.PHONY: build run docker up logs down test test-ci vet lint fmt fmt-check gosec vulncheck coverage size tidy clean help

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w -X github.com/fyaz05/ThunderGo/internal/http.Version=$(VERSION)' -o $(BINARY) ./cmd/thundergo

run: build
	./$(BINARY)

docker:
	docker build -t $(IMAGE) .

# F-016: match README (`docker compose up -d`), build images on the way up.
up:
	docker compose up -d --build

logs:
	docker compose logs -f

down:
	docker compose down

# F-017: run tests with race detector.
test:
	$(GO) vet ./...
	$(GO) test -race -count=1 ./...

# F-017: run tests 10x with race detector (for CI).
test-ci:
	$(GO) vet ./...
	$(GO) test -race -count=10 ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

fmt:
	$(GO) fmt ./...

# F-017: check formatting without modifying.
fmt-check:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run make fmt" && exit 1)

# F-017: run gosec. RA-F-004: pinned to GOSEC_VERSION for reproducibility.
gosec:
	$(GO) run github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) -quiet -severity medium -confidence medium ./...

# F-017: run govulncheck. RA-F-004: pinned to VULNCHECK_VERSION for reproducibility.
vulncheck:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(VULNCHECK_VERSION) ./...

# F-017: coverage report.
coverage:
	$(GO) test -race -covermode=atomic -coverprofile=cover.out ./...
	$(GO) tool cover -html=cover.out -o cover.html

# F-017: binary size check (assert < 25 MB).
size: build
	@SIZE=$$(du -m $(BINARY) | cut -f1); \
	if [ $$SIZE -gt 25 ]; then echo "FAIL: binary is $${SIZE}MB (max 25MB)"; exit 1; \
	else echo "OK: binary is $${SIZE}MB"; fi

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin/ cover.out cover.html thundergo.log bot_*.session

help:
	@echo "ThunderGo make targets:"
	@echo "  build      — compile a static binary (./bin/thundergo), version injected via -ldflags"
	@echo "  run        — build and run locally (requires .env)"
	@echo "  docker     — build Docker image"
	@echo "  up         — docker compose up -d --build"
	@echo "  logs       — docker compose logs -f"
	@echo "  down       — docker compose down"
	@echo "  test       — go vet + go test -race -count=1"
	@echo "  test-ci    — go vet + go test -race -count=10 (for CI)"
	@echo "  vet        — go vet ./..."
	@echo "  lint       — run golangci-lint"
	@echo "  fmt        — run go fmt"
	@echo "  fmt-check  — check formatting without modifying"
	@echo "  gosec      — run gosec security scanner"
	@echo "  vulncheck  — run govulncheck"
	@echo "  coverage   — generate HTML coverage report (cover.html)"
	@echo "  size       — build and assert binary < 25 MB"
	@echo "  tidy       — go mod tidy"
	@echo "  clean      — remove build artifacts"
	@echo "  help       — show this help (default)"
