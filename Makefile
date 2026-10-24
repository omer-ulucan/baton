GOLANGCI_LINT_VERSION := v2.15.0
BIN := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint

.PHONY: all build vet test test-race lint check tools up down

all: check

build:
	go build -o $(BIN)/ ./cmd/...

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# What must pass before every commit.
check: vet lint test-race
	go build ./...

tools: $(GOLANGCI_LINT)

$(GOLANGCI_LINT):
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

up:
	docker compose up -d --build

down:
	docker compose down -v
