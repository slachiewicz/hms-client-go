SHELL := /bin/bash

.PHONY: all build test test-short test-docker lint vulncheck check gen clean

all: check build

build:
	go build -v ./...

test:
	go test -v -race ./...

test-short:
	go test -short -race ./...

test-docker:
	go test -race -tags integration ./test/...

lint:
	golangci-lint run ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

check:
	@echo "==> gofmt"
	@test -z "$$(gofmt -l .)" || (echo "gofmt found unformatted files:" && gofmt -l . && exit 1)
	@echo "==> go vet"
	@go vet ./...
	@echo "==> go test -short"
	@go test -short -race ./...
	@echo "==> golangci-lint"
	@golangci-lint run ./...
	@echo "==> govulncheck"
	@$(MAKE) --no-print-directory vulncheck
	@echo "✓ All checks passed"

gen:
	@./scripts/gen-thrift.sh

clean:
	go clean
