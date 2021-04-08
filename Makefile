.PHONY: all test test-local install-deps lint fmt

all: test

test-local: install-deps fmt lint
	@echo "+ $@"
	@go test -race -v ./...

test:
	@docker-compose run --rm test

install-deps:
	@echo "+ $@"
	@golangci-lint --version

lint:
	@echo "+ $@"
	@golangci-lint run

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)"

