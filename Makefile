.PHONY: all
all: lint build test

.PHONY: prebuild
prebuild:
	@echo "+ $@"
	@mkdir -p bin
	@go build -v -o ./bin ./cmd/modelgen
	@[ -f example/play_with_ovs/ovs.ovsschema ] || curl -o example/play_with_ovs/ovs.ovsschema https://raw.githubusercontent.com/openvswitch/ovs/v2.15.0/vswitchd/vswitch.ovsschema
	@go generate -v ./...

.PHONY: build
build: prebuild
	@echo "+ $@"
	@go build -v ./...

.PHONY: test
test: build
	@echo "+ $@"
	@go test -race -coverprofile=profile.cov -v ./...

.PHONY: integration-test
integration-test:
	@echo "+ $@"
	@go test -race -count 1 -v ./test/ovs

.PHONY: bench
bench: install-deps
	@echo "+ $@"
	@go test -run=XXX -count=3 -bench=. ./... | tee bench.out
	@benchstat bench.out

.PHONY: install-deps
install-deps:
	@echo "+ $@"
	@golangci-lint --version
	@go install golang.org/x/perf/cmd/benchstat@latest

.PHONY: lint
lint: install-deps prebuild
	@echo "+ $@"
	@golangci-lint run
