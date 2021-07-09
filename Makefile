.PHONY: all
all: lint build test integration-test coverage

.PHONY: prebuild
prebuild: 
	@echo "+ $@"
	@mkdir -p bin
	@go build -v -o ./bin ./cmd/modelgen
	@[ -f example/vswitchd/ovs.ovsschema ] || curl -o example/vswitchd/ovs.ovsschema https://raw.githubusercontent.com/openvswitch/ovs/v2.15.0/vswitchd/vswitch.ovsschema
	@go generate -v ./...

.PHONY: build
build: prebuild 
	@echo "+ $@"
	@go build -v ./...

.PHONY: test
test:
	@echo "+ $@"
	@go test -race -coverprofile=unit.cov -test.short -timeout 30s -v ./...

.PHONY: integration-test
integration-test:
	@echo "+ $@"
	@go test -race -coverprofile=integration.cov -coverpkg=github.com/ovn-org/libovsdb/... -timeout 60s -v ./test/ovs

.PHONY: coverage
coverage: test integration-test
	@sed -i '1d' integration.cov
	@cat unit.cov integration.cov > profile.cov

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
