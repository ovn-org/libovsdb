OVS_VERSION ?= v2.17.7
OVN_VERSION ?= v22.03.4

.PHONY: all
all: lint build test integration-test coverage

.PHONY: modelgen
modelgen:
	@mkdir -p bin
	@go build -v -o ./bin ./cmd/modelgen

.PHONY: prebuild
prebuild: modelgen ovsdb/serverdb/_server.ovsschema example/vswitchd/ovs.ovsschema example/vtep/vtep.ovsschema example/nb/ovn-nb.ovsschema example/sb/ovn-sb.ovsschema example/icnb/ovn-ic-nb.ovsschema example/icsb/ovn-ic-sb.ovsschema
	@echo "+ $@"
	@go generate -v ./...

.PHONY: build
build: prebuild
	@echo "+ $@"
	@go build -v ./...

.PHONY: test
test: prebuild
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
bench: install-deps prebuild
	@echo "+ $@"
	@go test -run=XXX -count=3 -bench=. ./... | tee bench.out
	@benchstat bench.out

.PHONY: install-deps
install-deps:
	@echo "+ $@"
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.49.0
	@golangci-lint --version
	@go install golang.org/x/perf/cmd/benchstat@latest

.PHONY: lint
lint: install-deps prebuild
	@echo "+ $@"
	@golangci-lint run

ovsdb/serverdb/_server.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/openvswitch/ovs/${OVS_VERSION}/ovsdb/_server.ovsschema -o $@

example/vswitchd/ovs.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/openvswitch/ovs/${OVS_VERSION}/vswitchd/vswitch.ovsschema -o $@

example/vtep/vtep.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/openvswitch/ovs/${OVS_VERSION}/vtep/vtep.ovsschema -o $@

example/nb/ovn-nb.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/ovn-org/ovn/${OVN_VERSION}/ovn-nb.ovsschema -o $@

example/sb/ovn-sb.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/ovn-org/ovn/${OVN_VERSION}/ovn-sb.ovsschema -o $@

example/icnb/ovn-ic-nb.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/ovn-org/ovn/${OVN_VERSION}/ovn-ic-nb.ovsschema -o $@

example/icsb/ovn-ic-sb.ovsschema:
	@curl -sSL https://raw.githubusercontent.com/ovn-org/ovn/${OVN_VERSION}/ovn-ic-sb.ovsschema -o $@
