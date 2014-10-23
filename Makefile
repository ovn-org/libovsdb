all: build test

GO_PACKAGES = $(shell find . -not \( -wholename ./Godeps -prune -o -wholename ./integration -prune -o -wholename ./.git -prune \) -name '*.go' -print0 | xargs -0n1 dirname | sort -u)

build:
	go build -v $(GO_PACKAGES)

test:
	go test -cover -v $(GO_PACKAGES)

test-full:
	go test -cover -v $(GO_PACKAGES) ./integration

