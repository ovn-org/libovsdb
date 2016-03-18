all: build test

godep:
	go get github.com/tools/godep

build: godep
	godep go build -v

test: godep
	godep go test -covermode=count -coverprofile=coverage.out -test.short -v

test-all: godep
	godep go test -covermode=count -coverprofile=coverage.out -v

