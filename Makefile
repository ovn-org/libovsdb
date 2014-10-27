all: build test

build:
	go build -v github.com/socketplane/libovsdb

test:
	go test -covermode=count -coverprofile=coverage.out -test.short -v github.com/socketplane/libovsdb

test-all:
	go test -covermode=count -coverprofile=coverage.out -v github.com/socketplane/libovsdb

