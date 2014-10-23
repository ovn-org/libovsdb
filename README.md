libovsdb
========

An OVSDB Library written in Golang

## Running the tests

To run only unit tests:

    make test

To run integration tests

    fig up -d
    make test-all
    fig stop

## Dependency Management

We use godep for dependency management with the `-r` flag to rewrite import paths. This allows the repo to be `go get`able.

    godep save -r ./...
