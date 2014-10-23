libovsdb
========

[![Circle CI](https://circleci.com/gh/socketplane/libovsdb.png?style=badge&circle-token=17838d6362be941ed8478bf9d10de5307d4b917d)](https://circleci.com/gh/socketplane/libovsdb)

An OVSDB Library written in Golang

## Running the tests

To run only unit tests:

    make test

To run integration tests

    fig up -d
    make test-all
    fig stop

## Dependency Management

We use [godep](https://github.com/tools/godep) for dependency management with rewritten import paths.
This allows the repo to be `go get`able.

To bump the version of a dependency, follow these [instructions](https://github.com/tools/godep#update-a-dependency)
