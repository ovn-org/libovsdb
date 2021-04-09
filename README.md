libovsdb
========
[![Travis-CI](https://travis-ci.org/eBay/libovsdb.svg?branch=master)](https://travis-ci.org/eBay/libovsdb)

An OVSDB Library written in Go

## What is OVSDB?

OVSDB is the Open vSwitch Database Protocol.
It's defined in [RFC 7047](http://tools.ietf.org/html/rfc7047)
It's used mainly for managing the configuration of Open vSwitch, but it could also be used to manage your stamp collection. Philatelists Rejoice!

## Running the tests

To run integration tests, you'll need access to docker to run an Open vSwitch container.
Mac users can use [boot2docker](http://boot2docker.io)

    export DOCKER_IP=$(boot2docker ip)

    docker-compose run test /bin/sh
    # make test-local
    ...
    # exit
    docker-compose down

By invoking the command **make**, you will automatically get the same behaviour as what
is shown above. In other words, it will start the two containers and execute
**make test-local** from the test container.