libovsdb
========

An OVSDB Library written in Go

## What is OVSDB?

OVSDB is the Open vSwitch Database Protocol.
It's defined in [RFC 7047](http://tools.ietf.org/html/rfc7047)
It's used mainly for managing the configuration of Open vSwitch, but it could also be used to manage your stamp collection. Philatelists Rejoice!

## Running the tests

To run integration tests, you'll need access to docker to run an Open vSwitch container.
Mac users can use [boot2docker](http://boot2docker.io)

```bash
    make
```
