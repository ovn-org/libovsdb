FROM golang:1.9

COPY . /go/src/libovsdb
ENV DOCKER_IP="172.16.0.4"