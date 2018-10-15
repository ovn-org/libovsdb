.DEFAULT_GOAL := all

NET := libovsdbnet
SUBNET := 172.16.0.0/24

# GO
GO_IMAGE := golang:1.9
GO_CONTAINER := golang
GO_IP := 172.16.0.3

# OVSDB
OVSDB_IMAGE := socketplane/openvswitch:2.4.0
OVSDB_CONTAINER := ovsdb
OVSDB_IP := 172.16.0.4

remove-network:
	-@docker network rm $(NET)
add-network: remove-network
	-@docker network create --subnet=$(SUBNET) $(NET)

ovsdb-clean-container:
	-@docker rm -f $(OVSDB_CONTAINER)
ovsdb-clean: ovsdb-clean-container
	-@docker rmi $(OVSDB_IMAGE)
ovsdb-build:
	@docker pull $(OVSDB_IMAGE)
ovsdb-run:
	@docker run \
		--privileged \
		--detach \
		--net $(NET) \
		--ip $(OVSDB_IP) \
		--name $(OVSDB_CONTAINER) \
		--publish 6640:6640 \
		$(OVSDB_IMAGE) 
ovsdb: ovsdb-clean ovsdb-run

go-clean-container:
	-@docker rm -f $(GO_CONTAINER)
go-clean: go-clean-container
	-@docker rmi $(GO_IMAGE)
go-build:
	@docker pull $(GO_IMAGE)
	@docker build . --tag $(GO_IMAGE)
go-test:
	@docker run \
		--detach \
		--net $(NET) \
		--ip $(GO_IP) \
		--name $(GO_CONTAINER) \
		$(GO_IMAGE) \
		sh -c cd /go/src/libovsdb && go test .
go-run: go-test go-clean-container ovsdb-clean-container
	
		
go: go-clean go-build go-run

clean: go-clean ovsdb-clean remove-network
all: add-network ovsdb go