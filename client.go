package libovsdb

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2/jsonrpc"
)

type OvsdbClient struct {
	rpcClient *rpc2.Client
}

func Connect(ipAddr string, port int) (OvsdbClient, error) {
	target := fmt.Sprintf("%s:%d", os.Getenv("DOCKER_IP"), port)
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))

	go c.Run()
	return OvsdbClient{c}, nil
}

func (ovs OvsdbClient) Disconnect() {
	ovs.rpcClient.Close()
}

// RFC 7047 : list_dbs
func (ovs OvsdbClient) ListDbs() ([]interface{}, error) {
	var reply []interface{}
	err := ovs.rpcClient.Call("list_dbs", nil, &reply)
	if err != nil {
		log.Fatal("ListDbs failure", err)
	}
	return reply, err
}

// RFC 7047 : transact

func (ovs OvsdbClient) Transact(database string, operation Operation) ([]interface{}, error) {
	args := NewTransactArgs(database, operation)
	var reply []interface{}
	err := ovs.rpcClient.Call("transact", args, &reply)
	if err != nil {
		log.Fatal("transact failure", err)
	}
	return reply, err
}
