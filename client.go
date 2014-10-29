package libovsdb

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2/jsonrpc"
)

type OvsdbClient struct {
	rpcClient *rpc2.Client
	Schema    map[string]DatabaseSchema
}

func Connect(ipAddr string, port int) (OvsdbClient, error) {
	target := fmt.Sprintf("%s:%d", os.Getenv("DOCKER_IP"), port)
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))

	go c.Run()
	ovs := OvsdbClient{c, make(map[string]DatabaseSchema)}
	dbs, err := ovs.ListDbs()
	if err == nil {
		for _, db := range dbs {
			schema, err := ovs.GetSchema(db)
			if err == nil {
				ovs.Schema[db] = *schema
			}
		}
	}
	return ovs, err
}

func (ovs OvsdbClient) Disconnect() {
	ovs.rpcClient.Close()
}

// RFC 7047 : get_schema
func (ovs OvsdbClient) GetSchema(dbName string) (*DatabaseSchema, error) {
	args := NewGetSchemaArgs(dbName)
	var reply DatabaseSchema
	err := ovs.rpcClient.Call("get_schema", args, &reply)
	if err != nil {
		return nil, err
	} else {
		ovs.Schema[dbName] = reply
	}
	return &reply, err
}

// RFC 7047 : list_dbs
func (ovs OvsdbClient) ListDbs() ([]string, error) {
	var dbs []string
	err := ovs.rpcClient.Call("list_dbs", nil, &dbs)
	if err != nil {
		log.Fatal("ListDbs failure", err)
	}
	return dbs, err
}

// RFC 7047 : transact

func (ovs OvsdbClient) Transact(database string, operation ...Operation) ([]interface{}, error) {
	var reply []interface{}
	db, ok := ovs.Schema[database]
	if !ok {
		return nil, errors.New("invalid Database Schema")
	}

	if ok := db.validateOperations(operation...); !ok {
		return nil, errors.New("Validation failed for the operation")
	}

	args := NewTransactArgs(database, operation...)
	err := ovs.rpcClient.Call("transact", args, &reply)
	if err != nil {
		log.Fatal("transact failure", err)
	}
	return reply, err
}
