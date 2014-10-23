package integration

import (
	"fmt"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2/jsonrpc"
	"github.com/socketplane/libovsdb/op"
	"log"
	"net"
	"os"
	"testing"
)

func TestListDbs(t *testing.T) {
	target := fmt.Sprintf("%s:6640", os.Getenv("DOCKER_IP"))
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))

	go c.Run()

	var reply []interface{}

	operation := op.Operation{}

	operation.Op = "insert"
	operation.Table = "Bridge"
	operation.Row = `{"name: "docker0"}`

	err = c.Call("list_dbs", nil, &reply)

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if reply[0] != "Open_vSwitch" {
		t.Error("Expected: 'Open_vSwitch', Got:", reply)
	}
}

func TestTransact(t *testing.T) {
	target := fmt.Sprintf("%s:6640", os.Getenv("DOCKER_IP"))
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))

	go c.Run()

	var reply []map[string]interface{}

	// ToDo: Should make use of constructors here
	operation := op.Operation{}
	operation.Op = "insert"
	operation.Table = "Bridge"

	// W00t! Anonymous struct
	operation.Row = struct {
		name string `json:"name"`
	}{
		"docker-ovs1",
	}

	args := []interface{}{"Open_vSwitch", operation}

	err = c.Call("transact", args, &reply)

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if reply[0]["map"] != nil {
		t.Error("No UUID Returned", reply[0]["map"])
	}
}
