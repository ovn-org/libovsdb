package libovsdb

import (
	"fmt"
	"log"
	"net"
	"os"
	"testing"

	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2/jsonrpc"
)

func TestListDbs(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	target := fmt.Sprintf("%s:6640", os.Getenv("DOCKER_IP"))
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))
	defer c.Close()

	go c.Run()

	var reply []interface{}

	err = c.Call("list_dbs", nil, &reply)

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if reply[0] != "Open_vSwitch" {
		t.Error("Expected: 'Open_vSwitch', Got:", reply)
	}
}

func TestTransact(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	target := fmt.Sprintf("%s:6640", os.Getenv("DOCKER_IP"))
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))
	defer c.Close()

	go c.Run()

	var reply []interface{}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	err = c.Call("transact", NewTransactArgs("Open_vSwitch", operation), &reply)

	inner := reply[0].(map[string]interface{})
	uuid := inner["uuid"].([]interface{})

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if uuid[1] == nil {
		t.Error("No UUID Returned")
	}
}
