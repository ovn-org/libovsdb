package libovsdb

import (
	"log"
	"os"
	"testing"
)

func TestListDbs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if err != nil {
		panic(err)
	}
	reply, err := ovs.ListDbs()

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

	ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	reply, err := ovs.Transact("Open_vSwitch", operation)

	inner := reply[0].(map[string]interface{})
	uuid := inner["uuid"].([]interface{})

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if uuid[1] == nil {
		t.Error("No UUID Returned")
	}
}
