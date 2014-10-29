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
	ovs.Schema[reply[0]].Print()
	ovs.Disconnect()
}

func TestGetSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if err != nil {
		panic(err)
	}

	var dbName string = "Open_vSwitch"
	reply, err := ovs.GetSchema(dbName)

	if err != nil {
		log.Fatal("GetSchemas error:", err)
		t.Error("Error Processing GetSchema for ", dbName, err)
	}

	if reply.Name != dbName {
		t.Error("Schema Name mismatch. Expected: ", dbName, "Got: ", reply.Name)
	}
	ovs.Disconnect()
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
	ovs.Disconnect()
}

func TestDatabaseValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	// 1. Valid scenario
	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	reply, err := ovs.Transact("Open_vSwitch", operation)
	if err != nil {
		t.Error("Error processing Transact RPC ", err)
	}

	inner := reply[0].(map[string]interface{})
	uuid := inner["uuid"].([]interface{})

	if err != nil {
		log.Fatal("transact error:", err)
	}

	if uuid[1] == nil {
		t.Error("No UUID Returned")
	}

	// 2. Invalid Database Name

	reply, err = ovs.Transact("Invalid_DB", operation)
	if err == nil {
		t.Error("Invalid DB operation Validation failed")
	}

	// 3. Invalid Table Name

	operation = Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   bridge,
	}
	reply, err = ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Table Name Validation failed")
	}

	// 4. Invalid Column Name in a Single Row

	bridge["invalid_column"] = "invalid_column"
	operation = Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}
	reply, err = ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	// 5. Invalid Column Name in Multiple Rows

	rows := make([]map[string]interface{}, 2)
	invalidBridge := make(map[string]interface{})
	invalidBridge["invalid_column"] = "invalid_column"

	rows[0] = invalidBridge
	rows[1] = bridge
	operation = Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	reply, err = ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	// 6. Invalid Column Name in a Columns

	bridge["invalid_column"] = "invalid_column"
	operation = Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	reply, err = ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}
