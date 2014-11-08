package libovsdb

import (
	"bytes"
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
		log.Fatal("ListDbs error:", err)
	}

	if reply[0] != "Open_vSwitch" {
		t.Error("Expected: 'Open_vSwitch', Got:", reply)
	}
	var b bytes.Buffer
	ovs.Schema[reply[0]].Print(&b)
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

	// NamedUuid is used to add multiple related Operations in a single Transact operation
	namedUuid := "gopher"

	// bridge row to insert
	bridge := make(map[string]interface{})
	bridge["name"] = "gopher-br"

	// simple insert operation
	insertOp := Operation{
		Op:       "insert",
		Table:    "Bridge",
		Row:      bridge,
		UUIDName: namedUuid,
	}

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	mutateUuid := []UUID{UUID{namedUuid}}
	mutateSet, _ := newOvsSet(mutateUuid)
	mutation := NewMutation("bridges", "insert", mutateSet)
	// hacked Condition till we get Monitor / Select working
	condition := NewCondition("_uuid", "!=", UUID{"2f77b348-9768-4866-b761-89d5177ecdab"})

	// simple mutate operation
	mutateOp := Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	reply, err := ovs.Transact("Open_vSwitch", insertOp, mutateOp)

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

func TestDBSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	_, err := ovs.Transact("Invalid_DB", operation)
	if err == nil {
		t.Error("Invalid DB operation Validation failed")
	}

	ovs.Disconnect()
}

func TestTableSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   bridge,
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Table Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInRowValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"
	bridge["invalid_column"] = "invalid_column"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInMultipleRowsValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	rows := make([]map[string]interface{}, 2)

	invalidBridge := make(map[string]interface{})
	invalidBridge["invalid_column"] = "invalid_column"

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	rows[0] = invalidBridge
	rows[1] = bridge
	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	operation := Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}
