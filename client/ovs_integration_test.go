package client

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	defOvsRunDir = "/var/run/openvswitch"
	defOvsSocket = "db.sock"
)

// ORMBridge is the simplified ORM model of the Bridge table
type bridgeType struct {
	UUID        string            `ovsdb:"_uuid"`
	Name        string            `ovsdb:"name"`
	OtherConfig map[string]string `ovsdb:"other_config"`
	ExternalIds map[string]string `ovsdb:"external_ids"`
	Ports       []string          `ovsdb:"ports"`
	Status      map[string]string `ovsdb:"status"`
}

// ORMovs is the simplified ORM model of the Bridge table
type ovsType struct {
	UUID    string   `ovsdb:"_uuid"`
	Bridges []string `ovsdb:"bridges"`
}

var defDB, _ = model.NewDBModel("Open_vSwitch", map[string]model.Model{
	"Open_vSwitch": &ovsType{},
	"Bridge":       &bridgeType{}})

var cfg *Config

func SetConfig() {
	cfg = &Config{}
	var ovsRunDir = os.Getenv("OVS_RUNDIR")
	if ovsRunDir == "" {
		ovsRunDir = defOvsRunDir
	}
	var ovsDb = os.Getenv("OVS_DB")
	if ovsDb == "" {
		cfg.Addr = "unix:" + ovsRunDir + "/" + defOvsSocket
	} else {
		cfg.Addr = ovsDb
	}
}

func TestConnectIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}
	connected := make(chan bool)
	errs := make(chan error)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
		if err != nil {
			errs <- err
			return
		}
		err = ovs.Connect(ctx)
		if err != nil {
			errs <- err
		} else {
			connected <- true
			ovs.Disconnect()
		}
	}()

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("Connection Timed Out")
	case <-connected:
		return
	}
}

func TestConnectReconnectIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(ctx)
	require.NoError(t, err)

	err = ovs.Echo()
	require.NoError(t, err)

	ovs.Disconnect()

	err = ovs.Echo()
	require.EqualError(t, err, ErrNotConnected.Error())

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = ovs.Connect(ctx)
	require.NoError(t, err)

	err = ovs.Echo()
	assert.NoError(t, err)

}

func TestListDbsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}
	ovs, err := newOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	reply, err := ovs.listDbs()
	if err != nil {
		log.Fatal("ListDbs error:", err)
	}

	found := false
	for _, db := range reply {
		if db == "Open_vSwitch" {
			found = true
		}
	}

	if !found {
		t.Error("Expected: 'Open_vSwitch'", reply)
	}
	var b bytes.Buffer
	ovs.Schema().Print(&b)
	ovs.Disconnect()
}

func TestGetSchemasIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := newOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	dbName := "Open_vSwitch"
	reply, err := ovs.getSchema(dbName)

	if err != nil {
		log.Fatal("GetSchemas error:", err)
		t.Error("Error Processing GetSchema for ", dbName, err)
	}

	if reply.Name != dbName {
		t.Error("Schema Name mismatch. Expected: ", dbName, "Got: ", reply.Name)
	}
	ovs.Disconnect()
}

var bridgeName = "gopher-br7"
var bridgeUUID string

func TestInsertTransactIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	err = ovs.MonitorAll(nil)
	assert.Nil(t, err)

	// NamedUUID is used to add multiple related Operations in a single Transact operation
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: bridgeName,
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := ovs.Create(&br)
	assert.Nil(t, err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := ovs.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationInsert,
			Value:   []string{namedUUID},
		})
	assert.Nil(t, err)

	operations := append(insertOp, mutateOp...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}

	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			t.Error(oe)
		}
		t.Fatal(err)
	}
	bridgeUUID = reply[0].UUID.GoUUID
	ovs.Disconnect()
}

func TestDeleteTransactIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	if bridgeUUID == "" {
		t.Skip()
	}

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	err = ovs.MonitorAll(nil)
	assert.Nil(t, err)

	deleteOp, err := ovs.Where(&bridgeType{Name: bridgeName}).Delete()
	assert.Nil(t, err)

	ovsRow := ovsType{}
	mutateOp, err := ovs.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationDelete,
			Value:   []string{bridgeUUID},
		})

	assert.Nil(t, err)

	operations := append(deleteOp, mutateOp...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}

	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			t.Error(oe)
		}
		t.Fatal(err)
	}
	ovs.Disconnect()
}

func TestMonitorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	err = ovs.MonitorAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	ovs.Disconnect()
}

func TestTableSchemaValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   ovsdb.Row(map[string]interface{}{"name": "docker-ovs"}),
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Table Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInRowValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   ovsdb.Row(map[string]interface{}{"name": "docker-ovs", "invalid_column": "invalid_column"}),
	}

	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInMultipleRowsValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	invalidBridge := ovsdb.Row(map[string]interface{}{"invalid_column": "invalid_column"})
	bridge := ovsdb.Row(map[string]interface{}{"name": "docker-ovs"})
	rows := []ovsdb.Row{invalidBridge, bridge}

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	operation := ovsdb.Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestMonitorCancelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	monitorID := "f1b2ca48-aad7-11e7-abc4-cec278b6b50a"

	requests := make(map[string]ovsdb.MonitorRequest)
	requests["Bridge"] = ovsdb.MonitorRequest{
		Columns: []string{"name"},
		Select:  ovsdb.NewDefaultMonitorSelect(),
	}

	err = ovs.Monitor(monitorID,
		ovs.NewTableMonitor(&ovsType{}),
		ovs.NewTableMonitor(&bridgeType{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = ovs.MonitorCancel(monitorID)

	if err != nil {
		t.Error("MonitorCancel operation failed with error=", err)
	}
	ovs.Disconnect()
}

func TestInsertDuplicateTransactIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := NewOVSDBClient(defDB, WithEndpoint(cfg.Addr))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	err = ovs.MonitorAll(nil)
	assert.Nil(t, err)

	// NamedUUID is used to add multiple related Operations in a single Transact operation
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: "br-dup",
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := ovs.Create(&br)
	assert.Nil(t, err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := ovs.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationInsert,
			Value:   []string{namedUUID},
		})
	assert.Nil(t, err)

	operations := append(insertOp, mutateOp...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}
	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			t.Error(oe)
		}
		t.Fatal(err)
	}
	bridgeUUID = reply[0].UUID.GoUUID

	reply, err = ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ovsdb.CheckOperationResults(reply, operations)
	assert.Error(t, err)
	assert.IsType(t, &ovsdb.ConstraintViolation{}, err)
	ovs.Disconnect()
}
