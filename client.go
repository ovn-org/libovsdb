package libovsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2"
	"github.com/socketplane/libovsdb/Godeps/_workspace/src/github.com/cenkalti/rpc2/jsonrpc"
)

type OvsdbClient struct {
	rpcClient *rpc2.Client
	Schema    map[string]DatabaseSchema
}

func Connect(ipAddr string, port int) (OvsdbClient, error) {
	target := fmt.Sprintf("%s:%d", ipAddr, port)
	conn, err := net.Dial("tcp", target)

	if err != nil {
		panic(err)
	}

	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))
	// Process Async Notifications
	c.Handle("echo", echo)
	c.Handle("update", update)

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

// RFC 7047 : Section 4.1.6 : Echo
func echo(client *rpc2.Client, args string, reply *interface{}) error {
	*reply = args
	return nil
}

// RFC 7047 : Update Notification Section 4.1.6
// Processing "params": [<json-value>, <table-updates>]
func update(client *rpc2.Client, params []interface{}, reply *interface{}) error {
	if len(params) < 2 {
		return errors.New("Invalid Update message")
	}
	// Ignore params[0] as we dont use the <json-value> currently for comparison

	raw, ok := params[1].(map[string]interface{})
	if !ok {
		return errors.New("Invalid Update message")
	}
	var rowUpdates map[string]map[string]RowUpdate

	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &rowUpdates)
	if err != nil {
		return err
	}

	// Update the local DB cache with the tableUpdates
	tableUpdates := getTableUpdatesFromRawUnmarshal(rowUpdates)
	if len(tableUpdates.Updates) > 0 {
		return nil
	}

	return nil
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

func (ovs OvsdbClient) Transact(database string, operation ...Operation) ([]OperationResult, error) {
	var reply []OperationResult
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

// Convenience method to monitor every table/column
func (ovs OvsdbClient) MonitorAll(database string, jsonContext interface{}) (*TableUpdates, error) {
	schema, ok := ovs.Schema[database]
	if !ok {
		return nil, errors.New("invalid Database Schema")
	}

	requests := make(map[string]MonitorRequest)
	for table, tableSchema := range schema.Tables {
		var columns []string
		for column, _ := range tableSchema.Columns {
			columns = append(columns, column)
		}
		requests[table] = MonitorRequest{
			Columns: columns,
			Select: MonitorSelect{
				Initial: true,
				Insert:  true,
				Delete:  true,
				Modify:  true,
			}}
	}
	return ovs.Monitor(database, jsonContext, requests)
}

// RFC 7047 : monitor
func (ovs OvsdbClient) Monitor(database string, jsonContext interface{}, requests map[string]MonitorRequest) (*TableUpdates, error) {
	var reply TableUpdates

	args := NewMonitorArgs(database, jsonContext, requests)

	// This totally sucks. Refer to golang JSON issue #6213
	var response map[string]map[string]RowUpdate
	err := ovs.rpcClient.Call("monitor", args, &response)
	reply = getTableUpdatesFromRawUnmarshal(response)
	if err != nil {
		return nil, err
	}
	return &reply, err
}

func getTableUpdatesFromRawUnmarshal(raw map[string]map[string]RowUpdate) TableUpdates {
	var tableUpdates TableUpdates
	tableUpdates.Updates = make(map[string]TableUpdate)
	for table, update := range raw {
		tableUpdate := TableUpdate{update}
		tableUpdates.Updates[table] = tableUpdate
	}
	return tableUpdates
}
