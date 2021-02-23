package main

import (
	"fmt"
	"log"

	"github.com/ovn-org/libovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS
const (
	bridgeTable = "Bridge"
	ovsTable    = "Open_vSwitch"
)

var quit chan bool
var update chan libovsdb.Row
var rootUUID string

func play(ovs *libovsdb.OvsdbClient) {
	go processInput(ovs)
	for row := range update {
		if _, ok := row.Fields["name"]; ok {
			name := row.Fields["name"].(string)
			if name == "stop" {
				fmt.Println("Bridge stop detected")
				ovs.Disconnect()
				quit <- true
			}
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, bridgeName string) {
	namedUUID := "gopher"
	// bridge row to insert
	bridge := make(map[string]interface{})
	bridge["name"] = bridgeName

	// simple insert operation
	insertOp := libovsdb.Operation{
		Op:       "insert",
		Table:    bridgeTable,
		Row:      bridge,
		UUIDName: namedUUID,
	}

	uuidParameter := libovsdb.UUID{GoUUID: rootUUID}
	mutateUUID := []libovsdb.UUID{{GoUUID: namedUUID}}
	mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
	mutation := libovsdb.NewMutation("bridges", "insert", mutateSet)
	condition := libovsdb.NewCondition("_uuid", "==", uuidParameter)

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     ovsTable,
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	reply, err := ovs.Transact(operations...)
	if err != nil {
		log.Fatal(err)
	}

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUUID)
	}
}

func processInput(ovs *libovsdb.OvsdbClient) {
	for {
		fmt.Printf("\n Enter a Bridge Name : ")
		var bridgeName string
		fmt.Scanf("%s", &bridgeName)
		createBridge(ovs, bridgeName)
	}
}

func main() {
	quit = make(chan bool)
	update = make(chan libovsdb.Row)
	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := libovsdb.Connect("tcp:", ovsTable, nil)

	// If you prefer to connect to OVS in a specific location :
	// ovs, err := libovsdb.Connect("tcp:192.168.56.101:6640", nil)

	if err != nil {
		log.Fatal("Unable to Connect ", err)
	}

	ovs.Cache.AddEventHandler(&libovsdb.EventHandlerFuncs{
		AddFunc: func(table string, row libovsdb.Row) {
			if table == bridgeTable {
				update <- row
			}
		},
	})

	err = ovs.MonitorAll("")
	if err != nil {
		log.Fatal(err)
	}

	rootUUID = ovs.Cache.Table(ovsTable).Rows()[0]

	fmt.Println(`Silly game of stopping this app when a Bridge with name "stop" is monitored !`)
	go play(ovs)
	<-quit
}
