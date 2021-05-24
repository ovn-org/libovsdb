package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS
const (
	bridgeTable = "Bridge"
	ovsTable    = "Open_vSwitch"
)

// ORMBridge is the simplified ORM model of the Bridge table
type ormBridge struct {
	UUID        string            `ovs:"_uuid"`
	Name        string            `ovs:"name"`
	OtherConfig map[string]string `ovs:"other_config"`
	ExternalIds map[string]string `ovs:"external_ids"`
	Ports       []string          `ovs:"ports"`
	Status      map[string]string `ovs:"status"`
}

// ORMOVS is the simplified ORM model of the Open_vSwitch table
type ormOvs struct {
	UUID    string   `ovs:"_uuid"`
	Bridges []string `ovs:"bridges"`
}

var quit chan bool
var update chan client.Model

var rootUUID string
var connection = flag.String("ovsdb", "unix:/var/run/openvswitch/db.sock", "OVSDB connection string")

func play(ovs *client.OvsdbClient) {
	go processInput(ovs)
	for model := range update {
		bridge := model.(*ormBridge)
		if bridge.Name == "stop" {
			fmt.Printf("Bridge stop detected: %+v\n", *bridge)
			ovs.Disconnect()
			quit <- true
		} else {
			fmt.Printf("Current list of bridges:\n")
			var bridges []ormBridge
			if err := ovs.List(&bridges); err != nil {
				log.Fatal(err)
			}
			for _, b := range bridges {
				fmt.Printf("UUID: %s  Name: %s\n", b.UUID, b.Name)
			}
		}
	}
}

func createBridge(ovs *client.OvsdbClient, bridgeName string) {
	bridge := ormBridge{
		UUID: "gopher",
		Name: bridgeName,
	}
	insertOp, err := ovs.Create(&bridge)
	if err != nil {
		log.Fatal(err)
	}

	ovsRow := ormOvs{
		UUID: rootUUID,
	}
	mutateOps, err := ovs.Where(&ovsRow).Mutate(&ovsRow, client.Mutation{
		Field:   &ovsRow.Bridges,
		Mutator: "insert",
		Value:   []string{bridge.UUID},
	})
	if err != nil {
		log.Fatal(err)
	}

	operations := append(insertOp, mutateOps...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := ovsdb.CheckOperationResults(reply, operations); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUUID)
}

func processInput(ovs *client.OvsdbClient) {
	for {
		fmt.Printf("\n Enter a Bridge Name : ")
		var bridgeName string
		fmt.Scanf("%s", &bridgeName)
		if bridgeName == "" {
			continue
		}
		createBridge(ovs, bridgeName)
	}
}

func main() {
	flag.Parse()
	quit = make(chan bool)
	update = make(chan client.Model)

	dbmodel, err := client.NewDBModel("Open_vSwitch",
		map[string]client.Model{bridgeTable: &ormBridge{}, ovsTable: &ormOvs{}})
	if err != nil {
		log.Fatal("Unable to create DB model ", err)
	}
	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := client.Connect(*connection, dbmodel, nil)

	// If you prefer to connect to OVS in a specific location :
	// ovs, err := client.Connect("tcp:192.168.56.101:6640", nil)

	if err != nil {
		log.Fatal("Unable to Connect ", err)
	}

	ovs.Cache.AddEventHandler(&client.EventHandlerFuncs{
		AddFunc: func(table string, model client.Model) {
			if table == bridgeTable {
				update <- model
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
