package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/ovn-org/libovsdb"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to this file")
var memprofile = flag.String("memoryprofile", "", "write memory profile to this file")
var nins = flag.Int("ninserts", 100, "insert this number of elements in the database")
var verbose = flag.Bool("verbose", false, "Be verbose")
var connection = flag.String("ovsdb", "unix:/var/run/openvswitch/db.sock", "OVSDB connection string")

var (
	rootUUID   string
	insertions int
	deletions  int
)

func run() {
	ovs, err := libovsdb.Connect(*connection, "Open_vSwitch", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ovs.Disconnect()
	ovs.Cache.AddEventHandler(
		&libovsdb.EventHandlerFuncs{
			AddFunc: func(table string, row libovsdb.Row) {
				insertions++
			},
			DeleteFunc: func(table string, row libovsdb.Row) {
				deletions++
			},
		},
	)

	if err := ovs.MonitorAll(""); err != nil {
		log.Fatal(err)
	}

	// Get root UUID
	for _, uuid := range ovs.Cache.Table("Open_vSwitch").Rows() {
		rootUUID = uuid
		if *verbose {
			fmt.Printf("rootUUID is %v", rootUUID)
		}
	}

	// Remove all existing bridges
	if ovs.Cache.Table("Bridge") != nil {
		for _, uuid := range ovs.Cache.Table("Bridge").Rows() {
			deleteBridge(ovs, uuid)
		}
	}

	for i := 0; i < *nins; i++ {
		createBridge(ovs, i)
	}
}

func transact(ovs *libovsdb.OvsdbClient, operations []libovsdb.Operation) (ok bool, uuid string) {
	reply, _ := ovs.Transact(operations...)

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok = true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	uuid = reply[0].UUID.GoUUID
	return
}

func deleteBridge(ovs *libovsdb.OvsdbClient, uuid string) {
	var mutation []interface{}
	var delCondition []interface{}
	var mutCondition []interface{}

	delCondition = libovsdb.NewCondition("_uuid", "==", libovsdb.UUID{GoUUID: uuid})

	mutateUUID := []libovsdb.UUID{{GoUUID: uuid}}
	mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
	mutation = libovsdb.NewMutation("bridges", "delete", mutateSet)
	// hacked Condition till we get Monitor / Select working
	mutCondition = libovsdb.NewCondition("_uuid", "==", libovsdb.UUID{GoUUID: rootUUID})

	deleteOp := libovsdb.Operation{
		Op:    "delete",
		Table: "Bridge",
		Where: []interface{}{delCondition},
	}
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{mutCondition},
	}

	operations := []libovsdb.Operation{deleteOp, mutateOp}
	ok, _ := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Deletion Successful : ", uuid)
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, iter int) {
	bridge := make(map[string]interface{})
	namedUUID := "gopher"
	bridgeName := fmt.Sprintf("bridge-%d", iter)
	datapathID, _ := libovsdb.NewOvsSet([]string{"blablabla"})
	otherConfig, _ := libovsdb.NewOvsMap(map[string]string{
		"foo":  "bar",
		"fake": "config",
	})
	externalIds, _ := libovsdb.NewOvsMap(map[string]string{
		"key1": "val1",
		"key2": "val2",
	})
	bridge["name"] = bridgeName
	bridge["other_config"] = otherConfig
	bridge["datapath_id"] = datapathID
	bridge["external_ids"] = externalIds

	// simple insert operation
	insertOp := libovsdb.Operation{
		Op:       "insert",
		Table:    "Bridge",
		Row:      bridge,
		UUIDName: namedUUID,
	}

	var mutation []interface{}
	var condition []interface{}

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	uuidParameter := libovsdb.UUID{GoUUID: rootUUID}
	mutateUUID := []libovsdb.UUID{{GoUUID: namedUUID}}
	mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
	mutation = libovsdb.NewMutation("bridges", "insert", mutateSet)
	condition = libovsdb.NewCondition("_uuid", "==", uuidParameter)

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	ok, uuid := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Addition Successful : ", uuid)
		}
	}
}
func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	run()

	fmt.Printf("Summary:\n")
	fmt.Printf("\tInsertions: %d\n", insertions)
	fmt.Printf("\tDeletions: %d\n", deletions)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
