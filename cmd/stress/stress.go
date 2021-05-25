package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// ORMBridge is the simplified ORM model of the Bridge table
type bridgeType struct {
	UUID        string            `ovs:"_uuid"`
	Name        string            `ovs:"name"`
	OtherConfig map[string]string `ovs:"other_config"`
	ExternalIds map[string]string `ovs:"external_ids"`
	Ports       []string          `ovs:"ports"`
	Status      map[string]string `ovs:"status"`
}

// ORMovs is the simplified ORM model of the Bridge table
type ovsType struct {
	UUID    string   `ovs:"_uuid"`
	Bridges []string `ovs:"bridges"`
}

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to this file")
	memprofile = flag.String("memoryprofile", "", "write memory profile to this file")
	nins       = flag.Int("ninserts", 100, "insert this number of elements in the database")
	verbose    = flag.Bool("verbose", false, "Be verbose")
	connection = flag.String("ovsdb", "unix:/var/run/openvswitch/db.sock", "OVSDB connection string")
	dbModel    *model.DBModel

	ready      bool
	rootUUID   string
	insertions int
	deletions  int
)

func run() {
	ovs, err := client.Connect(*connection, dbModel, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ovs.Disconnect()
	ovs.Cache.AddEventHandler(
		&client.EventHandlerFuncs{
			AddFunc: func(table string, model model.Model) {
				if ready && table == "Bridge" {
					insertions++
					if *verbose {
						fmt.Printf(".")
					}
				}
			},
			DeleteFunc: func(table string, model model.Model) {
				if table == "Bridge" {
					deletions++
				}
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
			fmt.Printf("rootUUID is %v\n", rootUUID)
		}
	}

	// Remove all existing bridges
	var bridges []bridgeType
	if err := ovs.List(&bridges); err == nil {
		for _, bridge := range bridges {
			deleteBridge(ovs, &bridge)
		}
	} else {
		if err != client.ErrNotFound {
			log.Fatal(err)
		}
	}

	ready = true
	for i := 0; i < *nins; i++ {
		createBridge(ovs, i)
	}
}

func transact(ovs *client.OvsdbClient, operations []ovsdb.Operation) (ok bool, uuid string) {
	reply, err := ovs.Transact(operations...)
	if err != nil {
		ok = false
		return
	}
	if _, err := ovsdb.CheckOperationResults(reply, operations); err != nil {
		ok = false
		return
	}
	uuid = reply[0].UUID.GoUUID
	return
}

func deleteBridge(ovs *client.OvsdbClient, bridge *bridgeType) {
	deleteOp, err := ovs.Where(bridge).Delete()
	if err != nil {
		log.Fatal(err)
	}
	ovsRow := ovsType{
		UUID: rootUUID,
	}

	mutateOp, err := ovs.Where(&ovsRow).Mutate(&ovsRow, client.Mutation{
		Field:   &ovsRow.Bridges,
		Mutator: ovsdb.MutateOperationDelete,
		Value:   []string{bridge.UUID},
	})
	if err != nil {
		log.Fatal(err)
	}

	operations := append(deleteOp, mutateOp...)
	ok, _ := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Deletion Successful : ", bridge.UUID)
		}
	}
}

func createBridge(ovs *client.OvsdbClient, iter int) {
	bridge := bridgeType{
		UUID: "gopher",
		Name: fmt.Sprintf("bridge-%d", iter),
		OtherConfig: map[string]string{
			"foo":  "bar",
			"fake": "config",
		},
		ExternalIds: map[string]string{
			"key1": "val1",
			"key2": "val2",
		},
	}
	insertOp, err := ovs.Create(&bridge)
	if err != nil {
		log.Fatal(err)
	}
	ovsRow := ovsType{}
	mutateOp, err := ovs.Where(&ovsType{UUID: rootUUID}).Mutate(&ovsRow, client.Mutation{
		Field:   &ovsRow.Bridges,
		Mutator: ovsdb.MutateOperationInsert,
		Value:   []string{bridge.UUID},
	})
	if err != nil {
		log.Fatal(err)
	}

	operations := append(insertOp, mutateOp...)
	ok, uuid := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Addition Successful : ", uuid)
		}
	}
}
func main() {
	flag.Parse()
	var err error
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

	dbModel, err = model.NewDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &ovsType{}, "Bridge": &bridgeType{}})
	if err != nil {
		log.Fatal(err)
	}

	run()

	fmt.Printf("\n\n\n")
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
