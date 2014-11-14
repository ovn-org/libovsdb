package main

import (
	"fmt"
	"os"

	"github.com/socketplane/libovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS

var quit chan bool
var update chan *libovsdb.TableUpdates

func play(ovs *libovsdb.OvsdbClient) {
	ovs.MonitorAll("Open_vSwitch", "")
	for {
		select {
		case currUpdate := <-update:
			for table, tableUpdate := range currUpdate.Updates {
				fmt.Println("Received Table update on : ", table)
				if table == "Bridge" {
					rows := tableUpdate.Rows
					for uuid, row := range rows {
						newRow := row.New
						if _, ok := newRow.Fields["name"]; ok {
							name := newRow.Fields["name"].(string)
							if name == "stop" {
								fmt.Println("Bridge stop detected : ", uuid)
								quit <- true
							}
						}
					}
				}
			}
		}
	}

}

func main() {
	quit = make(chan bool)
	update = make(chan *libovsdb.TableUpdates)

	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := libovsdb.Connect("", 0)

	// If you prefer to connect to OVS in a specific location :
	//ovs, err := libovsdb.Connect("192.168.56.101", 6640)

	if err != nil {
		fmt.Println("Unable to Connect ", err)
		os.Exit(1)
	}
	var notifier Notifier
	ovs.Register(notifier)

	fmt.Println(`Silly game of stopping this app when a Bridge with name "stop" is monitored`)
	go play(ovs)
	<-quit
}

type Notifier struct {
}

func (n Notifier) Update(context interface{}, tableUpdates libovsdb.TableUpdates) {
	update <- &tableUpdates
}
func (n Notifier) Locked([]interface{}) {
}
func (n Notifier) Stolen([]interface{}) {
}
func (n Notifier) Echo([]interface{}) {
}
