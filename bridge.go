package libovsdb

import (
	"errors"
	"fmt"
	"strconv"
)

type CreateBridgeOps struct {
	BridgeName string
	Ports      []Port
}

type Port struct {
	Name        string
	Interface   Interface //Todo: Support bonding maybe?
	VlanMode    string
	Tag         uint16
	Trunks      []uint16
	OtherConfig map[string]interface{}
}

type Interface struct {
	Name          string
	Ifindex       string
	MacInUse      string
	Mac           string
	Ofport        uint32
	OfportRequest uint32
	Type          string
	Options       map[string]interface{}
}

func CreateBridge(ovs *OvsdbClient, ops CreateBridgeOps) error {
	NEW_BRIDGE_UUID_NAME := "new_bridge"
	var row map[string]interface{}

	operations := []Operation{}
	ports := []string{}

	// Insert each port into the Port table and its corresponding interface into Interface table
	for i, port := range ops.Ports {
		intUUIDName := "new_int_" + strconv.Itoa(i)
		portUUIDName := "new_port_" + strconv.Itoa(i)
		intRow := make(map[string]interface{})
		portRow := make(map[string]interface{})

		ports = append(ports, "named-uuid", portUUIDName)

		intRow["name"] = port.Interface.Name
		if port.Interface.Type != "" {
			intRow["type"] = port.Interface.Type
		}
		if len(port.Interface.Options) != 0 {
			intRow["options"] = port.Interface.Options
		}
		if port.Interface.Mac != "" {
			intRow["mac"] = port.Interface.Mac
		}
		if port.Interface.Ofport != 0 {
			intRow["ofport"] = port.Interface.Ofport
		}

		portRow["name"] = port.Name
		portRow["interfaces"] = [2]string{"named-uuid", intUUIDName}
		if port.VlanMode != "" {
			portRow["vlan_mode"] = port.VlanMode
			portRow["tag"] = port.Tag
			if len(port.Trunks) != 0 {
				portRow["trunks"] = port.Trunks
			}
		}
		if len(port.OtherConfig) != 0 {
			portRow["other_config"] = port.OtherConfig
		}

		IntOp := Operation{
			Op:       "insert",
			Table:    "Interface",
			Row:      intRow,
			UUIDName: intUUIDName,
		}

		PortOp := Operation{
			Op:       "insert",
			Table:    "Port",
			Row:      portRow,
			UUIDName: portUUIDName,
		}

		operations = append(operations, IntOp, PortOp)
	}

	// Insert row in bridge table for new bridge
	row = make(map[string]interface{})
	row["name"] = ops.BridgeName
	row["ports"] = ports
	newBridgeOp := Operation{
		Op:       "insert",
		Table:    "Bridge",
		Row:      row,
		UUIDName: NEW_BRIDGE_UUID_NAME,
	}

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	mutateUuid := []UUID{UUID{NEW_BRIDGE_UUID_NAME}}
	mutateSet, _ := NewOvsSet(mutateUuid)
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

	operations = append(operations, newBridgeOp, mutateOp)

	reply, err := ovs.Transact("Open_vSwitch", operations...)

	if err != nil {
		return err
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
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUuid)
		return nil
	}
	return errors.New("Transaction failed.")
}
