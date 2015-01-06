package libovsdb

import (
	"fmt"
	"testing"
)

func TestCreateBridge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	iface := Interface{
		Name: "br-test",
	}
	port := Port{
		Name:      "br-test",
		Interface: iface,
	}
	ops := CreateBridgeOps{
		BridgeName: "br-test",
		Ports:      []Port{port},
	}

	ovs, err := Connect("192.168.59.103", int(6640))
	if err != nil {
		panic(err)
	}

	err = CreateBridge(ovs, ops)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

}
