package libovsdb

import (
	"encoding/json"
	"testing"
)

func TestNewGetSchemaArgs(t *testing.T) {
	database := "Open_vSwitch"
	args := NewGetSchemaArgs(database)
	argString, _ := json.Marshal(args)
	expected := `["Open_vSwitch"]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}

func TestNewTransactArgs(t *testing.T) {
	database := "Open_vSwitch"
	operation := Operation{Op: "insert", Table: "Bridge"}
	args := NewTransactArgs(database, operation)
	argString, _ := json.Marshal(args)
	expected := `["Open_vSwitch",{"op":"insert","table":"Bridge"}]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}

func TestNewCancelArgs(t *testing.T) {
	id := 1
	args := NewCancelArgs(id)
	argString, _ := json.Marshal(args)
	expected := `[1]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}

func TestNewMonitorArgs(t *testing.T) {
	database := "Open_vSwitch"
	value := 1
	r := MonitorRequest{
		Columns: []string{"Bridge", "Port", "Interface"},
		Select: MonitorSelect{
			Initial: true,
			Insert:  true,
			Delete:  true,
			Modify:  true,
		},
	}
	requests := []MonitorRequest{r}
	args := NewMonitorArgs(database, value, requests)
	argString, _ := json.Marshal(args)
	expected := `["Open_vSwitch",1,[{"columns":["Bridge","Port","Interface"],"select":{"initial":true,"insert":true,"delete":true,"modify":true}}]]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}

func TestNewMonitorCancelArgs(t *testing.T) {
	value := 1
	args := NewMonitorCancelArgs(value)
	argString, _ := json.Marshal(args)
	expected := `[1]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}

func TestNewLockArgs(t *testing.T) {
	id := "testId"
	args := NewLockArgs(id)
	argString, _ := json.Marshal(args)
	expected := `["testId"]`
	if string(argString) != expected {
		t.Error("arguments not properly formatted")
	}
}
