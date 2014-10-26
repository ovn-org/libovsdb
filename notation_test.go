package libovsdb

import (
	"encoding/json"
	"log"
	"testing"
)

func TestOpRowSerialization(t *testing.T) {
	operation := Operation{
        Op: "insert",
        Table: "Bridge",
    }

	operation.Row = make(map[string]interface{})
	operation.Row["name"] = "docker-ovs"

	str, err := json.Marshal(operation)

	if err != nil {
		log.Fatal("serialization error:", err)
	}

	expected := `{"op":"insert","table":"Bridge","row":{"name":"docker-ovs"}}`

	if string(str) != expected {
		t.Error("Expected: ", expected, "Got", string(str))
	}
}

func TestOpRowsSerialization(t *testing.T) {
	operation := Operation{
        Op: "insert",
	    Table: "Interface",
    }

	iface1 := make(map[string]interface{})
	iface1["name"] = "test-iface1"
	iface1["mac"] = "0000ffaaaa"
	iface1["ofport"] = 1

	iface2 := make(map[string]interface{})
	iface2["name"] = "test-iface2"
	iface2["mac"] = "0000ffaabb"
	iface2["ofport"] = 2

	operation.Rows = []map[string]interface{}{iface1, iface2}

	str, err := json.Marshal(operation)

	if err != nil {
		log.Fatal("serialization error:", err)
	}

	expected := `{"op":"insert","table":"Interface","rows":[{"mac":"0000ffaaaa","name":"test-iface1","ofport":1},{"mac":"0000ffaabb","name":"test-iface2","ofport":2}]}`

	if string(str) != expected {
		t.Error("Expected: ", expected, "Got", string(str))
	}
}

func TestValidateUuid(t *testing.T) {
	uuid1 := "this is a bad uuid"                   // Bad
	uuid2 := "alsoabaduuid"                         // Bad
	uuid3 := "550e8400-e29b-41d4-a716-446655440000" // Good
	uuid4 := "thishoul-dnot-pass-vali-dationchecks" // Bad

	err := validateUUID(uuid1)

	if err == nil {
		t.Error(uuid1, " is not a valid UUID")
	}

	err = validateUUID(uuid2)

	if err == nil {
		t.Error(uuid2, " is not a valid UUID")
	}

	err = validateUUID(uuid3)

	if err != nil {
		t.Error(uuid3, " is a valid UUID")
	}

	err = validateUUID(uuid4)

	if err == nil {
		t.Error(uuid4, " is not a valid UUID")
	}
}

func TestNewUUID(t *testing.T) {
    uuid, _ := NewUUID("550e8400-e29b-41d4-a716-446655440000")
    uuidStr, _ := json.Marshal(uuid)
    expected := `["uuid","550e8400-e29b-41d4-a716-446655440000"]`
    if string(uuidStr) != expected {
        t.Error("uuid is not correctly formatted")
    }
}

func TestNewCondition(t *testing.T) {
    cond := NewCondition("uuid", "==", "550e8400-e29b-41d4-a716-446655440000")
    condStr, _ := json.Marshal(cond)
    expected := `["uuid","==","550e8400-e29b-41d4-a716-446655440000"]`
    if string(condStr) != expected {
        t.Error("condition is not correctly formatted")
    }
}

func TestNewMutation(t *testing.T) {
    mutation := NewCondition("column", "+=", 1)
    mutationStr, _ := json.Marshal(mutation)
    expected := `["column","+=",1]`
    if string(mutationStr) != expected {
        t.Error("mutation is not correctly formatted")
    }
}

