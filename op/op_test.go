package op

import (
    "encoding/json"
    "log"
    "testing"
)

func TestOpRowSerialization(t *testing.T) {
    operation := Operation{}
    operation.Op = "insert"
    operation.Table = "Bridge"

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
    operation := Operation{}
    operation.Op = "insert"
    operation.Table = "Interface"

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
