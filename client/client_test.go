package client

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
)

var (
	aUUID0 = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1 = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2 = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3 = "2f77b348-9768-4866-b761-89d5177ecda3"
)

func testOvsSet(t *testing.T, set interface{}) *ovsdb.OvsSet {
	oSet, err := ovsdb.NewOvsSet(set)
	assert.Nil(t, err)
	return oSet
}

func testOvsMap(t *testing.T, set interface{}) *ovsdb.OvsMap {
	oMap, err := ovsdb.NewOvsMap(set)
	assert.Nil(t, err)
	return oMap
}

func updateBenchmark(updates []byte, b *testing.B) {
	ovs := OvsdbClient{
		handlers:      []ovsdb.NotificationHandler{},
		handlersMutex: &sync.Mutex{},
	}
	for n := 0; n < b.N; n++ {
		params := []json.RawMessage{[]byte(`"v1"`), updates}
		var reply []interface{}
		err := ovs.update(params, &reply)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func newBridgeRow(name string) string {
	return `{
		"connection_mode": [ "set", [] ],
		"controller": [ "set", [] ],
		"datapath_id": "blablabla",
		"datapath_type": "",
		"datapath_version": "",
		"external_ids": [ "map", [["foo","bar"]]],
		"fail_mode": [ "set", [] ],
		"flood_vlans": [ "set", [] ],
		"flow_tables": [ "map", [] ],
		"ipfix": [ "set", [] ],
		"mcast_snooping_enable": false,
		"mirrors": [ "set", [] ],
		"name": "` + name + `",
		"netflow": [ "set", [] ],
		"other_config": [ "map", [["bar","quux"]]],
		"ports": [ "set", [] ],
		"protocols": [ "set", [] ],
		"rstp_enable": false,
		"rstp_status": [ "map", [] ],
		"sflow": [ "set", [] ],
		"status": [ "map", [] ],
		"stp_enable": false
	}`
}

func newOvsRow(bridges ...string) string {
	return `{
		"bridges": [ "set", ["` + strings.Join(bridges, `","`) + `"]],
		"cur_cfg": 0,
		"datapath_types": [ "set", [] ],
		"datapaths": [ "map", [] ],
		"db_version":       "8.2.0",
		"dpdk_initialized": false,
		"dpdk_version":     [ "set", [] ],
		"external_ids":     [ "map", [["system-id","829f8534-94a8-468e-9176-132738cf260a"]]],
		"iface_types":      [ "set", [] ],
		"manager_options":  "6e4cd5fc-f51a-462a-b3d6-a696af6d7a84",
		"next_cfg":         0,
		"other_config":     [ "map", [] ],
		"ovs_version":      "2.15.90",
		"ssl":              [ "set", [] ],
		"statistics":       [ "map", [] ],
		"system_type":      "docker-ovs",
		"system_version":   "0.1"
	}`
}

func BenchmarkUpdate1(b *testing.B) {
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": ` + newOvsRow("foo") + `
		},
		"Bridge": {
			"foo": ` + newBridgeRow("foo") + `
		}
	}`)
	updateBenchmark(update, b)
}

func BenchmarkUpdate2(b *testing.B) {
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": ` + newOvsRow("foo", "bar") + `
		},
		"Bridge": {
			"foo": ` + newBridgeRow("foo") + `,
			"bar": ` + newBridgeRow("bar") + `
		}
	}`)
	updateBenchmark(update, b)
}

func BenchmarkUpdate3(b *testing.B) {
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": ` + newOvsRow("foo", "bar", "baz") + `
		},
		"Bridge": {
			"foo": ` + newBridgeRow("foo") + `,
			"bar": ` + newBridgeRow("bar") + `,
			"baz": ` + newBridgeRow("baz") + `
		}
	}`)
	updateBenchmark(update, b)
}

func BenchmarkUpdate5(b *testing.B) {
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": ` + newOvsRow("foo", "bar", "baz", "quux", "foofoo") + `
		},
		"Bridge": {
			"foo": ` + newBridgeRow("foo") + `,
			"bar": ` + newBridgeRow("bar") + `,
			"baz": ` + newBridgeRow("baz") + `,
			"quux": ` + newBridgeRow("quux") + `,
			"foofoo": ` + newBridgeRow("foofoo") + `
		}
	}`)
	updateBenchmark(update, b)
}

func BenchmarkUpdate8(b *testing.B) {
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": ` + newOvsRow("foo", "bar", "baz", "quux", "foofoo", "foobar", "foobaz", "fooquux") + `
		},
		"Bridge": {
			"foo": ` + newBridgeRow("foo") + `,
			"bar": ` + newBridgeRow("bar") + `,
			"baz": ` + newBridgeRow("baz") + `,
			"quux": ` + newBridgeRow("quux") + `,
			"foofoo": ` + newBridgeRow("foofoo") + `,
			"foobar": ` + newBridgeRow("foobar") + `,
			"foobaz": ` + newBridgeRow("foobaz") + `,
			"fooquux": ` + newBridgeRow("fooquux") + `
		}
	}`)
	updateBenchmark(update, b)
}

func TestEcho(t *testing.T) {
	req := []interface{}{"hi"}
	var reply []interface{}
	ovs := OvsdbClient{
		handlers:      []ovsdb.NotificationHandler{},
		handlersMutex: &sync.Mutex{},
	}
	err := ovs.echo(req, &reply)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, reply) {
		t.Error("Expected: ", req, " Got: ", reply)
	}
}

func TestUpdate(t *testing.T) {
	ovs := OvsdbClient{
		handlers:      []ovsdb.NotificationHandler{},
		handlersMutex: &sync.Mutex{},
	}
	var reply []interface{}
	validUpdate := ovsdb.TableUpdates{
		"table": {
			"uuid": &ovsdb.RowUpdate{},
		},
	}
	b, err := json.Marshal(validUpdate)
	if err != nil {
		t.Fatal(err)
	}
	err = ovs.update([]json.RawMessage{[]byte(`"hello"`), b}, &reply)
	if err != nil {
		t.Error(err)
	}
}
