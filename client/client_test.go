package client

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cenkalti/rpc2"
	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/cache"
	db "github.com/ovn-org/libovsdb/database"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/libovsdb/ovsdb/serverdb"
	"github.com/ovn-org/libovsdb/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/ovn-org/libovsdb/test"
)

var (
	aUUID0 = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1 = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2 = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3 = "2f77b348-9768-4866-b761-89d5177ecda3"
)

func testOvsSet(t *testing.T, set interface{}) ovsdb.OvsSet {
	oSet, err := ovsdb.NewOvsSet(set)
	assert.Nil(t, err)
	return oSet
}

func testOvsMap(t *testing.T, set interface{}) ovsdb.OvsMap {
	oMap, err := ovsdb.NewOvsMap(set)
	assert.Nil(t, err)
	return oMap
}

func updateBenchmark(ovs *ovsdbClient, updates []byte, b *testing.B) {
	for n := 0; n < b.N; n++ {
		params := []json.RawMessage{[]byte(`{"databaseName":"Open_vSwitch","id":"v1"}`), updates}
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
	bridgeUUIDs := []string{}
	for _, b := range bridges {
		bridgeUUIDs = append(bridgeUUIDs, `[ "uuid", "`+b+`" ]`)
	}
	return `{
		"bridges": [ "set", [` + strings.Join(bridgeUUIDs, `,`) + `]],
		"cur_cfg": 0,
		"datapath_types": [ "set", [] ],
		"datapaths": [ "map", [] ],
		"db_version":       "8.2.0",
		"dpdk_initialized": false,
		"dpdk_version":     [ "set", [] ],
		"external_ids":     [ "map", [["system-id","829f8534-94a8-468e-9176-132738cf260a"]]],
		"iface_types":      [ "set", [] ],
		"manager_options":  ["uuid", "6e4cd5fc-f51a-462a-b3d6-a696af6d7a84"],
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
	clientDbModel, err := FullDatabaseModel()
	require.NoError(b, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(b, err)
	s, err := Schema()
	require.NoError(b, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDbModel)
	require.Empty(b, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(b, err)
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `}
		}
	}`)
	updateBenchmark(ovs, update, b)
}

func BenchmarkUpdate2(b *testing.B) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(b, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(b, err)
	s, err := Schema()
	require.NoError(b, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	require.NoError(b, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	require.Empty(b, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(b, err)
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo", "bar") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `},
			"bar": {"new": ` + newBridgeRow("bar") + `}
		}
	}`)
	updateBenchmark(ovs, update, b)
}

func BenchmarkUpdate3(b *testing.B) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(b, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(b, err)
	s, err := Schema()
	require.NoError(b, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	require.NoError(b, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	require.Empty(b, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(b, err)
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo", "bar", "baz") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `},
			"bar": {"new": ` + newBridgeRow("bar") + `},
			"baz": {"new": ` + newBridgeRow("baz") + `}
		}
	}`)
	updateBenchmark(ovs, update, b)
}

func BenchmarkUpdate5(b *testing.B) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(b, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(b, err)
	s, err := Schema()
	require.NoError(b, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	require.NoError(b, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	require.Empty(b, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(b, err)
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo", "bar", "baz", "quux", "foofoo") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `},
			"bar": {"new": ` + newBridgeRow("bar") + `},
			"baz": {"new": ` + newBridgeRow("baz") + `},
			"quux": {"new": ` + newBridgeRow("quux") + `},
			"foofoo": {"new": ` + newBridgeRow("foofoo") + `}
		}
	}`)
	updateBenchmark(ovs, update, b)
}

func BenchmarkUpdate8(b *testing.B) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(b, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(b, err)
	s, err := Schema()
	require.NoError(b, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	require.NoError(b, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	require.Empty(b, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(b, err)
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo", "bar", "baz", "quux", "foofoo", "foobar", "foobaz", "fooquux") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `},
			"bar": {"new": ` + newBridgeRow("bar") + `},
			"baz": {"new": ` + newBridgeRow("baz") + `},
			"quux": {"new": ` + newBridgeRow("quux") + `},
			"foofoo": {"new": ` + newBridgeRow("foofoo") + `},
			"foobar": {"new": ` + newBridgeRow("foobar") + `},
			"foobaz": {"new": ` + newBridgeRow("foobaz") + `},
			"fooquux": {"new": ` + newBridgeRow("fooquux") + `}
		}
	}`)
	updateBenchmark(ovs, update, b)
}

func TestEcho(t *testing.T) {
	req := []interface{}{"hi"}
	var reply []interface{}
	clientDbModel, err := FullDatabaseModel()
	require.NoError(t, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(t, err)
	err = ovs.echo(req, &reply)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, reply) {
		t.Error("Expected: ", req, " Got: ", reply)
	}
}

func TestUpdate(t *testing.T) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(t, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(t, err)
	s, err := Schema()
	require.NoError(t, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	require.Empty(t, errs)
	ovs.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	require.NoError(t, err)
	var reply []interface{}
	update := []byte(`{
		"Open_vSwitch": {
			"ovs": {"new": ` + newOvsRow("foo") + `}
		},
		"Bridge": {
			"foo": {"new": ` + newBridgeRow("foo") + `}
		}
	}`)
	params := []json.RawMessage{[]byte(`{"databaseName":"Open_vSwitch","id":"v1"}`), update}
	err = ovs.update(params, &reply)
	if err != nil {
		t.Error(err)
	}
}

func TestOperationWhenNeverConnected(t *testing.T) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(t, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(t, err)
	s, err := Schema()
	require.NoError(t, err)

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			"echo",
			func() error {
				return ovs.Echo(context.TODO())
			},
		},
		{
			"transact",
			func() error {
				comment := "this is only a test"
				_, err := ovs.Transact(context.TODO(), ovsdb.Operation{Op: ovsdb.OperationComment, Comment: &comment})
				return err
			},
		},
		{
			"monitor/monitor all",
			func() error {
				_, err := ovs.MonitorAll(context.TODO())
				return err
			},
		},
		{
			"monitor cancel",
			func() error {
				return ovs.MonitorCancel(context.TODO(), newMonitorCookie(s.Name))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			assert.EqualError(t, err, ErrNotConnected.Error())
		})
	}
}

func TestOperationWhenNotConnected(t *testing.T) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(t, err)
	ovs, err := newOVSDBClient(clientDbModel)
	require.NoError(t, err)
	s, err := Schema()
	require.NoError(t, err)
	var errs []error
	fullModel, errs := model.NewDatabaseModel(s, ovs.primaryDB().model.Client())
	require.Equalf(t, len(errs), 0, "expected no error but some occurred: %+v", errs)
	ovs.primaryDB().model = fullModel

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			"echo",
			func() error {
				return ovs.Echo(context.TODO())
			},
		},
		{
			"transact",
			func() error {
				comment := "this is only a test"
				_, err := ovs.Transact(context.TODO(), ovsdb.Operation{Op: ovsdb.OperationComment, Comment: &comment})
				return err
			},
		},
		{
			"monitor/monitor all",
			func() error {
				_, err := ovs.MonitorAll(context.TODO())
				return err
			},
		},
		{
			"monitor cancel",
			func() error {
				return ovs.MonitorCancel(context.TODO(), newMonitorCookie(s.Name))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			assert.EqualError(t, err, ErrNotConnected.Error())
		})
	}
}

func TestSetOption(t *testing.T) {
	clientDbModel, err := FullDatabaseModel()
	require.NoError(t, err)
	o, err := newOVSDBClient(clientDbModel)
	require.NoError(t, err)

	o.options, err = newOptions()
	require.NoError(t, err)

	err = o.SetOption(WithEndpoint("tcp::6640"))
	require.NoError(t, err)

	o.rpcClient = &rpc2.Client{}

	err = o.SetOption(WithEndpoint("tcp::6641"))
	assert.EqualError(t, err, "cannot set option when client is connected")
}

func newOVSDBServer(t testing.TB, dbModel model.ClientDBModel, schema ovsdb.DatabaseSchema) (*server.OvsdbServer, string) {
	serverDBModel, err := serverdb.FullDatabaseModel()
	require.NoError(t, err)
	serverSchema := serverdb.Schema()

	db := db.NewInMemoryDatabase(map[string]model.ClientDBModel{
		schema.Name:       dbModel,
		serverSchema.Name: serverDBModel,
	})

	dbMod, errs := model.NewDatabaseModel(schema, dbModel)
	require.Empty(t, errs)

	servMod, errs := model.NewDatabaseModel(serverSchema, serverDBModel)
	require.Empty(t, errs)

	server, err := server.NewOvsdbServer(db, dbMod, servMod)
	require.NoError(t, err)

	tmpfile := fmt.Sprintf("/tmp/ovsdb-%d.sock", rand.Intn(10000))
	t.Cleanup(func() {
		os.Remove(tmpfile)
	})
	go func() {
		if err := server.Serve("unix", tmpfile); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(server.Close)
	require.Eventually(t, func() bool {
		return server.Ready()
	}, 1*time.Second, 10*time.Millisecond)

	return server, tmpfile
}

func newClientServerPair(t *testing.T, connectCounter *int32, isLeader bool) (Client, *serverdb.Database, string) {
	defSchema, err := Schema()
	require.NoError(t, err)

	serverDBModel, err := serverdb.FullDatabaseModel()
	require.NoError(t, err)

	// Create server
	clientDBModel, err := FullDatabaseModel()
	require.NoError(t, err)
	s, sock := newOVSDBServer(t, clientDBModel, defSchema)
	s.OnConnect(func(_ *rpc2.Client) {
		atomic.AddInt32(connectCounter, 1)
	})

	// Create client for this server's Server database
	endpoint := fmt.Sprintf("unix:%s", sock)
	cli, err := newOVSDBClient(serverDBModel, WithEndpoint(endpoint))
	require.NoError(t, err)
	err = cli.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(cli.Close)

	// Populate the _Server database table
	sid := fmt.Sprintf("%04x", rand.Uint32())
	row := &serverdb.Database{
		UUID:      uuid.NewString(),
		Name:      clientDBModel.Name(),
		Connected: true,
		Leader:    isLeader,
		Model:     serverdb.DatabaseModelClustered,
		Sid:       &sid,
	}
	ops, err := cli.Create(row)
	require.Nil(t, err)
	reply, err := cli.Transact(context.Background(), ops...)
	assert.Nil(t, err)
	opErr, err := ovsdb.CheckOperationResults(reply, ops)
	assert.NoErrorf(t, err, "%+v", opErr)

	row.UUID = reply[0].UUID.GoUUID
	return cli, row, endpoint
}

func setLeader(t *testing.T, cli Client, row *serverdb.Database, isLeader bool) {
	row.Leader = isLeader
	ops, err := cli.Where(row).Update(row, &row.Leader)
	require.Nil(t, err)
	reply, err := cli.Transact(context.Background(), ops...)
	require.Nil(t, err)
	opErr, err := ovsdb.CheckOperationResults(reply, ops)
	assert.NoErrorf(t, err, "%+v", opErr)
}

func TestClientReconnectLeaderOnly(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	var connected1, connected2 int32
	cli1, row1, endpoint1 := newClientServerPair(t, &connected1, true)
	cli2, row2, endpoint2 := newClientServerPair(t, &connected2, false)

	// Create client to test reconnection for
	clientDBModel, err := FullDatabaseModel()
	require.NoError(t, err)
	ovs, err := newOVSDBClient(clientDBModel,
		WithLeaderOnly(true),
		WithReconnect(5*time.Second, &backoff.ZeroBackOff{}),
		WithEndpoint(endpoint1),
		WithEndpoint(endpoint2))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(ovs.Close)

	// Server1 should have 2 connections: cli1 and ovs
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&connected1) == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Server2 should have 1 connection: cli2
	require.Never(t, func() bool {
		return atomic.LoadInt32(&connected2) > 1
	}, 2*time.Second, 10*time.Millisecond)

	// First leadership change
	setLeader(t, cli2, row2, true)
	setLeader(t, cli1, row1, false)

	// Server2 should have 2 connections: cli2 and ovs
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&connected2) == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Server1 should still only have 2 total connections; eg the
	// client under test should not have reconnected
	require.Never(t, func() bool {
		return atomic.LoadInt32(&connected1) > 2
	}, 2*time.Second, 10*time.Millisecond)

	// Second leadership change
	setLeader(t, cli1, row1, true)
	setLeader(t, cli2, row2, false)

	// Server1 should now have 3 total connections: cli1, original ovs,
	// and second ovs
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&connected1) == 3
	}, 2*time.Second, 10*time.Millisecond)

	// Server2 should still only have 2 total connections; eg the
	// client under test should not have reconnected
	require.Never(t, func() bool {
		return atomic.LoadInt32(&connected2) > 2
	}, 2*time.Second, 10*time.Millisecond)
}

func TestClientValidateTransaction(t *testing.T) {
	defSchema, err := Schema()
	require.NoError(t, err)

	// Create server with default model
	serverDBModel, err := FullDatabaseModel()
	require.NoError(t, err)
	_, sock := newOVSDBServer(t, serverDBModel, defSchema)

	// Create a client with primary and secondary indexes
	// and transaction validation
	clientDBModel, err := FullDatabaseModel()
	require.NoError(t, err)
	clientDBModel.SetIndexes(
		map[string][]model.ClientIndex{
			"Bridge": {
				model.ClientIndex{
					Type: model.PrimaryIndexType,
					Columns: []model.ColumnKey{
						{
							Column: "datapath_type",
						},
						{
							Column: "datapath_version",
						},
					},
				},
				model.ClientIndex{
					Type: model.SecondaryIndexType,
					Columns: []model.ColumnKey{
						{
							Column: "datapath_type",
						},
					},
				},
			},
		},
	)

	endpoint := fmt.Sprintf("unix:%s", sock)
	cli, err := newOVSDBClient(clientDBModel, WithEndpoint(endpoint), WithTransactionValidation(true))
	require.NoError(t, err)
	err = cli.Connect(context.Background())
	require.NoError(t, err)
	_, err = cli.MonitorAll(context.Background())
	require.NoError(t, err)

	tests := []struct {
		desc              string
		create            *Bridge
		update            *Bridge
		delete            *Bridge
		expectedErrorType interface{}
	}{
		{
			"Creating a first bridge should succeed",
			&Bridge{
				Name:            "bridge",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			nil,
			nil,
			nil,
		},
		{
			"Creating a duplicate schema index should fail",
			&Bridge{
				Name:            "bridge",
				DatapathType:    "type2",
				DatapathVersion: "2",
			},
			nil,
			nil,
			&ovsdb.ConstraintViolation{},
		},
		{
			"Creating a duplicate primary client index should fail",
			&Bridge{
				Name:            "bridge2",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			nil,
			nil,
			&ovsdb.ConstraintViolation{},
		},
		{
			"Creating a duplicate secondary client index should succeed",
			&Bridge{
				Name:            "bridge2",
				DatapathType:    "type1",
				DatapathVersion: "2",
			},
			nil,
			nil,
			nil,
		},
		{
			"Updating to duplicate a primary client index should fail",
			nil,
			&Bridge{
				Name:            "bridge2",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			nil,
			&ovsdb.ConstraintViolation{},
		},
		{
			"Changing an existing index and creating it again in the same transaction should succeed",
			&Bridge{
				Name:            "bridge3",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			&Bridge{
				Name:            "bridge",
				DatapathVersion: "3",
			},
			nil,
			nil,
		},
		{
			"Deleting an existing index and creating it again in the same transaction should succeed",
			&Bridge{
				Name:            "bridge4",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			nil,
			&Bridge{
				Name:            "bridge3",
				DatapathType:    "type1",
				DatapathVersion: "1",
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			ops := []ovsdb.Operation{}
			if tt.delete != nil {
				deleteOps, err := cli.Where(tt.delete).Delete()
				require.NoError(t, err)
				ops = append(ops, deleteOps...)
			}
			if tt.update != nil {
				updateOps, err := cli.Where(tt.update).Update(tt.update)
				require.NoError(t, err)
				ops = append(ops, updateOps...)
			}
			if tt.create != nil {
				createOps, err := cli.Create(tt.create)
				require.NoError(t, err)
				ops = append(ops, createOps...)
			}

			res, err := cli.Transact(context.Background(), ops...)
			if tt.expectedErrorType != nil {
				require.Error(t, err)
				require.IsTypef(t, tt.expectedErrorType, err, err.Error())
			} else {
				require.NoError(t, err)
				_, err = ovsdb.CheckOperationResults(res, ops)
				require.NoError(t, err)
			}
		})
	}
}

func BenchmarkClientValidateMutateTransaction(b *testing.B) {
	defSchema, err := Schema()
	require.NoError(b, err)

	dbModel, err := FullDatabaseModel()
	require.NoError(b, err)

	// Create server with default model
	_, sock := newOVSDBServer(b, dbModel, defSchema)
	verbosity := stdr.SetVerbosity(0)
	b.Cleanup(func() {
		stdr.SetVerbosity(verbosity)
	})

	// Create a client with transaction validation
	getClient := func(opts ...Option) *ovsdbClient {
		endpoint := fmt.Sprintf("unix:%s", sock)
		l := logr.Discard()
		cli, err := newOVSDBClient(dbModel, append(opts, WithEndpoint(endpoint), WithLogger(&l))...)
		stdr.SetVerbosity(0)
		require.NoError(b, err)
		err = cli.Connect(context.Background())
		require.NoError(b, err)
		_, err = cli.MonitorAll(context.Background())
		require.NoError(b, err)
		return cli
	}

	cli := getClient()

	numRows := 500
	models := []*Bridge{}
	for i := 0; i < numRows; i++ {
		model := &Bridge{
			Name: fmt.Sprintf("Name-%d", i),
		}
		for j := 0; j < numRows; j++ {
			model.Ports = append(model.Ports, uuid.New().String())
		}
		ops, err := cli.Create(model)
		require.NoError(b, err)
		_, err = cli.Transact(context.Background(), ops...)
		require.NoError(b, err)
		models = append(models, model)
	}

	rand.Seed(int64(b.N))

	benchmarks := []struct {
		name   string
		client *ovsdbClient
		ops    int
	}{
		{
			"1 mutate ops with validating client",
			getClient(WithTransactionValidation(true)),
			1,
		},
		{
			"1 mutate ops with non validating client",
			getClient(),
			1,
		},
		{
			"10 mutate ops with validating client",
			getClient(WithTransactionValidation(true)),
			10,
		},
		{
			"10 mutate ops with non validating client",
			getClient(),
			10,
		},
		{
			"100 mutate ops with validating client",
			getClient(WithTransactionValidation(true)),
			100,
		},
		{
			"100 mutate ops with non validating client",
			getClient(),
			100,
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cli := bm.client
			ops := []ovsdb.Operation{}
			for j := 0; j < bm.ops; j++ {
				m := models[rand.Intn(numRows)]
				op, err := cli.Where(m).Mutate(m, model.Mutation{
					Field:   &m.Ports,
					Mutator: ovsdb.MutateOperationInsert,
					Value:   []string{uuid.New().String()},
				})
				require.NoError(b, err)
				ops = append(ops, op...)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := cli.Transact(context.Background(), ops...)
				require.NoError(b, err)
			}
		})
	}
}
