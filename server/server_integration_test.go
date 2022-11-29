package server

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/database"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/ovn-org/libovsdb/test"
)

func buildTestServerAndClient(t *testing.T) (client.Client, func()) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	require.Nil(t, err)

	schema, err := GetSchema()
	require.Nil(t, err)

	ovsDB := database.NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})

	rand.Seed(time.Now().UnixNano())
	tmpfile := fmt.Sprintf("/tmp/ovsdb-%d.sock", rand.Intn(10000))
	defer os.Remove(tmpfile)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)
	server, err := NewOvsdbServer(ovsDB, dbModel)
	assert.Nil(t, err)

	go func(t *testing.T, o *OvsdbServer) {
		if err := o.Serve("unix", tmpfile); err != nil {
			t.Error(err)
		}
	}(t, server)
	defer server.Close()
	require.Eventually(t, func() bool {
		return server.Ready()
	}, 1*time.Second, 10*time.Millisecond)

	ovs, err := client.NewOVSDBClient(defDB, client.WithEndpoint(fmt.Sprintf("unix:%s", tmpfile)))
	require.NoError(t, err)
	err = ovs.Connect(context.Background())
	require.NoError(t, err)

	return ovs, func() {
		ovs.Disconnect()
		server.Close()
	}
}

func TestClientServerEcho(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	err := ovs.Echo(context.Background())
	assert.Nil(t, err)
}

func TestClientServerInsert(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	_, err := ovs.MonitorAll(context.Background())
	require.NoError(t, err)

	wallace := "wallace"
	bridgeRow := &BridgeType{
		Name:         "foo",
		DatapathType: "bar",
		DatapathID:   &wallace,
		ExternalIds:  map[string]string{"go": "awesome", "docker": "made-for-each-other"},
	}

	ops, err := ovs.Create(bridgeRow)
	require.Nil(t, err)
	reply, err := ovs.Transact(context.Background(), ops...)
	assert.Nil(t, err)
	opErr, err := ovsdb.CheckOperationResults(reply, ops)
	assert.NoErrorf(t, err, "%+v", opErr)

	uuid := reply[0].UUID.GoUUID
	require.Eventually(t, func() bool {
		br := &BridgeType{UUID: uuid}
		err := ovs.Get(context.Background(), br)
		return err == nil
	}, 2*time.Second, 500*time.Millisecond)

	br := &BridgeType{UUID: uuid}
	err = ovs.Get(context.Background(), br)
	require.NoError(t, err)

	assert.Equal(t, bridgeRow.Name, br.Name)
	assert.Equal(t, bridgeRow.ExternalIds, br.ExternalIds)
	assert.Equal(t, bridgeRow.DatapathType, br.DatapathType)
	assert.Equal(t, *bridgeRow.DatapathID, wallace)
}

func TestClientServerMonitor(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	ovsRow := &OvsType{
		UUID: "ovs",
	}
	bridgeRow := &BridgeType{
		UUID:        "foo",
		Name:        "foo",
		ExternalIds: map[string]string{"go": "awesome", "docker": "made-for-each-other"},
	}

	seenMutex := sync.RWMutex{}
	seenInsert := false
	seenMutation := false
	seenInitialOvs := false
	ovs.Cache().AddEventHandler(&cache.EventHandlerFuncs{
		AddFunc: func(table string, model model.Model) {
			if table == "Bridge" {
				br := model.(*BridgeType)
				assert.Equal(t, bridgeRow.Name, br.Name)
				assert.Equal(t, bridgeRow.ExternalIds, br.ExternalIds)
				seenMutex.Lock()
				seenInsert = true
				seenMutex.Unlock()
			}
			if table == "Open_vSwitch" {
				seenMutex.Lock()
				seenInitialOvs = true
				seenMutex.Unlock()
			}
		},
		UpdateFunc: func(table string, old, new model.Model) {
			if table == "Open_vSwitch" {
				ov := new.(*OvsType)
				assert.Equal(t, 1, len(ov.Bridges))
				seenMutex.Lock()
				seenMutation = true
				seenMutex.Unlock()
			}
		},
	})

	var ops []ovsdb.Operation
	ovsOps, err := ovs.Create(ovsRow)
	require.Nil(t, err)
	reply, err := ovs.Transact(context.Background(), ovsOps...)
	require.Nil(t, err)
	_, err = ovsdb.CheckOperationResults(reply, ovsOps)
	require.Nil(t, err)
	require.NotEmpty(t, reply[0].UUID.GoUUID)
	ovsRow.UUID = reply[0].UUID.GoUUID

	_, err = ovs.MonitorAll(context.Background())
	require.Nil(t, err)
	require.Eventually(t, func() bool {
		seenMutex.RLock()
		defer seenMutex.RUnlock()
		return seenInitialOvs
	}, 1*time.Second, 10*time.Millisecond)

	bridgeOps, err := ovs.Create(bridgeRow)
	require.Nil(t, err)
	ops = append(ops, bridgeOps...)

	mutateOps, err := ovs.Where(ovsRow).Mutate(ovsRow, model.Mutation{
		Field:   &ovsRow.Bridges,
		Mutator: ovsdb.MutateOperationInsert,
		Value:   []string{"foo"},
	})
	require.Nil(t, err)
	ops = append(ops, mutateOps...)

	reply, err = ovs.Transact(context.Background(), ops...)
	require.Nil(t, err)

	_, err = ovsdb.CheckOperationResults(reply, ops)
	assert.Nil(t, err)
	assert.Equal(t, 1, reply[1].Count)

	assert.Eventually(t, func() bool {
		seenMutex.RLock()
		defer seenMutex.RUnlock()
		return seenInsert
	}, 1*time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool {
		seenMutex.RLock()
		defer seenMutex.RUnlock()
		return seenMutation
	}, 1*time.Second, 10*time.Millisecond)
}

func TestClientServerInsertAndDelete(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	_, err := ovs.MonitorAll(context.Background())
	require.NoError(t, err)

	bridgeRow := &BridgeType{
		Name:        "foo",
		ExternalIds: map[string]string{"go": "awesome", "docker": "made-for-each-other"},
	}

	ops, err := ovs.Create(bridgeRow)
	require.Nil(t, err)
	reply, err := ovs.Transact(context.Background(), ops...)
	require.Nil(t, err)
	_, err = ovsdb.CheckOperationResults(reply, ops)
	require.Nil(t, err)

	uuid := reply[0].UUID.GoUUID
	assert.Eventually(t, func() bool {
		br := &BridgeType{UUID: uuid}
		err := ovs.Get(context.Background(), br)
		return err == nil
	}, 2*time.Second, 500*time.Millisecond)

	bridgeRow.UUID = uuid
	deleteOp, err := ovs.Where(bridgeRow).Delete()
	require.Nil(t, err)

	reply, err = ovs.Transact(context.Background(), deleteOp...)
	assert.Nil(t, err)
	_, err = ovsdb.CheckOperationResults(reply, ops)
	assert.Nil(t, err)
	assert.Equal(t, 1, reply[0].Count)
}

func TestClientServerInsertDuplicate(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	bridgeRow := &BridgeType{
		Name:        "foo",
		ExternalIds: map[string]string{"go": "awesome", "docker": "made-for-each-other"},
	}

	ops, err := ovs.Create(bridgeRow)
	require.Nil(t, err)
	reply, err := ovs.Transact(context.Background(), ops...)
	require.Nil(t, err)
	_, err = ovsdb.CheckOperationResults(reply, ops)
	require.Nil(t, err)

	// duplicate
	reply, err = ovs.Transact(context.Background(), ops...)
	require.Nil(t, err)
	opErrs, err := ovsdb.CheckOperationResults(reply, ops)
	require.Nil(t, opErrs)
	require.Error(t, err)
	require.IsTypef(t, &ovsdb.ConstraintViolation{}, err, err.Error())
}

func TestClientServerInsertAndUpdate(t *testing.T) {
	ovs, close := buildTestServerAndClient(t)
	defer close()

	_, err := ovs.MonitorAll(context.Background())
	require.NoError(t, err)

	bridgeRow := &BridgeType{
		Name:        "br-update",
		ExternalIds: map[string]string{"go": "awesome", "docker": "made-for-each-other"},
	}

	ops, err := ovs.Create(bridgeRow)
	require.NoError(t, err)
	reply, err := ovs.Transact(context.Background(), ops...)
	require.NoError(t, err)
	_, err = ovsdb.CheckOperationResults(reply, ops)
	require.NoError(t, err)

	uuid := reply[0].UUID.GoUUID
	assert.Eventually(t, func() bool {
		br := &BridgeType{UUID: uuid}
		err := ovs.Get(context.Background(), br)
		return err == nil
	}, 2*time.Second, 500*time.Millisecond)

	// try to modify immutable field
	bridgeRow.UUID = uuid
	bridgeRow.Name = "br-update2"
	_, err = ovs.Where(bridgeRow).Update(bridgeRow, &bridgeRow.Name)
	require.Error(t, err)
	bridgeRow.Name = "br-update"

	// update many fields
	bridgeRow.UUID = uuid
	bridgeRow.Name = "br-update"
	bridgeRow.ExternalIds["baz"] = "foobar"
	bridgeRow.OtherConfig = map[string]string{"foo": "bar"}
	ops, err = ovs.Where(bridgeRow).Update(bridgeRow)
	require.NoError(t, err)
	reply, err = ovs.Transact(context.Background(), ops...)
	require.NoError(t, err)
	opErrs, err := ovsdb.CheckOperationResults(reply, ops)
	require.NoErrorf(t, err, "%+v", opErrs)

	require.Eventually(t, func() bool {
		br := &BridgeType{UUID: uuid}
		err = ovs.Get(context.Background(), br)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(br, bridgeRow)
	}, 2*time.Second, 50*time.Millisecond)

	newExternalIds := map[string]string{"foo": "bar"}
	bridgeRow.ExternalIds = newExternalIds
	ops, err = ovs.Where(bridgeRow).Update(bridgeRow, &bridgeRow.ExternalIds)
	require.NoError(t, err)
	reply, err = ovs.Transact(context.Background(), ops...)
	require.NoError(t, err)
	opErr, err := ovsdb.CheckOperationResults(reply, ops)
	require.NoErrorf(t, err, "%+v", opErr)

	assert.Eventually(t, func() bool {
		br := &BridgeType{UUID: uuid}
		err = ovs.Get(context.Background(), br)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(br.ExternalIds, bridgeRow.ExternalIds)
	}, 2*time.Second, 500*time.Millisecond)

	br := &BridgeType{UUID: uuid}
	err = ovs.Get(context.Background(), br)
	assert.NoError(t, err)

	assert.Equal(t, bridgeRow, br)
}

func TestUnsetOptional(t *testing.T) {
	c, close := buildTestServerAndClient(t)
	defer close()
	_, err := c.MonitorAll(context.Background())
	require.NoError(t, err)

	// Create the default bridge which has an optional DatapathID set
	optional := "optional"
	br := BridgeType{
		Name:       "br-with-optional",
		DatapathID: &optional,
	}
	ops, err := c.Create(&br)
	require.NoError(t, err)
	r, err := c.Transact(context.Background(), ops...)
	require.NoError(t, err)
	_, err = ovsdb.CheckOperationResults(r, ops)
	require.NoError(t, err)

	// verify the bridge has DatapathID set
	err = c.Get(context.Background(), &br)
	require.NoError(t, err)
	require.NotNil(t, br.DatapathID)

	// modify bridge to unset DatapathID
	br.DatapathID = nil
	ops, err = c.Where(&br).Update(&br, &br.DatapathID)
	require.NoError(t, err)
	r, err = c.Transact(context.Background(), ops...)
	require.NoError(t, err)
	_, err = ovsdb.CheckOperationResults(r, ops)
	require.NoError(t, err)

	// verify the bridge has DatapathID unset
	err = c.Get(context.Background(), &br)
	require.NoError(t, err)
	require.Nil(t, br.DatapathID)
}
