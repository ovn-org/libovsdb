package server

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutateOp(t *testing.T) {
	ctx := context.Background()
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &ovsType{},
		"Bridge":       &bridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := getSchema()
	if err != nil {
		t.Fatal(err)
	}
	ovsDB := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	o, err := NewOvsdbServer(ovsDB, DatabaseModel{
		Model: defDB, Schema: schema})
	require.Nil(t, err)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := ovsType{}
	ovsRow, err := m.NewRow("Open_vSwitch", &ovs)
	require.Nil(t, err)

	bridge := bridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "quux",
			"waldo": "fred",
		},
	}
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert(ctx, "Open_vSwitch", "Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := o.Insert(ctx, "Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = o.db.Commit(context.TODO(), "Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	gotResult, gotUpdate := o.Mutate(
		ctx,
		"Open_vSwitch",
		"Open_vSwitch",
		[]ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: ovsUUID}),
		},
		[]ovsdb.Mutation{
			*ovsdb.NewMutation("bridges", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
	)
	assert.Equal(t, ovsdb.OperationResult{Count: 1}, gotResult)
	err = o.db.Commit(context.TODO(), "Open_vSwitch", uuid.New(), gotUpdate)
	require.NoError(t, err)

	bridgeSet, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: bridgeUUID}})
	assert.Nil(t, err)
	assert.Equal(t, ovsdb.TableUpdates2{
		"Open_vSwitch": ovsdb.TableUpdate2{
			ovsUUID: &ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"bridges": bridgeSet,
				},
				Old: &ovsdb.Row{
					// TODO: _uuid should be filtered
					"_uuid": ovsdb.UUID{GoUUID: ovsUUID},
				},
				New: &ovsdb.Row{
					// TODO: _uuid should be filtered
					"_uuid":   ovsdb.UUID{GoUUID: ovsUUID},
					"bridges": bridgeSet,
				},
			},
		},
	}, gotUpdate)

	keyDelete, err := ovsdb.NewOvsSet([]string{"foo"})
	assert.Nil(t, err)
	keyValueDelete, err := ovsdb.NewOvsMap(map[string]string{"baz": "quux"})
	assert.Nil(t, err)
	gotResult, gotUpdate = o.Mutate(
		ctx,
		"Open_vSwitch",
		"Bridge",
		[]ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		[]ovsdb.Mutation{
			*ovsdb.NewMutation("external_ids", ovsdb.MutateOperationDelete, keyDelete),
			*ovsdb.NewMutation("external_ids", ovsdb.MutateOperationDelete, keyValueDelete),
		},
	)
	assert.Equal(t, ovsdb.OperationResult{Count: 1}, gotResult)

	oldExternalIds, _ := ovsdb.NewOvsMap(bridge.ExternalIds)
	newExternalIds, _ := ovsdb.NewOvsMap(map[string]string{"waldo": "fred"})
	diffExternalIds, _ := ovsdb.NewOvsMap(map[string]string{"foo": "bar", "baz": "quux"})

	assert.Nil(t, err)

	gotModify := *gotUpdate["Bridge"][bridgeUUID].Modify
	gotOld := *gotUpdate["Bridge"][bridgeUUID].Old
	gotNew := *gotUpdate["Bridge"][bridgeUUID].New
	assert.Equal(t, diffExternalIds, gotModify["external_ids"])
	assert.Equal(t, oldExternalIds, gotOld["external_ids"])
	assert.Equal(t, newExternalIds, gotNew["external_ids"])
}

func TestDiff(t *testing.T) {
	originSet, _ := ovsdb.NewOvsSet([]interface{}{"foo"})

	originSetAdd, _ := ovsdb.NewOvsSet([]interface{}{"bar"})
	setAddDiff, _ := ovsdb.NewOvsSet([]interface{}{"foo", "bar"})

	originSetDel, _ := ovsdb.NewOvsSet([]interface{}{})
	setDelDiff, _ := ovsdb.NewOvsSet([]interface{}{"foo"})

	originMap, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar"})

	originMapAdd, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar", "baz": "quux"})
	originMapAddDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"baz": "quux"})

	originMapDel, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{})
	originMapDelDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar"})

	originMapReplace, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "baz"})
	originMapReplaceDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "baz"})

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected interface{}
	}{
		{
			"add to set",
			originSet,
			originSetAdd,
			setAddDiff,
		},
		{
			"delete from set",
			originSet,
			originSetDel,
			setDelDiff,
		},
		{
			"noop set",
			originSet,
			originSet,
			nil,
		},
		{
			"add to map",
			originMap,
			originMapAdd,
			originMapAddDiff,
		},
		{
			"delete from map",
			originMap,
			originMapDel,
			originMapDelDiff,
		},
		{
			"replace in map",
			originMap,
			originMapReplace,
			originMapReplaceDiff,
		},
		{
			"noop map",
			originMap,
			originMap,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := diff(tt.a, tt.b)
			assert.Equal(t, tt.expected, res)
		})
	}
}

func TestOvsdbServerInsert(t *testing.T) {
	t.Skip("need a helper for comparing rows as map elements aren't in same order")
	ctx := context.Background()
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &ovsType{},
		"Bridge":       &bridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := getSchema()
	if err != nil {
		t.Fatal(err)
	}
	ovsDB := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	o, err := NewOvsdbServer(ovsDB, DatabaseModel{
		Model: defDB, Schema: schema})
	require.Nil(t, err)
	m := mapper.NewMapper(schema)

	gromit := "gromit"
	bridge := bridgeType{
		Name:         "foo",
		DatapathType: "bar",
		DatapathID:   &gromit,
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "qux",
			"waldo": "fred",
		},
	}
	bridgeUUID := uuid.NewString()
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert(ctx, "Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.NoError(t, err)

	err = ovsDB.Commit(context.TODO(), "Open_vSwitch", uuid.New(), updates)
	assert.NoError(t, err)

	bridge.UUID = bridgeUUID
	br, err := o.db.Get(context.TODO(), "Open_vSwitch", "Bridge", bridgeUUID)
	assert.NoError(t, err)
	assert.Equal(t, &bridge, br)
	assert.Equal(t, ovsdb.TableUpdates2{
		"Bridge": {
			bridgeUUID: &ovsdb.RowUpdate2{
				Insert: &bridgeRow,
				New:    &bridgeRow,
			},
		},
	}, updates)
}

func TestOvsdbServerUpdate(t *testing.T) {
	ctx := context.Background()
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &ovsType{},
		"Bridge":       &bridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := getSchema()
	if err != nil {
		t.Fatal(err)
	}
	ovsDB := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	o, err := NewOvsdbServer(ovsDB, DatabaseModel{
		Model: defDB, Schema: schema})
	require.Nil(t, err)
	m := mapper.NewMapper(schema)

	bridge := bridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "qux",
			"waldo": "fred",
		},
	}
	bridgeUUID := uuid.NewString()
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert(ctx, "Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.NoError(t, err)

	err = ovsDB.Commit(context.TODO(), "Open_vSwitch", uuid.New(), updates)
	assert.NoError(t, err)

	halloween, _ := ovsdb.NewOvsSet([]string{"halloween"})
	tests := []struct {
		name     string
		row      ovsdb.Row
		expected *ovsdb.RowUpdate2
	}{
		{
			"update single field",
			ovsdb.Row{"datapath_type": "waldo"},
			&ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"datapath_type": "waldo",
				},
			},
		},
		{
			"update single optional field",
			ovsdb.Row{"datapath_id": "halloween"},
			&ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"datapath_id": halloween,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, updates := o.Update(
				ctx,
				"Open_vSwitch", "Bridge",
				[]ovsdb.Condition{{
					Column: "_uuid", Function: ovsdb.ConditionEqual, Value: ovsdb.UUID{GoUUID: bridgeUUID},
				}}, tt.row)
			errs, err := ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "update"}})
			require.NoErrorf(t, err, "%+v", errs)

			bridge.UUID = bridgeUUID
			row, err := o.db.Get(context.TODO(), "Open_vSwitch", "Bridge", bridgeUUID)
			assert.NoError(t, err)
			br := row.(*bridgeType)
			assert.NotEqual(t, br, bridgeRow)
			assert.Equal(t, tt.expected.Modify, updates["Bridge"][bridgeUUID].Modify)
		})
	}
}
