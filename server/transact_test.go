package server

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutateOp(t *testing.T) {
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
			"baz":   "qux",
			"waldo": "fred",
		},
	}
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert("Open_vSwitch", "Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := o.Insert("Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = o.db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	gotResult, gotUpdate := o.Mutate(
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

	bridgeSet, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: bridgeUUID}})
	assert.Nil(t, err)
	assert.Equal(t, ovsdb.TableUpdates2{
		"Open_vSwitch": ovsdb.TableUpdate2{
			ovsUUID: &ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					// TODO: _uuid should be filtered
					"_uuid":   ovsdb.UUID{GoUUID: ovsUUID},
					"bridges": bridgeSet,
				},
			},
		},
	}, gotUpdate)

	keyDelete, err := ovsdb.NewOvsSet([]string{"foo"})
	assert.Nil(t, err)
	keyValueDelete, err := ovsdb.NewOvsMap(map[string]string{"baz": "qux"})
	assert.Nil(t, err)
	gotResult, gotUpdate = o.Mutate(
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

	// oldExternalIds, err := ovsdb.NewOvsMap(bridge.ExternalIds)
	assert.Nil(t, err)
	newExternalIds, err := ovsdb.NewOvsMap(map[string]string{"waldo": "fred"})
	assert.Nil(t, err)
	assert.Equal(t, ovsdb.TableUpdates2{
		"Bridge": ovsdb.TableUpdate2{
			bridgeUUID: &ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"_uuid":        ovsdb.UUID{GoUUID: bridgeUUID},
					"name":         "foo",
					"external_ids": newExternalIds,
				},
			},
		},
	}, gotUpdate)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := diff(tt.a, tt.b)
			assert.Equal(t, tt.expected, res)
		})
	}
}
