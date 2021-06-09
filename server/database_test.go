package server

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutate(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"add int",
			1,
			ovsdb.MutateOperationAdd,
			1,
			2,
		},
		{
			"add float",
			1.0,
			ovsdb.MutateOperationAdd,
			1.0,
			2.0,
		},
		{
			"add float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationAdd,
			1.0,
			[]float64{2.0, 3.0, 4.0},
		},
		{
			"add int set float",
			[]int{1, 2, 3},
			ovsdb.MutateOperationAdd,
			1,
			[]int{2, 3, 4},
		},
		{
			"subtract int",
			1,
			ovsdb.MutateOperationSubtract,
			1,
			0,
		},
		{
			"subtract float",
			1.0,
			ovsdb.MutateOperationSubtract,
			1.0,
			0.0,
		},
		{
			"subtract float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationSubtract,
			1.0,
			[]float64{0.0, 1.0, 2.0},
		},
		{
			"subtract int set",
			[]int{1, 2, 3},
			ovsdb.MutateOperationSubtract,
			1,
			[]int{0, 1, 2},
		},
		{
			"multiply int",
			1,
			ovsdb.MutateOperationMultiply,
			2,
			2,
		},
		{
			"multiply float",
			1.0,
			ovsdb.MutateOperationMultiply,
			2.0,
			2.0,
		},
		{
			"multiply float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationMultiply,
			2.0,
			[]float64{2.0, 4.0, 6.0},
		},
		{
			"multiply int set",
			[]int{1, 2, 3},
			ovsdb.MutateOperationMultiply,
			2,
			[]int{2, 4, 6},
		},
		{
			"divide int",
			10,
			ovsdb.MutateOperationDivide,
			2,
			5,
		},
		{
			"divide float",
			1.0,
			ovsdb.MutateOperationDivide,
			2.0,
			0.5,
		},
		{
			"divide float set",
			[]float64{1.0, 2.0, 4.0},
			ovsdb.MutateOperationDivide,
			2.0,
			[]float64{0.5, 1.0, 2.0},
		},
		{
			"divide int set",
			[]int{10, 20, 30},
			ovsdb.MutateOperationDivide,
			5,
			[]int{2, 4, 6},
		},
		{
			"modulo int",
			3,
			ovsdb.MutateOperationModulo,
			2,
			1,
		},
		{
			"modulo int set",
			[]int{3, 5, 7},
			ovsdb.MutateOperationModulo,
			2,
			[]int{1, 1, 1},
		},
		{
			"insert single string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationInsert,
			"baz",
			[]string{"foo", "bar", "baz"},
		},
		{
			"insert multiple string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationInsert,
			[]string{"baz", "quux"},
			[]string{"foo", "bar", "baz", "quux"},
		},
		{
			"delete single string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationDelete,
			"bar",
			[]string{"foo"},
		},
		{
			"delete multiple string",
			[]string{"foo", "bar", "baz"},
			ovsdb.MutateOperationDelete,
			[]string{"bar", "baz"},
			[]string{"foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateOp(t *testing.T) {
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &ovsType{},
		"Bridge":       &bridgeType{}})
	require.Nil(t, err)
	db := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	schema, err := getSchema()
	require.Nil(t, err)

	err = db.CreateDatabase("Open_vSwitch", schema)
	require.Nil(t, err)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := ovsType{}
	ovsRow, err := m.NewRow("Open_vSwitch", &ovs)
	require.Nil(t, err)

	bridge := bridgeType{
		Name: "foo",
	}
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, _ := db.Insert("Open_vSwitch", "Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, _ = db.Insert("Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	gotResult, gotUpdate := db.Mutate(
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
	assert.Equal(t, ovsdb.TableUpdates{
		"Open_vSwitch": ovsdb.TableUpdate{
			ovsUUID: &ovsdb.RowUpdate{
				Old: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: ovsUUID},
				},
				New: &ovsdb.Row{
					"_uuid":   ovsdb.UUID{GoUUID: ovsUUID},
					"bridges": bridgeSet,
				},
			},
		},
	}, gotUpdate)
}
