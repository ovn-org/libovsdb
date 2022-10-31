package database

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"

	. "github.com/ovn-org/libovsdb/test"
)

func TestWaitOpEquals(t *testing.T) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := OvsType{}
	info, err := dbModel.NewModelInfo(&ovs)
	require.NoError(t, err)
	ovsRow, err := m.NewRow(info)
	require.Nil(t, err)

	bridge := BridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "quux",
			"waldo": "fred",
		},
	}
	bridgeInfo, err := dbModel.NewModelInfo(&bridge)
	require.NoError(t, err)
	bridgeRow, err := m.NewRow(bridgeInfo)
	require.Nil(t, err)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)

	res, updates := transaction.Insert("Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := transaction.Insert("Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	timeout := 0
	// Attempt to wait for row with name foo to appear
	gotResult := transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name"},
		"==",
		[]ovsdb.Row{{"name": "foo"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Attempt to wait for 2 rows, where one does not exist
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name"},
		"==",
		[]ovsdb.Row{{"name": "foo"}, {"name": "blah"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.NotNil(t, err)

	extIDs, err := ovsdb.NewOvsMap(map[string]string{
		"foo":   "bar",
		"baz":   "quux",
		"waldo": "fred",
	})
	require.Nil(t, err)
	// Attempt to wait for a row, with multiple columns specified
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name", "external_ids"},
		"==",
		[]ovsdb.Row{{"name": "foo", "external_ids": extIDs}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Attempt to wait for a row, with multiple columns, but not specified in row filtering
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name", "external_ids"},
		"==",
		[]ovsdb.Row{{"name": "foo"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Attempt to get something with a non-zero timeout that will fail
	timeout = 400
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name", "external_ids"},
		"==",
		[]ovsdb.Row{{"name": "doesNotExist"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.NotNil(t, err)

}

func TestWaitOpNotEquals(t *testing.T) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := OvsType{}
	info, err := dbModel.NewModelInfo(&ovs)
	require.NoError(t, err)
	ovsRow, err := m.NewRow(info)
	require.Nil(t, err)

	bridge := BridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "quux",
			"waldo": "fred",
		},
	}
	bridgeInfo, err := dbModel.NewModelInfo(&bridge)
	require.NoError(t, err)
	bridgeRow, err := m.NewRow(bridgeInfo)
	require.Nil(t, err)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)

	res, updates := transaction.Insert("Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := transaction.Insert("Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	timeout := 0
	// Attempt a wait where no entry with name blah should exist
	gotResult := transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name"},
		"!=",
		[]ovsdb.Row{{"name": "blah"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Attempt another wait with multiple rows specified, one that would match, and one that doesn't
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name"},
		"!=",
		[]ovsdb.Row{{"name": "blah"}, {"name": "foo"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Attempt another wait where name would match, but ext ids would not match
	NoMatchExtIDs, err := ovsdb.NewOvsMap(map[string]string{
		"foo":   "bar",
		"baz":   "quux",
		"waldo": "is_different",
	})
	require.Nil(t, err)
	// Attempt to wait for a row, with multiple columns specified and one is not a match
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name", "external_ids"},
		"!=",
		[]ovsdb.Row{{"name": "foo", "external_ids": NoMatchExtIDs}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	require.Nil(t, err)

	// Check to see if a non match takes around the timeout
	start := time.Now()
	timeout = 200
	gotResult = transaction.Wait(
		"Bridge",
		&timeout,
		[]ovsdb.Condition{ovsdb.NewCondition("name", ovsdb.ConditionEqual, "foo")},
		[]string{"name"},
		"!=",
		[]ovsdb.Row{{"name": "foo"}},
	)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{gotResult}, []ovsdb.Operation{{Op: "wait"}})
	ts := time.Since(start)
	if ts < time.Duration(timeout)*time.Millisecond {
		t.Fatalf("Should have taken at least %d milliseconds to return, but it took %d instead", timeout, ts)
	}
	require.NotNil(t, err)
}

func TestMutateOp(t *testing.T) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := OvsType{}
	info, err := dbModel.NewModelInfo(&ovs)
	require.NoError(t, err)
	ovsRow, err := m.NewRow(info)
	require.Nil(t, err)

	bridge := BridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "quux",
			"waldo": "fred",
		},
	}
	bridgeInfo, err := dbModel.NewModelInfo(&bridge)
	require.NoError(t, err)
	bridgeRow, err := m.NewRow(bridgeInfo)
	require.Nil(t, err)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)

	res, updates := transaction.Insert("Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := transaction.Insert("Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	gotResult, gotUpdate := transaction.Mutate(
		"Open_vSwitch",
		[]ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: ovsUUID}),
		},
		[]ovsdb.Mutation{
			*ovsdb.NewMutation("bridges", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
	)
	assert.Equal(t, ovsdb.OperationResult{Count: 1}, gotResult)
	err = db.Commit("Open_vSwitch", uuid.New(), gotUpdate)
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
	gotResult, gotUpdate = transaction.Mutate(
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
	setAddDiffSingleValue := originSetAdd
	setAddDiffMultipleValues, _ := ovsdb.NewOvsSet([]interface{}{"foo", "bar"})

	originSetDel, _ := ovsdb.NewOvsSet([]interface{}{})
	setDelDiffSingleValue := originSetDel
	setDelDiffMultipleValues, _ := ovsdb.NewOvsSet([]interface{}{"foo"})

	originMap, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar"})

	originMapAdd, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar", "baz": "quux"})
	originMapAddDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"baz": "quux"})

	originMapDel, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{})
	originMapDelDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "bar"})

	originMapReplace, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "baz"})
	originMapReplaceDiff, _ := ovsdb.NewOvsMap(map[interface{}]interface{}{"foo": "baz"})

	singleValueSet := `{
		"type": {
			"key": {
				"type": "string"
			},
			"min": 0,
			"max": 1
		}
	}`
	singleValueSetSchema := ovsdb.ColumnSchema{}
	err := json.Unmarshal([]byte(singleValueSet), &singleValueSetSchema)
	require.NoError(t, err)

	multipleValueSet := `{
		"type": {
			"key": {
				"type": "string"
			},
			"min": 0,
			"max": 2
		}
	}`
	multipleValueSetSchema := ovsdb.ColumnSchema{}
	err = json.Unmarshal([]byte(multipleValueSet), &multipleValueSetSchema)
	require.NoError(t, err)

	tests := []struct {
		name     string
		schema   *ovsdb.ColumnSchema
		a        interface{}
		b        interface{}
		expected interface{}
	}{
		{
			"add to set, max == 1",
			&singleValueSetSchema,
			originSet,
			originSetAdd,
			setAddDiffSingleValue,
		},
		{
			"add to set, max > 1",
			&multipleValueSetSchema,
			originSet,
			originSetAdd,
			setAddDiffMultipleValues,
		},
		{
			"delete from set, max == 1",
			&singleValueSetSchema,
			originSet,
			originSetDel,
			setDelDiffSingleValue,
		},
		{
			"delete from set, max > 1",
			&multipleValueSetSchema,
			originSet,
			originSetDel,
			setDelDiffMultipleValues,
		},
		{
			"noop set, max == 1",
			&singleValueSetSchema,
			originSet,
			originSet,
			originSet,
		},
		{
			"noop set, max > 1",
			&multipleValueSetSchema,
			originSet,
			originSet,
			nil,
		},
		{
			"add to map",
			nil,
			originMap,
			originMapAdd,
			originMapAddDiff,
		},
		{
			"delete from map",
			nil,
			originMap,
			originMapDel,
			originMapDelDiff,
		},
		{
			"replace in map",
			nil,
			originMap,
			originMapReplace,
			originMapReplaceDiff,
		},
		{
			"noop map",
			nil,
			originMap,
			originMap,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := diff(tt.schema, tt.a, tt.b)
			assert.Equal(t, tt.expected, res)
		})
	}
}

func TestOvsdbServerInsert(t *testing.T) {
	t.Skip("need a helper for comparing rows as map elements aren't in same order")
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)
	m := mapper.NewMapper(schema)

	gromit := "gromit"
	bridge := BridgeType{
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
	bridgeInfo, err := dbModel.NewModelInfo(&bridge)
	require.NoError(t, err)
	bridgeRow, err := m.NewRow(bridgeInfo)
	require.Nil(t, err)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)

	res, updates := transaction.Insert("Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.NoError(t, err)

	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	assert.NoError(t, err)

	bridge.UUID = bridgeUUID
	br, err := db.Get("Open_vSwitch", "Bridge", bridgeUUID)
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
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)
	m := mapper.NewMapper(schema)

	christmas := "christmas"
	bridge := BridgeType{
		Name:       "foo",
		DatapathID: &christmas,
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "qux",
			"waldo": "fred",
		},
	}
	bridgeUUID := uuid.NewString()
	bridgeInfo, err := dbModel.NewModelInfo(&bridge)
	require.NoError(t, err)
	bridgeRow, err := m.NewRow(bridgeInfo)
	require.Nil(t, err)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)

	res, updates := transaction.Insert("Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.NoError(t, err)

	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	assert.NoError(t, err)

	halloween, _ := ovsdb.NewOvsSet([]string{"halloween"})
	emptySet, _ := ovsdb.NewOvsSet([]string{})
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
			"update single optional field, with direct value",
			ovsdb.Row{"datapath_id": "halloween"},
			&ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"datapath_id": halloween,
				},
			},
		},
		{
			"update single optional field, with set",
			ovsdb.Row{"datapath_id": halloween},
			&ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"datapath_id": halloween,
				},
			},
		},
		{
			"unset single optional field",
			ovsdb.Row{"datapath_id": emptySet},
			&ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"datapath_id": emptySet,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, updates := transaction.Update(
				"Bridge",
				[]ovsdb.Condition{{
					Column: "_uuid", Function: ovsdb.ConditionEqual, Value: ovsdb.UUID{GoUUID: bridgeUUID},
				}}, tt.row)
			errs, err := ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "update"}})
			require.NoErrorf(t, err, "%+v", errs)

			bridge.UUID = bridgeUUID
			row, err := db.Get("Open_vSwitch", "Bridge", bridgeUUID)
			assert.NoError(t, err)
			br := row.(*BridgeType)
			assert.NotEqual(t, br, bridgeRow)
			assert.Equal(t, tt.expected.Modify, updates["Bridge"][bridgeUUID].Modify)
		})
	}
}

func TestMultipleOps(t *testing.T) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)

	var ops []ovsdb.Operation
	var op ovsdb.Operation

	bridgeUUID := uuid.NewString()
	op = ovsdb.Operation{
		Op:       ovsdb.OperationInsert,
		Table:    "Bridge",
		UUIDName: bridgeUUID,
		Row: ovsdb.Row{
			"name": "a_bridge_to_nowhere",
		},
	}
	ops = append(ops, op)

	op = ovsdb.Operation{
		Op:    ovsdb.OperationUpdate,
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Row: ovsdb.Row{
			"ports": ovsdb.OvsSet{GoSet: []interface{}{ovsdb.UUID{GoUUID: "port1"}, ovsdb.UUID{GoUUID: "port10"}}},
		},
	}
	ops = append(ops, op)

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)
	results, _ := transaction.Transact(ops)
	assert.Len(t, results, len(ops))
	assert.NotNil(t, results[0])
	assert.Empty(t, results[0].Error)
	assert.Equal(t, 0, results[0].Count)
	assert.Equal(t, bridgeUUID, results[0].UUID.GoUUID)
	assert.NotNil(t, results[1])
	assert.Equal(t, 1, results[1].Count)
	assert.Empty(t, results[1].Error)

	ops = ops[:0]
	portA, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "portA"}})
	require.NoError(t, err)
	op = ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, portA),
		},
	}
	ops = append(ops, op)

	portBC, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "portB"}, {GoUUID: "portC"}})
	require.NoError(t, err)
	op2 := ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, portBC),
		},
	}
	ops = append(ops, op2)

	results, updates := transaction.Transact(ops)
	require.Len(t, results, len(ops))
	for _, result := range results {
		assert.Empty(t, result.Error)
		assert.Equal(t, 1, result.Count)
	}

	modifiedPorts, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "portA"}, {GoUUID: "portB"}, {GoUUID: "portC"}})
	assert.Nil(t, err)

	oldPorts, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "port1"}, {GoUUID: "port10"}})
	assert.Nil(t, err)

	newPorts, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "port1"}, {GoUUID: "port10"}, {GoUUID: "portA"}, {GoUUID: "portB"}, {GoUUID: "portC"}})
	assert.Nil(t, err)

	assert.Equal(t, ovsdb.TableUpdates2{
		"Bridge": ovsdb.TableUpdate2{
			bridgeUUID: &ovsdb.RowUpdate2{
				Modify: &ovsdb.Row{
					"ports": modifiedPorts,
				},
				Old: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: bridgeUUID},
					"name":  "a_bridge_to_nowhere",
					"ports": oldPorts,
				},
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: bridgeUUID},
					"name":  "a_bridge_to_nowhere",
					"ports": newPorts,
				},
			},
		},
	}, updates)

}

func TestOvsdbServerDbDoesNotExist(t *testing.T) {
	defDB, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &OvsType{},
		"Bridge":       &BridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": defDB})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, defDB)
	require.Empty(t, errs)

	ops := []ovsdb.Operation{
		{
			Op:       ovsdb.OperationInsert,
			Table:    "Bridge",
			UUIDName: uuid.NewString(),
			Row: ovsdb.Row{
				"name": "bridge",
			},
		},
		{
			Op:    ovsdb.OperationUpdate,
			Table: "Bridge",
			Where: []ovsdb.Condition{
				{
					Column:   "name",
					Function: ovsdb.ConditionEqual,
					Value:    "bridge",
				},
			},
			Row: ovsdb.Row{
				"datapath_type": "type",
			},
		},
	}

	transaction := NewTransaction(dbModel, "nonexsitent_db", db, nil)
	res, _ := transaction.Transact(ops)
	assert.Len(t, res, len(ops))
	assert.Equal(t, "database does not exist", res[0].Error)
	assert.Nil(t, res[1])
}

func TestCheckIndexes(t *testing.T) {
	clientDbModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch":              &OvsType{},
		"Bridge":                    &BridgeType{},
		"Flow_Sample_Collector_Set": &FlowSampleCollectorSetType{},
	})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := GetSchema()
	if err != nil {
		t.Fatal(err)
	}
	db := NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": clientDbModel})
	err = db.CreateDatabase("Open_vSwitch", schema)
	require.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, clientDbModel)
	require.Empty(t, errs)

	bridgeUUID := uuid.NewString()
	fscsUUID := uuid.NewString()
	ops := []ovsdb.Operation{
		{
			Table:    "Bridge",
			Op:       ovsdb.OperationInsert,
			UUIDName: bridgeUUID,
			Row: ovsdb.Row{
				"name": "a_bridge_to_nowhere",
			},
		},
		{
			Table:    "Flow_Sample_Collector_Set",
			Op:       ovsdb.OperationInsert,
			UUIDName: fscsUUID,
			Row: ovsdb.Row{
				"id":     1,
				"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
			},
		},
		{
			Table: "Flow_Sample_Collector_Set",
			Op:    ovsdb.OperationInsert,
			Row: ovsdb.Row{
				"id":     2,
				"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
			},
		},
	}

	transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)
	results, updates := transaction.Transact(ops)
	require.Len(t, results, len(ops))
	for _, result := range results {
		assert.Equal(t, "", result.Error)
	}
	err = db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	tests := []struct {
		desc        string
		ops         func() []ovsdb.Operation
		expectedErr string
	}{
		{
			"Inserting an existing database index should fail",
			func() []ovsdb.Operation {
				return []ovsdb.Operation{
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationInsert,
						Row: ovsdb.Row{
							"id":     1,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
					},
				}
			},
			"constraint violation",
		},
		{
			"Updating an index to an existing database index should fail",
			func() []ovsdb.Operation {
				return []ovsdb.Operation{
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationUpdate,
						Row: ovsdb.Row{
							"id":     2,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
						Where: []ovsdb.Condition{
							ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: fscsUUID}),
						},
					},
				}
			},
			"constraint violation",
		},
		{
			"Updating an index to an existing transaction index should fail",
			func() []ovsdb.Operation {
				return []ovsdb.Operation{
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationInsert,
						Row: ovsdb.Row{
							"id":     3,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
					},
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationUpdate,
						Row: ovsdb.Row{
							"id":     3,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
						Where: []ovsdb.Condition{
							ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: fscsUUID}),
						},
					},
				}
			},
			"constraint violation",
		},
		{
			"Updating an index to an old index that is updated in the same transaction should succeed",
			func() []ovsdb.Operation {
				return []ovsdb.Operation{
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationInsert,
						Row: ovsdb.Row{
							"id":     1,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
					},
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationUpdate,
						Row: ovsdb.Row{
							"id":     3,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
						Where: []ovsdb.Condition{
							ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: fscsUUID}),
						},
					},
				}
			},
			"",
		},
		{
			"Updating an index to a old index that is deleted in the same transaction should succeed",
			func() []ovsdb.Operation {
				return []ovsdb.Operation{
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationInsert,
						Row: ovsdb.Row{
							"id":     1,
							"bridge": ovsdb.UUID{GoUUID: bridgeUUID},
						},
					},
					{
						Table: "Flow_Sample_Collector_Set",
						Op:    ovsdb.OperationDelete,
						Where: []ovsdb.Condition{
							ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: fscsUUID}),
						},
					},
				}
			},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			transaction := NewTransaction(dbModel, "Open_vSwitch", db, nil)
			ops := tt.ops()
			results, _ := transaction.Transact(ops)
			var err string
			for _, result := range results {
				if result.Error != "" {
					err = result.Error
					break
				}
			}
			require.Equal(t, tt.expectedErr, err, "got a different error than expected")
			if tt.expectedErr != "" {
				require.Len(t, results, len(ops)+1)
			} else {
				require.Len(t, results, len(ops))
			}
		})
	}
}
