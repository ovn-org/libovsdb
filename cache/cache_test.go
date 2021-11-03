package cache

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testModel struct {
	UUID string `ovsdb:"_uuid"`
	Foo  string `ovsdb:"foo"`
	Bar  string `ovsdb:"bar"`
}

func TestRowCache_Row(t *testing.T) {

	type fields struct {
		cache map[string]model.Model
	}
	type args struct {
		uuid string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   model.Model
	}{
		{
			"returns a row that exists",
			fields{cache: map[string]model.Model{"test": &testModel{}}},
			args{uuid: "test"},
			&testModel{},
		},
		{
			"returns a nil for a row that does not exist",
			fields{cache: map[string]model.Model{"test": &testModel{}}},
			args{uuid: "foo"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.fields.cache,
			}
			got := r.Row(tt.args.uuid)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowCache_Rows(t *testing.T) {
	tests := []struct {
		name  string
		cache map[string]model.Model
		want  map[string]model.Model
	}{
		{
			"returns a rows that exist",
			map[string]model.Model{"test1": &testModel{}, "test2": &testModel{}, "test3": &testModel{}},
			map[string]model.Model{"test1": &testModel{}, "test2": &testModel{}, "test3": &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.cache,
			}
			got := r.Rows()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowCacheCreate(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.Nil(t, err)
	testData := Data{
		"Open_vSwitch": map[string]model.Model{"bar": &testModel{Foo: "bar"}},
	}

	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.Nil(t, err)

	tests := []struct {
		name       string
		uuid       string
		model      *testModel
		checkIndex bool
		wantErr    bool
	}{
		{
			"inserts a new row",
			"foo",
			&testModel{Foo: "foo"},
			true,
			false,
		},
		{
			"error duplicate uuid",
			"bar",
			&testModel{Foo: "foo"},
			true,
			true,
		},
		{
			"error duplicate index",
			"baz",
			&testModel{Foo: "bar"},
			true,
			true,
		},
		{
			"error duplicate uuid, no index check",
			"bar",
			&testModel{Foo: "bar"},
			false,
			true,
		},
		{
			"no error duplicate index",
			"baz",
			&testModel{Foo: "bar"},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := tc.Table("Open_vSwitch")
			require.NotNil(t, rc)
			err := rc.Create(tt.uuid, tt.model, tt.checkIndex)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.uuid, rc.indexes["foo"][tt.model.Foo])
			}
		})
	}
}

func TestRowCacheCreateMultiIndex(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo", "bar"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.Nil(t, err)
	testData := Data{
		"Open_vSwitch": map[string]model.Model{"bar": &testModel{Foo: "bar", Bar: "bar"}},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.Nil(t, err)
	tests := []struct {
		name               string
		uuid               string
		model              *testModel
		wantErr            bool
		wantIndexExistsErr bool
	}{
		{
			"inserts a new row",
			"foo",
			&testModel{Foo: "foo", Bar: "foo"},
			false,
			false,
		},
		{
			"error duplicate uuid",
			"bar",
			&testModel{Foo: "bar", Bar: "bar"},
			true,
			false,
		},
		{
			"error duplicate index",
			"baz",
			&testModel{Foo: "foo", Bar: "foo"},
			true,
			true,
		},
		{
			"new row with one duplicate value",
			"baz",
			&testModel{Foo: "foo", Bar: "bar"},
			false,
			false,
		},
		{
			"new row with other duplicate value",
			"quux",
			&testModel{Foo: "bar", Bar: "baz"},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := tc.Table("Open_vSwitch")
			require.NotNil(t, rc)
			err := rc.Create(tt.uuid, tt.model, true)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantIndexExistsErr {
					assert.IsType(t, &ErrIndexExists{}, err)
				}
			} else {
				assert.Nil(t, err)
				mapperInfo, err := dbModel.NewModelInfo(tt.model)
				require.Nil(t, err)
				h, err := valueFromIndex(mapperInfo, newIndex("foo", "bar"))
				require.Nil(t, err)
				assert.Equal(t, tt.uuid, rc.indexes["foo,bar"][h])
			}
		})
	}
}

func TestRowCacheUpdate(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.Nil(t, err)
	testData := Data{
		"Open_vSwitch": map[string]model.Model{
			"bar":    &testModel{Foo: "bar"},
			"foobar": &testModel{Foo: "foobar"},
		},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.Nil(t, err)

	tests := []struct {
		name       string
		uuid       string
		model      *testModel
		checkIndex bool
		wantErr    bool
	}{
		{
			"error if row does not exist",
			"foo",
			&testModel{Foo: "foo"},
			true,
			true,
		},
		{
			"update",
			"bar",
			&testModel{Foo: "baz"},
			true,
			false,
		},
		{
			"error new index would cause duplicate",
			"bar",
			&testModel{Foo: "foobar"},
			true,
			true,
		},
		{
			"no error new index would cause duplicate",
			"bar",
			&testModel{Foo: "foobar"},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := tc.Table("Open_vSwitch")
			require.NotNil(t, rc)
			err := rc.Update(tt.uuid, tt.model, tt.checkIndex)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.uuid, rc.indexes["foo"][tt.model.Foo])
			}
		})
	}
}

func TestRowCacheUpdateMultiIndex(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo", "bar"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.Nil(t, err)
	testData := Data{
		"Open_vSwitch": map[string]model.Model{
			"bar":    &testModel{Foo: "bar", Bar: "bar"},
			"foobar": &testModel{Foo: "foobar", Bar: "foobar"},
		},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	assert.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.Nil(t, err)

	tests := []struct {
		name    string
		uuid    string
		model   *testModel
		wantErr bool
	}{
		{
			"error if row does not exist",
			"foo",
			&testModel{Foo: "foo", Bar: "foo"},
			true,
		},
		{
			"update both index cols",
			"bar",
			&testModel{Foo: "baz", Bar: "baz"},
			false,
		},
		{
			"update single index col",
			"bar",
			&testModel{Foo: "baz", Bar: "quux"},
			false,
		},
		{
			"error new index would cause duplicate",
			"baz",
			&testModel{Foo: "foobar", Bar: "foobar"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := tc.Table("Open_vSwitch")
			require.NotNil(t, rc)
			err := rc.Update(tt.uuid, tt.model, true)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				mapperInfo, err := dbModel.NewModelInfo(tt.model)
				require.Nil(t, err)
				h, err := valueFromIndex(mapperInfo, newIndex("foo", "bar"))
				require.Nil(t, err)
				assert.Equal(t, tt.uuid, rc.indexes["foo,bar"][h])
			}
		})
	}
}

func TestRowCacheDelete(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.Nil(t, err)
	testData := Data{
		"Open_vSwitch": map[string]model.Model{
			"bar": &testModel{Foo: "bar"},
		},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.Nil(t, err)

	tests := []struct {
		name    string
		uuid    string
		model   *testModel
		wantErr bool
	}{
		{
			"deletes a row",
			"bar",
			&testModel{Foo: "bar"},
			false,
		},
		{
			"error if row does not exist",
			"foobar",
			&testModel{Foo: "bar"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := tc.Table("Open_vSwitch")
			require.NotNil(t, rc)
			err := rc.Delete(tt.uuid)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.Nil(t, err)
				assert.Equal(t, "", rc.indexes["foo"][tt.model.Foo])
			}
		})
	}
}

func TestEventHandlerFuncs_OnAdd(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row model.Model)
		UpdateFunc func(table string, old model.Model, new model.Model)
		DeleteFunc func(table string, row model.Model)
	}
	type args struct {
		table string
		row   model.Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}},
		},
		{
			"calls onadd function",
			fields{func(string, model.Model) {
				calls++
			}, nil, nil},
			args{"testTable", &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnAdd(tt.args.table, tt.args.row)
			if e.AddFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnUpdate(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row model.Model)
		UpdateFunc func(table string, old model.Model, new model.Model)
		DeleteFunc func(table string, row model.Model)
	}
	type args struct {
		table string
		old   model.Model
		new   model.Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}, &testModel{}},
		},
		{
			"calls onupdate function",
			fields{nil, func(string, model.Model, model.Model) {
				calls++
			}, nil},
			args{"testTable", &testModel{}, &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnUpdate(tt.args.table, tt.args.old, tt.args.new)
			if e.UpdateFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnDelete(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row model.Model)
		UpdateFunc func(table string, old model.Model, new model.Model)
		DeleteFunc func(table string, row model.Model)
	}
	type args struct {
		table string
		row   model.Model
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", &testModel{}},
		},
		{
			"calls ondelete function",
			fields{nil, nil, func(string, model.Model) {
				calls++
			}},
			args{"testTable", &testModel{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnDelete(tt.args.table, tt.args.row)
			if e.DeleteFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestTableCacheTable(t *testing.T) {
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tests := []struct {
		name  string
		cache map[string]*RowCache
		table string
		want  *RowCache
	}{
		{
			"returns nil for an empty table",
			map[string]*RowCache{"Open_vSwitch": newRowCache("Open_vSwitch", dbModel, nil)},
			"foo",
			nil,
		},
		{
			"returns valid row cache for valid table",
			map[string]*RowCache{"Open_vSwitch": newRowCache("Open_vSwitch", dbModel, nil)},
			"Open_vSwitch",
			newRowCache("Open_vSwitch", dbModel, nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.cache,
			}
			got := tr.Table(tt.table)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTableCacheTables(t *testing.T) {
	db, err := model.NewClientDBModel("TestDB",
		map[string]model.Model{
			"test1": &testModel{},
			"test2": &testModel{},
			"test3": &testModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "TestDB",
		  "tables": {
		    "test1": {
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    },
		    "test2": {
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    },
		    "test3": {
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tests := []struct {
		name  string
		cache map[string]*RowCache
		want  []string
	}{
		{
			"returns a table that exists",
			map[string]*RowCache{
				"test1": newRowCache("test1", dbModel, nil),
				"test2": newRowCache("test2", dbModel, nil),
				"test3": newRowCache("test3", dbModel, nil),
			},
			[]string{"test1", "test2", "test3"},
		},
		{
			"returns an empty slice if no tables exist",
			map[string]*RowCache{},
			[]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.cache,
			}
			got := tr.Tables()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestTableCache_populate(t *testing.T) {
	t.Log("Create")
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, nil, nil)
	assert.Nil(t, err)

	testRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "bar"})
	testRowModel := &testModel{UUID: "test", Foo: "bar"}
	updates := ovsdb.TableUpdates{
		"Open_vSwitch": {
			"test": &ovsdb.RowUpdate{
				Old: nil,
				New: &testRow,
			},
		},
	}
	err = tc.Populate(updates)
	require.NoError(t, err)

	got := tc.Table("Open_vSwitch").Row("test")
	assert.Equal(t, testRowModel, got)

	t.Log("Update")
	updatedRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "quux"})
	updatedRowModel := &testModel{UUID: "test", Foo: "quux"}
	updates["Open_vSwitch"]["test"] = &ovsdb.RowUpdate{
		Old: &testRow,
		New: &updatedRow,
	}
	err = tc.Populate(updates)
	require.NoError(t, err)

	got = tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, updatedRowModel, got)

	t.Log("Delete")
	updates["Open_vSwitch"]["test"] = &ovsdb.RowUpdate{
		Old: &updatedRow,
		New: nil,
	}

	err = tc.Populate(updates)
	require.NoError(t, err)

	_, ok := tc.cache["Open_vSwitch"].cache["test"]
	assert.False(t, ok)
}

func TestTableCachePopulate(t *testing.T) {
	t.Log("Create")
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, nil, nil)
	assert.Nil(t, err)

	testRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "bar"})
	testRowModel := &testModel{UUID: "test", Foo: "bar"}
	updates := ovsdb.TableUpdates{
		"Open_vSwitch": {
			"test": &ovsdb.RowUpdate{
				Old: nil,
				New: &testRow,
			},
		},
	}
	err = tc.Populate(updates)
	require.NoError(t, err)

	got := tc.Table("Open_vSwitch").Row("test")
	assert.Equal(t, testRowModel, got)

	t.Log("Update")
	updatedRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "quux"})
	updatedRowModel := &testModel{UUID: "test", Foo: "quux"}
	updates["Open_vSwitch"]["test"] = &ovsdb.RowUpdate{
		Old: &testRow,
		New: &updatedRow,
	}
	err = tc.Populate(updates)
	require.NoError(t, err)

	got = tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, updatedRowModel, got)

	t.Log("Delete")
	updates["Open_vSwitch"]["test"] = &ovsdb.RowUpdate{
		Old: &updatedRow,
		New: nil,
	}

	err = tc.Populate(updates)
	require.NoError(t, err)

	_, ok := tc.cache["Open_vSwitch"].cache["test"]
	assert.False(t, ok)
}

func TestTableCachePopulate2(t *testing.T) {
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, nil, nil)
	assert.Nil(t, err)

	testRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "bar"})
	testRowModel := &testModel{UUID: "test", Foo: "bar"}
	updates := ovsdb.TableUpdates2{
		"Open_vSwitch": {
			"test": &ovsdb.RowUpdate2{
				Initial: &testRow,
			},
		},
	}

	t.Log("Initial")
	err = tc.Populate2(updates)
	require.NoError(t, err)
	got := tc.Table("Open_vSwitch").Row("test")
	assert.Equal(t, testRowModel, got)

	t.Log("Insert")
	testRow2 := ovsdb.Row(map[string]interface{}{"_uuid": "test2", "foo": "bar2"})
	testRowModel2 := &testModel{UUID: "test2", Foo: "bar2"}
	updates = ovsdb.TableUpdates2{
		"Open_vSwitch": {
			"test2": &ovsdb.RowUpdate2{
				Insert: &testRow2,
			},
		},
	}
	err = tc.Populate2(updates)
	require.NoError(t, err)
	got = tc.Table("Open_vSwitch").Row("test2")
	assert.Equal(t, testRowModel2, got)

	t.Log("Update")
	updatedRow := ovsdb.Row(map[string]interface{}{"foo": "quux"})
	updatedRowModel := &testModel{UUID: "test", Foo: "quux"}
	updates = ovsdb.TableUpdates2{
		"Open_vSwitch": {
			"test": &ovsdb.RowUpdate2{
				Modify: &updatedRow,
			},
		},
	}
	err = tc.Populate2(updates)
	require.NoError(t, err)
	got = tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, updatedRowModel, got)

	t.Log("Delete")
	deletedRow := ovsdb.Row(map[string]interface{}{"_uuid": "test", "foo": "quux"})
	updates = ovsdb.TableUpdates2{
		"Open_vSwitch": {
			"test": &ovsdb.RowUpdate2{
				Delete: &deletedRow,
			},
		},
	}
	err = tc.Populate2(updates)
	require.NoError(t, err)
	_, ok := tc.cache["Open_vSwitch"].cache["test"]
	assert.False(t, ok)
}

func TestEventProcessor_AddEvent(t *testing.T) {
	logger := logr.Discard()
	ep := newEventProcessor(16, &logger)
	var events []event
	for i := 0; i < 17; i++ {
		events = append(events, event{
			table:     "bridge",
			eventType: addEvent,
			new: &testModel{
				UUID: "unique",
				Foo:  "bar",
			},
		})
	}
	// overfill channel so event 16 is dropped
	for _, e := range events {
		ep.AddEvent(e.eventType, e.table, nil, e.new)
	}
	// assert channel is full of events
	assert.Equal(t, 16, len(ep.events))

	// read events and ensure they are in FIFO order
	for i := 0; i < 16; i++ {
		event := <-ep.events
		assert.Equal(t, &testModel{UUID: "unique", Foo: "bar"}, event.new)
	}

	// assert channel is empty
	assert.Equal(t, 0, len(ep.events))
}

func TestIndex(t *testing.T) {
	type indexTestModel struct {
		UUID string `ovsdb:"_uuid"`
		Foo  string `ovsdb:"foo"`
		Bar  string `ovsdb:"bar"`
		Baz  int    `ovsdb:"baz"`
	}
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &indexTestModel{}}, nil)
	assert.Nil(t, err)
	var schema ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"], ["bar","baz"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			},
			"baz": {
				"type": "integer"
			}
		      }
		    }
		 }
	     }
	`), &schema)
	assert.Nil(t, err)
	dbModel, errs := model.NewDatabaseModel(schema, db)
	assert.Empty(t, errs)
	tc, err := NewTableCache(dbModel, nil, nil)
	assert.Nil(t, err)
	obj := &indexTestModel{
		UUID: "test1",
		Foo:  "foo",
		Bar:  "bar",
		Baz:  42,
	}
	table := tc.Table("Open_vSwitch")

	err = table.Create(obj.UUID, obj, true)
	assert.Nil(t, err)
	t.Run("Index by single column", func(t *testing.T) {
		idx, err := table.Index("foo")
		assert.Nil(t, err)
		info, err := dbModel.NewModelInfo(obj)
		assert.Nil(t, err)
		v, err := valueFromIndex(info, newIndex("foo"))
		assert.Nil(t, err)
		assert.Equal(t, idx[v], obj.UUID)
	})
	t.Run("Index by single column miss", func(t *testing.T) {
		idx, err := table.Index("foo")
		assert.Nil(t, err)
		obj2 := obj
		obj2.Foo = "wrong"
		assert.Nil(t, err)
		info, err := dbModel.NewModelInfo(obj2)
		assert.Nil(t, err)
		v, err := valueFromIndex(info, newIndex("foo"))
		assert.Nil(t, err)
		_, ok := idx[v]
		assert.False(t, ok)
	})
	t.Run("Index by single column wrong", func(t *testing.T) {
		_, err := table.Index("wrong")
		assert.NotNil(t, err)
	})
	t.Run("Index by multi-column wrong", func(t *testing.T) {
		_, err := table.Index("bar", "wrong")
		assert.NotNil(t, err)
	})
	t.Run("Index by multi-column", func(t *testing.T) {
		idx, err := table.Index("bar", "baz")
		assert.Nil(t, err)
		info, err := dbModel.NewModelInfo(obj)
		assert.Nil(t, err)
		v, err := valueFromIndex(info, newIndex("bar", "baz"))
		assert.Nil(t, err)
		assert.Equal(t, idx[v], obj.UUID)
	})
	t.Run("Index by multi-column miss", func(t *testing.T) {
		idx, err := table.Index("bar", "baz")
		assert.Nil(t, err)
		obj2 := obj
		obj2.Baz++
		info, err := dbModel.NewModelInfo(obj)
		assert.Nil(t, err)
		v, err := valueFromIndex(info, newIndex("bar", "baz"))
		assert.Nil(t, err)
		_, ok := idx[v]
		assert.False(t, ok)
	})
	t.Run("Index type", func(t *testing.T) {
		idx := newIndex("foo", "bar")
		assert.Equal(t, idx.columns(), []string{"foo", "bar"})
	})
}

func TestTableCacheRowByModelSingleIndex(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.NoError(t, err)
	myFoo := &testModel{Foo: "foo", Bar: "foo"}
	testData := Data{
		"Open_vSwitch": map[string]model.Model{
			"foo": myFoo,
			"bar": &testModel{Foo: "bar", Bar: "bar"},
		},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.NoError(t, err)

	t.Run("get foo by index", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "foo"})
		assert.NotNil(t, foo)
		assert.Equal(t, myFoo, foo)
	})

	t.Run("get non-existent item by index", func(t *testing.T) {
		baz := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "baz"})
		assert.Nil(t, baz)
	})

	t.Run("no index data", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Bar: "foo"})
		assert.Nil(t, foo)
	})
}

func TestTableCacheRowByModelTwoIndexes(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo"], ["bar"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.NoError(t, err)
	myFoo := &testModel{Foo: "foo", Bar: "foo"}
	testData := Data{
		"Open_vSwitch": map[string]model.Model{
			"foo": myFoo,
			"bar": &testModel{Foo: "bar", Bar: "bar"},
		},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.NoError(t, err)

	t.Run("get foo by Foo index", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "foo"})
		assert.NotNil(t, foo)
		assert.Equal(t, myFoo, foo)
	})

	t.Run("get foo by Bar index", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Bar: "foo"})
		assert.NotNil(t, foo)
		assert.Equal(t, myFoo, foo)
	})

	t.Run("get non-existent item by index", func(t *testing.T) {
		baz := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "baz"})
		assert.Nil(t, baz)
	})

}

func TestTableCacheRowByModelMultiIndex(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testModel{}}, nil)
	require.Nil(t, err)
	err = json.Unmarshal([]byte(`
		 {"name": "Open_vSwitch",
		  "tables": {
		    "Open_vSwitch": {
			  "indexes": [["foo", "bar"]],
		      "columns": {
		        "foo": {
			  "type": "string"
			},
			"bar": {
				"type": "string"
			  }
		      }
		    }
		 }
	     }
	`), &schema)
	require.NoError(t, err)
	myFoo := &testModel{Foo: "foo", Bar: "foo"}
	testData := Data{
		"Open_vSwitch": map[string]model.Model{"foo": myFoo, "bar": &testModel{Foo: "bar", Bar: "bar"}},
	}
	dbModel, errs := model.NewDatabaseModel(schema, db)
	require.Empty(t, errs)
	tc, err := NewTableCache(dbModel, testData, nil)
	require.NoError(t, err)

	t.Run("incomplete index", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "foo"})
		assert.Nil(t, foo)
	})

	t.Run("get foo by index", func(t *testing.T) {
		foo := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "foo", Bar: "foo"})
		assert.NotNil(t, foo)
		assert.Equal(t, myFoo, foo)
	})

	t.Run("get non-existent item by index", func(t *testing.T) {
		baz := tc.Table("Open_vSwitch").RowByModel(&testModel{Foo: "baz", Bar: "baz"})
		assert.Nil(t, baz)
	})
}

func TestTableCacheApplyModifications(t *testing.T) {
	type testDBModel struct {
		UUID  string            `ovsdb:"_uuid"`
		Value string            `ovsdb:"value"`
		Set   []string          `ovsdb:"set"`
		Map   map[string]string `ovsdb:"map"`
		Map2  map[string]string `ovsdb:"map2"`
		Ptr   *string           `ovsdb:"ptr"`
	}
	aEmptySet, _ := ovsdb.NewOvsSet([]string{})
	aFooSet, _ := ovsdb.NewOvsSet([]string{"foo"})
	aFooBarSet, _ := ovsdb.NewOvsSet([]string{"foo", "bar"})
	aFooMap, _ := ovsdb.NewOvsMap(map[string]string{"foo": "bar"})
	aBarMap, _ := ovsdb.NewOvsMap(map[string]string{"bar": "baz"})
	aBarBazMap, _ := ovsdb.NewOvsMap(map[string]string{
		"bar": "baz",
		"baz": "quux",
	})
	wallace := "wallace"
	aWallaceSet, _ := ovsdb.NewOvsSet([]string{wallace})
	gromit := "gromit"
	aWallaceGromitSet, _ := ovsdb.NewOvsSet([]string{wallace, gromit})
	tests := []struct {
		name     string
		update   ovsdb.Row
		base     *testDBModel
		expected *testDBModel
	}{
		{
			"replace value",
			ovsdb.Row{"value": "bar"},
			&testDBModel{Value: "foo"},
			&testDBModel{Value: "bar"},
		},
		{
			"noop",
			ovsdb.Row{"value": "bar"},
			&testDBModel{Value: "bar"},
			&testDBModel{Value: "bar"},
		},
		{
			"add to set",
			ovsdb.Row{"set": aFooSet},
			&testDBModel{Set: []string{}},
			&testDBModel{Set: []string{"foo"}},
		},
		{
			"remove from set",
			ovsdb.Row{"set": aFooSet},
			&testDBModel{Set: []string{"foo"}},
			&testDBModel{Set: []string{}},
		},
		{
			"add and remove from set",
			ovsdb.Row{"set": aFooBarSet},
			&testDBModel{Set: []string{"foo"}},
			&testDBModel{Set: []string{"bar"}},
		},
		{
			"replace map value",
			ovsdb.Row{"map": aFooMap},
			&testDBModel{Map: map[string]string{"foo": "baz"}},
			&testDBModel{Map: map[string]string{"foo": "bar"}},
		},
		{
			"add map key",
			ovsdb.Row{"map": aBarMap},
			&testDBModel{Map: map[string]string{"foo": "bar"}},
			&testDBModel{Map: map[string]string{"foo": "bar", "bar": "baz"}},
		},
		{
			"delete map key",
			ovsdb.Row{"map": aFooMap},
			&testDBModel{Map: map[string]string{"foo": "bar"}},
			&testDBModel{Map: nil},
		},
		{
			"multiple map operations",
			ovsdb.Row{"map": aBarBazMap, "map2": aFooMap},
			&testDBModel{Map: map[string]string{"foo": "bar"}},
			&testDBModel{
				Map:  map[string]string{"foo": "bar", "bar": "baz", "baz": "quux"},
				Map2: map[string]string{"foo": "bar"},
			},
		},
		{
			"set optional value",
			ovsdb.Row{"ptr": aWallaceSet},
			&testDBModel{Ptr: nil},
			&testDBModel{Ptr: &wallace},
		},
		{
			"replace optional value",
			ovsdb.Row{"ptr": aWallaceGromitSet},
			&testDBModel{Ptr: &wallace},
			&testDBModel{Ptr: &gromit},
		},
		{
			"delete optional value",
			ovsdb.Row{"ptr": aEmptySet},
			&testDBModel{Ptr: &wallace},
			&testDBModel{Ptr: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{"Open_vSwitch": &testDBModel{}}, nil)
			assert.Nil(t, err)
			var schema ovsdb.DatabaseSchema
			err = json.Unmarshal([]byte(`
			  {
				"name": "Open_vSwitch",
				"tables": {
				  "Open_vSwitch": {
				    "indexes": [["foo"]],
					"columns": {
					  "value": { "type": "string" },
					  "set": { "type": { "key": { "type": "string" }, "min": 0,	"max": "unlimited" } },
					  "map": { "type": { "key": "string", "max": "unlimited", "min": 0, "value": "string" } },
					  "map2": { "type": { "key": "string", "max": "unlimited", "min": 0, "value": "string" } },
					  "ptr": { "type": { "key": { "type": "string" }, "min": 0,	"max": 1 } }
					}
				  }
				}
			  }
			`), &schema)
			require.NoError(t, err)
			dbModel, errs := model.NewDatabaseModel(schema, db)
			require.Empty(t, errs)
			tc, err := NewTableCache(dbModel, nil, nil)
			assert.Nil(t, err)
			original := model.Clone(tt.base).(*testDBModel)
			err = tc.ApplyModifications("Open_vSwitch", original, tt.update)
			require.NoError(t, err)
			require.Equal(t, tt.expected, original)
			if !reflect.DeepEqual(tt.expected, tt.base) {
				require.NotEqual(t, tt.base, original)
			}
		})
	}
}
