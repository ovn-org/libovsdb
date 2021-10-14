package model

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/require"
)

var tableA = `
 "TableA": {
  "columns": {
    "aString": {
      "type": "string"
    },
    "aInteger": {
      "type": "integer"
    },
    "aSet": {
      "type": {
        "key": "string",
        "max": "unlimited",
        "min": 0
      }
    }
  }
}`
var tableB = `
 "TableB": {
  "columns": {
    "aString": {
      "type": "string"
    },
    "aInteger": {
      "type": "integer"
    },
    "aSet": {
      "type": {
        "key": "string",
        "max": "unlimited",
        "min": 0
      }
    }
  }
}`

var schema = ` {
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {` + tableA + "," + tableB + `
    }
  }
`

func TestNewDatabaseModel(t *testing.T) {

	tests := []struct {
		name            string
		schema          string
		requestTypes    map[string]Model
		compat          bool
		expectedCols    map[string][]string
		expectedNotCols map[string][]string
		err             bool
	}{
		{
			name:   "Fully matching model should succeed",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
				"TableB": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			expectedCols: map[string][]string{
				"TableA": {"aString", "aInteger", "aSet"},
				"TableB": {"aString", "aInteger", "aSet"},
			},
			compat: false,
			err:    false,
		},
		{
			name:   "Model with less tables should succeed",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			expectedCols: map[string][]string{
				"TableA": {"aString", "aInteger", "aSet"},
			},
			compat: false,
			err:    false,
		},
		{
			name:   "Model with less tables should succeed",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			expectedCols: map[string][]string{
				"TableA": {"aString", "aInteger", "aSet"},
			},
			compat: false,
			err:    false,
		},
		{
			name:   "Model more tables should fail",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
				"TableB": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
				"TableC": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			compat: false,
			err:    true,
		},
		{
			name:   "Model with more tables (compat) should succeed",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
				"TableB": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
				"TableC": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			expectedCols: map[string][]string{
				"TableA": {"aString", "aInteger", "aSet"},
				"TableB": {"aString", "aInteger", "aSet"},
			},
			expectedNotCols: map[string][]string{
				"TableC": {"aString", "aInteger", "aSet"},
			},
			compat: true,
			err:    false,
		},
		{
			name:   "Model with more columns should fail",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID   string   `ovsdb:"_uuid"`
					Foo    string   `ovsdb:"aString"`
					Bar    int      `ovsdb:"aInteger"`
					Baz    []string `ovsdb:"aSet"`
					FooBar []string `ovsdb:"aSecondSet"`
				}{},
				"TableB": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			compat: false,
			err:    true,
		},
		{
			name:   "Model with more columns (compat) should succeed",
			schema: schema,
			requestTypes: map[string]Model{
				"TableA": &struct {
					UUID   string   `ovsdb:"_uuid"`
					Foo    string   `ovsdb:"aString"`
					Bar    int      `ovsdb:"aInteger"`
					Baz    []string `ovsdb:"aSet"`
					FooBar []string `ovsdb:"aSecondSet"`
				}{},
				"TableB": &struct {
					UUID string   `ovsdb:"_uuid"`
					Foo  string   `ovsdb:"aString"`
					Bar  int      `ovsdb:"aInteger"`
					Baz  []string `ovsdb:"aSet"`
				}{},
			},
			expectedCols: map[string][]string{
				"TableA": {"aString", "aInteger", "aSet"},
				"TableB": {"aString", "aInteger", "aSet"},
			},
			expectedNotCols: map[string][]string{
				"TableA": {"aSecondSet"},
			},
			compat: false,
			err:    true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("NewDatabaseModel%s", tt.name), func(t *testing.T) {
			var schema ovsdb.DatabaseSchema
			err := json.Unmarshal([]byte(tt.schema), &schema)
			require.NoError(t, err)
			req, err := NewDatabaseModelRequest("TestSchema", tt.requestTypes)
			if tt.compat {
				req.SetCompatibility(true)
			}
			require.NoError(t, err)

			// Test single step creation
			db, errs := NewDatabaseModel(&schema, req)

			if tt.err {
				require.NotEmpty(t, errs)
			} else {
				require.Empty(t, errs)
				for table, cols := range tt.expectedCols {
					for _, col := range cols {
						require.Truef(t, db.HasColumn(table, col), "table %s column %s should be present in model", table, col)
					}
				}
				for table, cols := range tt.expectedNotCols {
					for _, col := range cols {
						require.Falsef(t, db.HasColumn(table, col), "table %s column %s should not be present in model", table, col)
					}
				}
			}

			// Test 2-step step creation
			db = NewPartialDatabaseModel(req)
			errs = db.SetSchema(&schema)

			if tt.err {
				require.NotEmpty(t, errs)
			} else {
				require.Empty(t, errs)
				for table, cols := range tt.expectedCols {
					for _, col := range cols {
						require.Truef(t, db.HasColumn(table, col), "table %s column %s should be present in model", table, col)
					}
				}
				for table, cols := range tt.expectedNotCols {
					for _, col := range cols {
						require.Falsef(t, db.HasColumn(table, col), "table %s column %s should not be present in model", table, col)
					}
				}
			}

		})
	}

}
