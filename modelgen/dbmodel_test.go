package modelgen

import (
	"encoding/json"
	"testing"
	"text/template"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDbModelTemplate(t *testing.T) {
	rawSchema := []byte(`
	{
		"name": "AtomicDB",
		"version": "0.0.0",
		"tables": {
			"atomicTable": {
				"columns": {
					"str": {
						"type": "string"
					},
					"int": {
						"type": "integer"
					},
					"float": {
						"type": "real"
					},
					"protocol": {
						"type": {"key": {"type": "string",
								 "enum": ["set", ["tcp", "udp", "sctp"]]},
								 "min": 0, "max": 1}},
					"event_type": {"type": {"key": {"type": "string",
													"enum": ["set", ["empty_lb_backends"]]}}}
				}
			}
		}
	}`)
	test := []struct {
		name      string
		extend    func(tmpl *template.Template, data map[string]interface{})
		expected  string
		err       bool
		formatErr bool
	}{
		{
			name: "normal",
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

import (
	"encoding/json"

	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// FullDatabaseModel returns the DatabaseModel object to be used in libovsdb
func FullDatabaseModel() (model.ClientDBModel, error) {
	return model.NewClientDBModel("AtomicDB", map[string]model.Model{
		"atomicTable": &AtomicTable{},
	}, nil)
}
` + `
var schema = ` + "`" + `{
  "name": "AtomicDB",
  "version": "0.0.0",
  "tables": {
    "atomicTable": {
      "columns": {
        "event_type": {
          "type": {
            "key": {
              "type": "string",
              "enum": "empty_lb_backends"
            }
          }
        },
        "float": {
          "type": "real"
        },
        "int": {
          "type": "integer"
        },
        "protocol": {
          "type": {
            "key": {
              "type": "string",
              "enum": [
                "set",
                [
                  "tcp",
                  "udp",
                  "sctp"
                ]
              ]
            },
            "min": 0,
            "max": 1
          }
        },
        "str": {
          "type": "string"
        }
      }
    }
  }
}` + "`" + `

func Schema() ovsdb.DatabaseSchema {
	var s ovsdb.DatabaseSchema
	err := json.Unmarshal([]byte(schema), &s)
	if err != nil {
		panic(err)
	}
	return s
}
`,
		},
	}
	var schema ovsdb.DatabaseSchema
	err := json.Unmarshal(rawSchema, &schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := NewDBTemplate()
			data := GetDBTemplateData("test", schema)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				g, err := NewGenerator()
				require.NoError(t, err)
				b, err := g.Format(tmpl, data)
				if tt.formatErr {
					assert.NotNil(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expected, string(b))
				}
			}
		})
	}
}
