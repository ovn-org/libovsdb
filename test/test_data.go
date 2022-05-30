package test

import (
	"encoding/json"

	"github.com/ovn-org/libovsdb/ovsdb"
)

const schema = `
{
    "name": "Open_vSwitch",
    "version": "0.0.1",
    "tables": {
        "Open_vSwitch": {
            "columns": {
                "bridges": {
                    "type": {
                        "key": {
                            "type": "uuid",
                            "refTable": "Bridge"
                        },
                        "min": 0,
                        "max": "unlimited"
                    }
                }
            },
            "isRoot": true,
            "maxRows": 1
        },
        "Bridge": {
            "columns": {
                "name": {
                    "type": "string",
                    "mutable": false
                },
                "datapath_type": {
                    "type": "string"
                },
                "datapath_id": {
                    "type": {
                        "key": "string",
                        "min": 0,
                        "max": 1
                    },
                    "ephemeral": true
                },
                "ports": {
                    "type": {
                        "key": {
                            "type": "uuid",
                            "refTable": "Port"
                        },
                        "min": 0,
                        "max": "unlimited"
                    }
                },
                "status": {
                    "type": {
                        "key": "string",
                        "value": "string",
                        "min": 0,
                        "max": "unlimited"
                    },
                    "ephemeral": true
                },
                "other_config": {
                    "type": {
                        "key": "string",
                        "value": "string",
                        "min": 0,
                        "max": "unlimited"
                    }
                },
                "external_ids": {
                    "type": {
                        "key": "string",
                        "value": "string",
                        "min": 0,
                        "max": "unlimited"
                    }
                }
            },
            "indexes": [
                [
                    "name"
                ]
            ]
        }
    }
}
`

// BridgeType is the simplified ORM model of the Bridge table
type BridgeType struct {
	UUID         string            `ovsdb:"_uuid"`
	Name         string            `ovsdb:"name"`
	DatapathType string            `ovsdb:"datapath_type"`
	DatapathID   *string           `ovsdb:"datapath_id"`
	OtherConfig  map[string]string `ovsdb:"other_config"`
	ExternalIds  map[string]string `ovsdb:"external_ids"`
	Ports        []string          `ovsdb:"ports"`
	Status       map[string]string `ovsdb:"status"`
}

// OvsType is the simplified ORM model of the Bridge table
type OvsType struct {
	UUID    string   `ovsdb:"_uuid"`
	Bridges []string `ovsdb:"bridges"`
}

func GetSchema() (ovsdb.DatabaseSchema, error) {
	var dbSchema ovsdb.DatabaseSchema
	err := json.Unmarshal([]byte(schema), &dbSchema)
	return dbSchema, err
}
