package test

import (
	"encoding/json"
	"fmt"

	"github.com/ovn-org/libovsdb/model"
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
        },
        "Flow_Sample_Collector_Set": {
            "columns": {
                "id": {
                    "type": {
                        "key": {
                            "type": "integer",
                            "minInteger": 0,
                            "maxInteger": 4294967295
                        },
                        "min": 1,
                        "max": 1
                    }
                },
                "bridge": {
                    "type": {
                        "key": {
                            "type": "uuid",
                            "refTable": "Bridge"
                        },
                        "min": 1,
                        "max": 1
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
            "isRoot": true,
            "indexes": [
                [
                    "id",
                    "bridge"
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

func (b *BridgeType) GetTableName() string {
	return "Bridge"
}

// OvsType is the simplified ORM model of the Bridge table
type OvsType struct {
	UUID    string   `ovsdb:"_uuid"`
	Bridges []string `ovsdb:"bridges"`
}

func (o *OvsType) GetTableName() string {
	return "Open_vSwitch"
}

type FlowSampleCollectorSetType struct {
	UUID        string            `ovsdb:"_uuid"`
	Bridge      string            `ovsdb:"bridge"`
	ExternalIDs map[string]string `ovsdb:"external_ids"`
	ID          int               `ovsdb:"id"`
	IPFIX       *string           // `ovsdb:"ipfix"`
}

func (f *FlowSampleCollectorSetType) GetTableName() string {
	return "FlowSampleCollectorSet"
}

func GetModel() (model.DatabaseModel, error) {
	client, err := model.NewClientDBModel(
		"Open_vSwitch",
		map[string]model.Model{
			"Open_vSwitch":              &OvsType{},
			"Bridge":                    &BridgeType{},
			"Flow_Sample_Collector_Set": &FlowSampleCollectorSetType{},
		},
	)
	if err != nil {
		return model.DatabaseModel{}, err
	}
	schema, err := GetSchema()
	if err != nil {
		return model.DatabaseModel{}, err
	}
	dbModel, errs := model.NewDatabaseModel(schema, client)
	if len(errs) > 0 {
		return model.DatabaseModel{}, fmt.Errorf("errors build model: %v", errs)
	}
	return dbModel, nil
}

func GetSchema() (ovsdb.DatabaseSchema, error) {
	var dbSchema ovsdb.DatabaseSchema
	err := json.Unmarshal([]byte(schema), &dbSchema)
	return dbSchema, err
}
