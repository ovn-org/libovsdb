package test

import (
	"encoding/json"

	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// FullDatabaseModel returns the DatabaseModel object to be used in libovsdb
func FullDatabaseModel() (model.ClientDBModel, error) {
	return model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
		"IPFIX":        &IPFIX{},
		"Queue":        &Queue{},
	})
}

var schema = `{
	"name": "Open_vSwitch",
	"version": "8.3.0",
	"tables": {
	  "Bridge": {
		"columns": {
		  "auto_attach": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "AutoAttach"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "controller": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "Controller"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "datapath_id": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			},
			"ephemeral": true
		  },
		  "datapath_type": {
			"type": "string"
		  },
		  "datapath_version": {
			"type": "string"
		  },
		  "external_ids": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "fail_mode": {
			"type": {
			  "key": {
				"type": "string",
				"enum": [
				  "set",
				  [
					"standalone",
					"secure"
				  ]
				]
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "flow_tables": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 254
			  },
			  "value": {
				"type": "uuid",
				"refTable": "Flow_Table"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "ipfix": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "IPFIX"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "mirrors": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "Mirror"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "name": {
			"type": "string",
			"mutable": false
		  },
		  "netflow": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "NetFlow"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "other_config": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
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
		  "protocols": {
			"type": {
			  "key": {
				"type": "string",
				"enum": [
				  "set",
				  [
					"OpenFlow10",
					"OpenFlow11",
					"OpenFlow12",
					"OpenFlow13",
					"OpenFlow14",
					"OpenFlow15"
				  ]
				]
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "rstp_status": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			},
			"ephemeral": true
		  },
		  "sflow": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "sFlow"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "status": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			},
			"ephemeral": true
		  }
		},
		"indexes": [
		  [
			"name"
		  ]
		]
	  },
	  "IPFIX": {
		"columns": {
		  "cache_active_timeout": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 4200
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "cache_max_flows": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 4294967295
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "external_ids": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "obs_domain_id": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 4294967295
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "obs_point_id": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 4294967295
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "other_config": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "sampling": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 1,
				"maxInteger": 4294967295
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "targets": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  }
		}
	  },
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
		  },
		  "cur_cfg": {
			"type": "integer"
		  },
		  "datapath_types": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "datapaths": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "uuid",
				"refTable": "Datapath"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "db_version": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "dpdk_version": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "external_ids": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "iface_types": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "manager_options": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "Manager"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "next_cfg": {
			"type": "integer"
		  },
		  "other_config": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "ovs_version": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "ssl": {
			"type": {
			  "key": {
				"type": "uuid",
				"refTable": "SSL"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "statistics": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			},
			"ephemeral": true
		  },
		  "system_type": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "system_version": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "min": 0,
			  "max": 1
			}
		  }
		}
	  },
	  "Queue": {
		"columns": {
		  "dscp": {
			"type": {
			  "key": {
				"type": "integer",
				"minInteger": 0,
				"maxInteger": 63
			  },
			  "min": 0,
			  "max": 1
			}
		  },
		  "external_ids": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  },
		  "other_config": {
			"type": {
			  "key": {
				"type": "string"
			  },
			  "value": {
				"type": "string"
			  },
			  "min": 0,
			  "max": "unlimited"
			}
		  }
		}
	  }
	}
  }`

func Schema() (ovsdb.DatabaseSchema, error) {
	var s ovsdb.DatabaseSchema
	err := json.Unmarshal([]byte(schema), &s)
	return s, err
}

// OpenvSwitch defines an object in Open_vSwitch table
type OpenvSwitch struct {
	UUID            string            `ovsdb:"_uuid"`
	Bridges         []string          `ovsdb:"bridges"`
	CurCfg          int               `ovsdb:"cur_cfg"`
	DatapathTypes   []string          `ovsdb:"datapath_types"`
	Datapaths       map[string]string `ovsdb:"datapaths"`
	DbVersion       *string           `ovsdb:"db_version"`
	DpdkInitialized bool              //`ovsdb:"dpdk_initialized"`
	DpdkVersion     *string           `ovsdb:"dpdk_version"`
	ExternalIDs     map[string]string `ovsdb:"external_ids"`
	IfaceTypes      []string          `ovsdb:"iface_types"`
	ManagerOptions  []string          `ovsdb:"manager_options"`
	NextCfg         int               `ovsdb:"next_cfg"`
	OtherConfig     map[string]string `ovsdb:"other_config"`
	OVSVersion      *string           `ovsdb:"ovs_version"`
	SSL             *string           `ovsdb:"ssl"`
	Statistics      map[string]string `ovsdb:"statistics"`
	SystemType      *string           `ovsdb:"system_type"`
	SystemVersion   *string           `ovsdb:"system_version"`
}

func copyOpenvSwitchBridges(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalOpenvSwitchBridges(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyOpenvSwitchDatapathTypes(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalOpenvSwitchDatapathTypes(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyOpenvSwitchDatapaths(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalOpenvSwitchDatapaths(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyOpenvSwitchDbVersion(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchDbVersion(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyOpenvSwitchDpdkVersion(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchDpdkVersion(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyOpenvSwitchExternalIDs(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalOpenvSwitchExternalIDs(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyOpenvSwitchIfaceTypes(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalOpenvSwitchIfaceTypes(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyOpenvSwitchManagerOptions(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalOpenvSwitchManagerOptions(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyOpenvSwitchOtherConfig(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalOpenvSwitchOtherConfig(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyOpenvSwitchOVSVersion(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchOVSVersion(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyOpenvSwitchSSL(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchSSL(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyOpenvSwitchStatistics(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalOpenvSwitchStatistics(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyOpenvSwitchSystemType(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchSystemType(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyOpenvSwitchSystemVersion(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalOpenvSwitchSystemVersion(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *OpenvSwitch) DeepCopyInto(b *OpenvSwitch) {
	*b = *a
	b.Bridges = copyOpenvSwitchBridges(a.Bridges)
	b.DatapathTypes = copyOpenvSwitchDatapathTypes(a.DatapathTypes)
	b.Datapaths = copyOpenvSwitchDatapaths(a.Datapaths)
	b.DbVersion = copyOpenvSwitchDbVersion(a.DbVersion)
	b.DpdkVersion = copyOpenvSwitchDpdkVersion(a.DpdkVersion)
	b.ExternalIDs = copyOpenvSwitchExternalIDs(a.ExternalIDs)
	b.IfaceTypes = copyOpenvSwitchIfaceTypes(a.IfaceTypes)
	b.ManagerOptions = copyOpenvSwitchManagerOptions(a.ManagerOptions)
	b.OtherConfig = copyOpenvSwitchOtherConfig(a.OtherConfig)
	b.OVSVersion = copyOpenvSwitchOVSVersion(a.OVSVersion)
	b.SSL = copyOpenvSwitchSSL(a.SSL)
	b.Statistics = copyOpenvSwitchStatistics(a.Statistics)
	b.SystemType = copyOpenvSwitchSystemType(a.SystemType)
	b.SystemVersion = copyOpenvSwitchSystemVersion(a.SystemVersion)
}

func (a *OpenvSwitch) DeepCopy() *OpenvSwitch {
	b := new(OpenvSwitch)
	a.DeepCopyInto(b)
	return b
}

func (a *OpenvSwitch) CloneModelInto(b model.Model) {
	c := b.(*OpenvSwitch)
	a.DeepCopyInto(c)
}

func (a *OpenvSwitch) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *OpenvSwitch) Equals(b *OpenvSwitch) bool {
	return a.UUID == b.UUID &&
		equalOpenvSwitchBridges(a.Bridges, b.Bridges) &&
		a.CurCfg == b.CurCfg &&
		equalOpenvSwitchDatapathTypes(a.DatapathTypes, b.DatapathTypes) &&
		equalOpenvSwitchDatapaths(a.Datapaths, b.Datapaths) &&
		equalOpenvSwitchDbVersion(a.DbVersion, b.DbVersion) &&
		a.DpdkInitialized == b.DpdkInitialized &&
		equalOpenvSwitchDpdkVersion(a.DpdkVersion, b.DpdkVersion) &&
		equalOpenvSwitchExternalIDs(a.ExternalIDs, b.ExternalIDs) &&
		equalOpenvSwitchIfaceTypes(a.IfaceTypes, b.IfaceTypes) &&
		equalOpenvSwitchManagerOptions(a.ManagerOptions, b.ManagerOptions) &&
		a.NextCfg == b.NextCfg &&
		equalOpenvSwitchOtherConfig(a.OtherConfig, b.OtherConfig) &&
		equalOpenvSwitchOVSVersion(a.OVSVersion, b.OVSVersion) &&
		equalOpenvSwitchSSL(a.SSL, b.SSL) &&
		equalOpenvSwitchStatistics(a.Statistics, b.Statistics) &&
		equalOpenvSwitchSystemType(a.SystemType, b.SystemType) &&
		equalOpenvSwitchSystemVersion(a.SystemVersion, b.SystemVersion)
}

func (a *OpenvSwitch) EqualsModel(b model.Model) bool {
	c := b.(*OpenvSwitch)
	return a.Equals(c)
}

var _ model.CloneableModel = &OpenvSwitch{}
var _ model.ComparableModel = &OpenvSwitch{}

type (
	BridgeFailMode  = string
	BridgeProtocols = string
)

var (
	BridgeFailModeStandalone  BridgeFailMode  = "standalone"
	BridgeFailModeSecure      BridgeFailMode  = "secure"
	BridgeProtocolsOpenflow10 BridgeProtocols = "OpenFlow10"
	BridgeProtocolsOpenflow11 BridgeProtocols = "OpenFlow11"
	BridgeProtocolsOpenflow12 BridgeProtocols = "OpenFlow12"
	BridgeProtocolsOpenflow13 BridgeProtocols = "OpenFlow13"
	BridgeProtocolsOpenflow14 BridgeProtocols = "OpenFlow14"
	BridgeProtocolsOpenflow15 BridgeProtocols = "OpenFlow15"
)

// Bridge defines an object in Bridge table
type Bridge struct {
	UUID                string            `ovsdb:"_uuid"`
	AutoAttach          *string           `ovsdb:"auto_attach"`
	Controller          []string          `ovsdb:"controller"`
	DatapathID          *string           `ovsdb:"datapath_id"`
	DatapathType        string            `ovsdb:"datapath_type"`
	DatapathVersion     string            `ovsdb:"datapath_version"`
	ExternalIDs         map[string]string `ovsdb:"external_ids"`
	FailMode            *BridgeFailMode   `ovsdb:"fail_mode"`
	FloodVLANs          [4096]int         //`ovsdb:"flood_vlans"`
	FlowTables          map[int]string    `ovsdb:"flow_tables"`
	IPFIX               *string           `ovsdb:"ipfix"`
	McastSnoopingEnable bool              //`ovsdb:"mcast_snooping_enable"`
	Mirrors             []string          `ovsdb:"mirrors"`
	Name                string            `ovsdb:"name"`
	Netflow             *string           `ovsdb:"netflow"`
	OtherConfig         map[string]string `ovsdb:"other_config"`
	Ports               []string          `ovsdb:"ports"`
	Protocols           []BridgeProtocols `ovsdb:"protocols"`
	RSTPEnable          bool              //`ovsdb:"rstp_enable"`
	RSTPStatus          map[string]string `ovsdb:"rstp_status"`
	Sflow               *string           `ovsdb:"sflow"`
	Status              map[string]string `ovsdb:"status"`
	STPEnable           bool              //`ovsdb:"stp_enable"`
}

func copyBridgeAutoAttach(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeAutoAttach(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeController(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalBridgeController(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyBridgeDatapathID(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeDatapathID(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeExternalIDs(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalBridgeExternalIDs(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyBridgeFailMode(a *BridgeFailMode) *BridgeFailMode {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeFailMode(a, b *BridgeFailMode) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeFlowTables(a map[int]string) map[int]string {
	if a == nil {
		return nil
	}
	b := make(map[int]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalBridgeFlowTables(a, b map[int]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyBridgeIPFIX(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeIPFIX(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeMirrors(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalBridgeMirrors(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyBridgeNetflow(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeNetflow(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeOtherConfig(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalBridgeOtherConfig(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyBridgePorts(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalBridgePorts(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyBridgeProtocols(a []BridgeProtocols) []BridgeProtocols {
	if a == nil {
		return nil
	}
	b := make([]BridgeProtocols, len(a))
	copy(b, a)
	return b
}

func equalBridgeProtocols(a, b []BridgeProtocols) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func copyBridgeRSTPStatus(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalBridgeRSTPStatus(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyBridgeSflow(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalBridgeSflow(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyBridgeStatus(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalBridgeStatus(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func (a *Bridge) DeepCopyInto(b *Bridge) {
	*b = *a
	b.AutoAttach = copyBridgeAutoAttach(a.AutoAttach)
	b.Controller = copyBridgeController(a.Controller)
	b.DatapathID = copyBridgeDatapathID(a.DatapathID)
	b.ExternalIDs = copyBridgeExternalIDs(a.ExternalIDs)
	b.FailMode = copyBridgeFailMode(a.FailMode)
	b.FlowTables = copyBridgeFlowTables(a.FlowTables)
	b.IPFIX = copyBridgeIPFIX(a.IPFIX)
	b.Mirrors = copyBridgeMirrors(a.Mirrors)
	b.Netflow = copyBridgeNetflow(a.Netflow)
	b.OtherConfig = copyBridgeOtherConfig(a.OtherConfig)
	b.Ports = copyBridgePorts(a.Ports)
	b.Protocols = copyBridgeProtocols(a.Protocols)
	b.RSTPStatus = copyBridgeRSTPStatus(a.RSTPStatus)
	b.Sflow = copyBridgeSflow(a.Sflow)
	b.Status = copyBridgeStatus(a.Status)
}

func (a *Bridge) DeepCopy() *Bridge {
	b := new(Bridge)
	a.DeepCopyInto(b)
	return b
}

func (a *Bridge) CloneModelInto(b model.Model) {
	c := b.(*Bridge)
	a.DeepCopyInto(c)
}

func (a *Bridge) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *Bridge) Equals(b *Bridge) bool {
	return a.UUID == b.UUID &&
		equalBridgeAutoAttach(a.AutoAttach, b.AutoAttach) &&
		equalBridgeController(a.Controller, b.Controller) &&
		equalBridgeDatapathID(a.DatapathID, b.DatapathID) &&
		a.DatapathType == b.DatapathType &&
		a.DatapathVersion == b.DatapathVersion &&
		equalBridgeExternalIDs(a.ExternalIDs, b.ExternalIDs) &&
		equalBridgeFailMode(a.FailMode, b.FailMode) &&
		a.FloodVLANs == b.FloodVLANs &&
		equalBridgeFlowTables(a.FlowTables, b.FlowTables) &&
		equalBridgeIPFIX(a.IPFIX, b.IPFIX) &&
		a.McastSnoopingEnable == b.McastSnoopingEnable &&
		equalBridgeMirrors(a.Mirrors, b.Mirrors) &&
		a.Name == b.Name &&
		equalBridgeNetflow(a.Netflow, b.Netflow) &&
		equalBridgeOtherConfig(a.OtherConfig, b.OtherConfig) &&
		equalBridgePorts(a.Ports, b.Ports) &&
		equalBridgeProtocols(a.Protocols, b.Protocols) &&
		a.RSTPEnable == b.RSTPEnable &&
		equalBridgeRSTPStatus(a.RSTPStatus, b.RSTPStatus) &&
		equalBridgeSflow(a.Sflow, b.Sflow) &&
		equalBridgeStatus(a.Status, b.Status) &&
		a.STPEnable == b.STPEnable
}

func (a *Bridge) EqualsModel(b model.Model) bool {
	c := b.(*Bridge)
	return a.Equals(c)
}

var _ model.CloneableModel = &Bridge{}
var _ model.ComparableModel = &Bridge{}

// IPFIX defines an object in IPFIX table
type IPFIX struct {
	UUID               string            `ovsdb:"_uuid"`
	CacheActiveTimeout *int              `ovsdb:"cache_active_timeout"`
	CacheMaxFlows      *int              `ovsdb:"cache_max_flows"`
	ExternalIDs        map[string]string `ovsdb:"external_ids"`
	ObsDomainID        *int              `ovsdb:"obs_domain_id"`
	ObsPointID         *int              `ovsdb:"obs_point_id"`
	OtherConfig        map[string]string `ovsdb:"other_config"`
	Sampling           *int              `ovsdb:"sampling"`
	Targets            []string          `ovsdb:"targets"`
}

func copyIPFIXCacheActiveTimeout(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalIPFIXCacheActiveTimeout(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyIPFIXCacheMaxFlows(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalIPFIXCacheMaxFlows(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyIPFIXExternalIDs(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalIPFIXExternalIDs(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyIPFIXObsDomainID(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalIPFIXObsDomainID(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyIPFIXObsPointID(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalIPFIXObsPointID(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyIPFIXOtherConfig(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalIPFIXOtherConfig(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyIPFIXSampling(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalIPFIXSampling(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyIPFIXTargets(a []string) []string {
	if a == nil {
		return nil
	}
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func equalIPFIXTargets(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func (a *IPFIX) DeepCopyInto(b *IPFIX) {
	*b = *a
	b.CacheActiveTimeout = copyIPFIXCacheActiveTimeout(a.CacheActiveTimeout)
	b.CacheMaxFlows = copyIPFIXCacheMaxFlows(a.CacheMaxFlows)
	b.ExternalIDs = copyIPFIXExternalIDs(a.ExternalIDs)
	b.ObsDomainID = copyIPFIXObsDomainID(a.ObsDomainID)
	b.ObsPointID = copyIPFIXObsPointID(a.ObsPointID)
	b.OtherConfig = copyIPFIXOtherConfig(a.OtherConfig)
	b.Sampling = copyIPFIXSampling(a.Sampling)
	b.Targets = copyIPFIXTargets(a.Targets)
}

func (a *IPFIX) DeepCopy() *IPFIX {
	b := new(IPFIX)
	a.DeepCopyInto(b)
	return b
}

func (a *IPFIX) CloneModelInto(b model.Model) {
	c := b.(*IPFIX)
	a.DeepCopyInto(c)
}

func (a *IPFIX) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *IPFIX) Equals(b *IPFIX) bool {
	return a.UUID == b.UUID &&
		equalIPFIXCacheActiveTimeout(a.CacheActiveTimeout, b.CacheActiveTimeout) &&
		equalIPFIXCacheMaxFlows(a.CacheMaxFlows, b.CacheMaxFlows) &&
		equalIPFIXExternalIDs(a.ExternalIDs, b.ExternalIDs) &&
		equalIPFIXObsDomainID(a.ObsDomainID, b.ObsDomainID) &&
		equalIPFIXObsPointID(a.ObsPointID, b.ObsPointID) &&
		equalIPFIXOtherConfig(a.OtherConfig, b.OtherConfig) &&
		equalIPFIXSampling(a.Sampling, b.Sampling) &&
		equalIPFIXTargets(a.Targets, b.Targets)
}

func (a *IPFIX) EqualsModel(b model.Model) bool {
	c := b.(*IPFIX)
	return a.Equals(c)
}

var _ model.CloneableModel = &IPFIX{}
var _ model.ComparableModel = &IPFIX{}

// Queue defines an object in Queue table
type Queue struct {
	UUID        string            `ovsdb:"_uuid"`
	DSCP        *int              `ovsdb:"dscp"`
	ExternalIDs map[string]string `ovsdb:"external_ids"`
	OtherConfig map[string]string `ovsdb:"other_config"`
}

func copyQueueDSCP(a *int) *int {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalQueueDSCP(a, b *int) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyQueueExternalIDs(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalQueueExternalIDs(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func copyQueueOtherConfig(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	b := make(map[string]string, len(a))
	for k, v := range a {
		b[k] = v
	}
	return b
}

func equalQueueOtherConfig(a, b map[string]string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}

func (a *Queue) DeepCopyInto(b *Queue) {
	*b = *a
	b.DSCP = copyQueueDSCP(a.DSCP)
	b.ExternalIDs = copyQueueExternalIDs(a.ExternalIDs)
	b.OtherConfig = copyQueueOtherConfig(a.OtherConfig)
}

func (a *Queue) DeepCopy() *Queue {
	b := new(Queue)
	a.DeepCopyInto(b)
	return b
}

func (a *Queue) CloneModelInto(b model.Model) {
	c := b.(*Queue)
	a.DeepCopyInto(c)
}

func (a *Queue) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *Queue) Equals(b *Queue) bool {
	return a.UUID == b.UUID &&
		equalQueueDSCP(a.DSCP, b.DSCP) &&
		equalQueueExternalIDs(a.ExternalIDs, b.ExternalIDs) &&
		equalQueueOtherConfig(a.OtherConfig, b.OtherConfig)
}

func (a *Queue) EqualsModel(b model.Model) bool {
	c := b.(*Queue)
	return a.Equals(c)
}

var _ model.CloneableModel = &Queue{}
var _ model.ComparableModel = &Queue{}
