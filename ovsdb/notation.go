package ovsdb

import (
	"encoding/json"
	"fmt"
)

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []interface{}            `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []Condition              `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid-name,omitempty"`
}

// MarshalJSON marshalls 'Operation' to a byte array
// For 'select' operations, we dont omit the 'Where' field
// to allow selecting all rows of a table
func (o Operation) MarshalJSON() ([]byte, error) {
	type OpAlias Operation
	switch o.Op {
	case "select":
		where := o.Where
		if where == nil {
			where = make([]Condition, 0)
		}
		return json.Marshal(&struct {
			Where []Condition `json:"where"`
			OpAlias
		}{
			Where:   where,
			OpAlias: (OpAlias)(o),
		})
	default:
		return json.Marshal(&struct {
			OpAlias
		}{
			OpAlias: (OpAlias)(o),
		})
	}
}

// MonitorRequests represents a group of monitor requests according to RFC7047
// We cannot use MonitorRequests by inlining the MonitorRequest Map structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type MonitorRequests struct {
	Requests map[string]MonitorRequest `json:"requests"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string       `json:"columns,omitempty"`
	Select  *MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
// We use pointers in order to separate cases, when filed is not set or set to false.
type MonitorSelect struct {
	Initial MonitorSelectValue `json:"initial,omitempty"`
	Insert  MonitorSelectValue `json:"insert,omitempty"`
	Delete  MonitorSelectValue `json:"delete,omitempty"`
	Modify  MonitorSelectValue `json:"modify,omitempty"`
}

func (m MonitorSelect) MarshalJSON() ([]byte, error) {
	val := map[string]interface{}{}
	if m.Initial.IsSet() {
		val["initial"] = m.Initial
	}
	if m.Insert.IsSet() {
		val["insert"] = m.Insert
	}
	if m.Delete.IsSet() {
		val["delete"] = m.Delete
	}
	if m.Modify.IsSet() {
		val["modify"] = m.Modify
	}
	return json.Marshal(val)
}

type MonitorSelectValue struct {
	value *bool `json:"-"`
}

// Helper function to check that a MonitorSelectValue is not set or it is true
// According to RFC7047, if field is unset, it equals to true.
func (m *MonitorSelectValue) IsTrue() bool {
	return m.value == nil || *m.value
}

func (m *MonitorSelectValue) IsSet() bool {
	return m.value != nil
}

func (m MonitorSelectValue) String() string {
	if m.value == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *m.value)
}

func (m MonitorSelectValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.value)
}

func (m *MonitorSelectValue) UnmarshalJSON(data []byte) error {
	var v bool

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	m.value = &v
	return nil
}

func NewMonitorSelectValue(value bool) MonitorSelectValue {
	return MonitorSelectValue{
		value: &value,
	}
}

// TableUpdates is a collection of TableUpdate entries
// We cannot use TableUpdates directly by json encoding by inlining the TableUpdate Map
// structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type TableUpdates struct {
	Updates map[string]TableUpdate `json:"updates"`
}

// TableUpdate represents a table update according to RFC7047
type TableUpdate struct {
	Rows map[string]RowUpdate `json:"rows"`
}

// RowUpdate represents a row update according to RFC7047
type RowUpdate struct {
	New Row `json:"new,omitempty"`
	Old Row `json:"old,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

// TransactResponse represents the response to a Transact Operation
type TransactResponse struct {
	Result []OperationResult `json:"result"`
	Error  string            `json:"error"`
}

// OperationResult is the result of an Operation
type OperationResult struct {
	Count   int         `json:"count,omitempty"`
	Error   string      `json:"error,omitempty"`
	Details string      `json:"details,omitempty"`
	UUID    UUID        `json:"uuid,omitempty"`
	Rows    []ResultRow `json:"rows,omitempty"`
}

func ovsSliceToGoNotation(val interface{}) (interface{}, error) {
	switch sl := val.(type) {
	case []interface{}:
		bsliced, err := json.Marshal(sl)
		if err != nil {
			return nil, err
		}

		switch sl[0] {
		case "uuid", "named-uuid":
			var uuid UUID
			err = json.Unmarshal(bsliced, &uuid)
			return uuid, err
		case "set":
			var oSet OvsSet
			err = json.Unmarshal(bsliced, &oSet)
			return oSet, err
		case "map":
			var oMap OvsMap
			err = json.Unmarshal(bsliced, &oMap)
			return oMap, err
		}
		return val, nil
	}
	return val, nil
}

type Mutator string

const (
	MutateOperationDelete    Mutator = "delete"
	MutateOperationInsert    Mutator = "insert"
	MutateOperationAdd       Mutator = "+="
	MutateOperationSubstract Mutator = "-="
	MutateOperationMultiply  Mutator = "*="
	MutateOperationDivide    Mutator = "/="
	MutateOperationModulo    Mutator = "%="
)
