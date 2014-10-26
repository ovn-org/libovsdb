package libovsdb

import (
	"errors"
	"regexp"
)

// Operation represents an operation according to RFC7047
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []string                 `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []string                 `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid_name,omitempty"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string `json:"columns,omitempty"`
	Select MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
type MonitorSelect struct {
	Initial bool `json:"initial,omitempty"`
	Insert bool `json:"insert,omitempty"`
	Delete bool `json:"delete,omitempty"`
	Modify bool `json:"modify,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
    Error string `json:"error"`
    Details string `json:"details,omitempty"`
}


// NewUUID creates a new uuid as specified in RFC7047
func NewUUID(uuid string) ([]string, error) {
	err := validateUUID(uuid)
	if err != nil {
		return nil, err
	}
	return []string{"uuid", uuid}, nil
}

// NewNamedUUID creates a new named-uuid as specified in RFC7047
func NewNamedUUID(uuid string) []string {

	return []string{"named-uuid", uuid}
}

// NewCondition creates a new condition as specified in RFC7047
func NewCondition(column string, function string, value interface{}) []interface{} {
	return []interface{}{column, function, value}
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

func validateUUID(uuid string) error {

	if len(uuid) != 36 {
		return errors.New("uuid exceeds 36 characters")
	}

	var validUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	if !validUUID.MatchString(uuid) {
		return errors.New("uuid does not match regexp")
	}

	return nil
}
