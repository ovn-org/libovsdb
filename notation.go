package libovsdb

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []interface{}            `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []interface{}            `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid-name,omitempty"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string      `json:"columns,omitempty"`
	Select  MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
type MonitorSelect struct {
	Initial bool `json:"initial,omitempty"`
	Insert  bool `json:"insert,omitempty"`
	Delete  bool `json:"delete,omitempty"`
	Modify  bool `json:"modify,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// NewCondition creates a new condition as specified in RFC7047
func NewCondition(column string, function string, value interface{}) []interface{} {
	return []interface{}{column, function, value}
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

type TransactResponse struct {
	Result []OperationResult `json:"result"`
	Error  string            `json:"error"`
}

type OperationResult struct {
	Count   int                      `json:"count,omitempty"`
	Error   string                   `json:"error,omitempty"`
	Details string                   `json:"details,omitempty"`
	UUID    UUID                     `json:"uuid,omitempty"`
	Rows    []map[string]interface{} `json:"rows,omitempty"`
}

// TODO : add Condition, Function, Mutation and Mutator notations
