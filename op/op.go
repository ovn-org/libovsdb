package op

type Operation struct {
	Op        string        `json:"op"`
	Table     string        `json:"table"`
	Row       interface{}   `json:"row,omitempty"`
	Rows      []interface{} `json:"rows,omitempty"`
	Columns   []string      `json:"columns,omitempty"`
	Mutations []string      `json:"mutations,omitempty"`
	Timeout   int           `json:"timeout,omitempty"`
	Where     []string      `json:"where,omitempty"`
	Until     string        `json:"until,omitempty"`
	UUIDName  string        `json:"uuid_name,omitempty""`
}
