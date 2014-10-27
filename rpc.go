package libovsdb

import ()

// NewGetSchemaArgs creates a new set of arguments for a get_schemas RPC
func NewGetSchemaArgs(schema string) []interface{} {
	return []interface{}{schema}
}

// NewTransactArgs creates a new set of arguments for a transact RPC
func NewTransactArgs(database string, operation Operation) []interface{} {
	return []interface{}{database, operation}
}

// NewCancelArgs creates a new set of arguments for a cancel RPC
func NewCancelArgs(id interface{}) []interface{} {
	return []interface{}{id}
}

// NewMonitorArgs creates a new set of arguments for a monitor RPC
func NewMonitorArgs(database string, value interface{}, requests []MonitorRequest) []interface{} {
	return []interface{}{database, value, requests}
}

// NewMonitorCancelArgs creates a new set of arguments for a monitor_cancel RPC
func NewMonitorCancelArgs(value interface{}) []interface{} {
	return []interface{}{value}
}

// NewLockArgs creates a new set of arguments for a lock, steal or unlock RPC
func NewLockArgs(id interface{}) []interface{} {
	return []interface{}{id}
}
