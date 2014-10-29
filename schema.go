package libovsdb

import "fmt"

type DatabaseSchema struct {
	Name    string                 `json:"name"`
	Version string                 `json:"version"`
	Tables  map[string]TableSchema `json:"tables"`
}

type TableSchema struct {
	Columns map[string]ColumnSchema `json:"columns"`
	Indexes [][]string              `json:"indexes,omitempty"`
}

type ColumnSchema struct {
	Name      string      `json:"name"`
	Type      interface{} `json:"type"`
	Ephemeral bool        `json:"ephemeral,omitempty"`
	Mutable   bool        `json:"mutable,omitempty"`
}

func (schema DatabaseSchema) Print() {
	fmt.Printf("%s, (%s)\n", schema.Name, schema.Version)
	for table, tableSchema := range schema.Tables {
		fmt.Printf("\t %s\n", table)
		for column, columnSchema := range tableSchema.Columns {
			fmt.Printf("\t\t %s => %v\n", column, columnSchema)
		}
	}
}
