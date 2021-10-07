package model

import (
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// A DatabaseModel represents libovsdb's metadata about the database.
// It's the result of combining the client's DatabaseModelRequest and the server's Schema
type DatabaseModel struct {
	request *DatabaseModelRequest
	schema  *ovsdb.DatabaseSchema
	mapper  *mapper.Mapper
}

// NewDatabaseModel returns a new DatabaseModel
func NewDatabaseModel(schema *ovsdb.DatabaseSchema, request *DatabaseModelRequest) *DatabaseModel {
	return &DatabaseModel{
		request: request,
		schema:  schema,
		mapper:  mapper.NewMapper(schema),
	}
}

// Request returns the DatabaseModel's request
func (db *DatabaseModel) Request() *DatabaseModelRequest {
	return db.request
}

// Schema returns the DatabaseModel's schema
func (db *DatabaseModel) Schema() *ovsdb.DatabaseSchema {
	return db.schema
}

// Mapper returns the DatabaseModel's mapper
func (db *DatabaseModel) Mapper() *mapper.Mapper {
	return db.mapper
}
