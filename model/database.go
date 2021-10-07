package model

import (
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// A DatabaseModel represents libovsdb's metadata about the database.
// It's the result of combining the client's ClientDBModel and the server's Schema
type DatabaseModel struct {
	client *ClientDBModel
	schema *ovsdb.DatabaseSchema
	mapper *mapper.Mapper
}

// NewDatabaseModel returns a new DatabaseModel
func NewDatabaseModel(schema *ovsdb.DatabaseSchema, request *ClientDBModel) *DatabaseModel {
	return &DatabaseModel{
		client: request,
		schema: schema,
		mapper: mapper.NewMapper(schema),
	}
}

// Client returns the DatabaseModel's client dbModel
func (db *DatabaseModel) Client() *ClientDBModel {
	return db.client
}

// Schema returns the DatabaseModel's schema
func (db *DatabaseModel) Schema() *ovsdb.DatabaseSchema {
	return db.schema
}

// Mapper returns the DatabaseModel's mapper
func (db *DatabaseModel) Mapper() *mapper.Mapper {
	return db.mapper
}
