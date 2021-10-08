package model

import (
	"sync"

	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// A DatabaseModel represents libovsdb's metadata about the database.
// It's the result of combining the client's ClientDBModel and the server's Schema
type DatabaseModel struct {
	client *ClientDBModel
	schema *ovsdb.DatabaseSchema
	mapper *mapper.Mapper
	mutex  sync.RWMutex
}

// NewDatabaseModel returns a new DatabaseModel
func NewDatabaseModel(schema *ovsdb.DatabaseSchema, request *ClientDBModel) *DatabaseModel {
	return &DatabaseModel{
		client: request,
		schema: schema,
		mapper: mapper.NewMapper(schema),
	}
}

// NewPartialDatabaseModel returns a DatabaseModel what does not have a schema yet
func NewPartialDatabaseModel(client *ClientDBModel) *DatabaseModel {
	return &DatabaseModel{
		client: client,
	}
}

// Valid returns whether the DatabaseModel is fully functional
func (db *DatabaseModel) Valid() bool {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.schema != nil
}

// SetSchema adds the Schema to the DatabaseModel making it valid if it was not before
func (db *DatabaseModel) SetSchema(schema *ovsdb.DatabaseSchema) []error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	errors := db.client.Validate(schema)
	if len(errors) > 0 {
		return errors
	}
	db.schema = schema
	db.mapper = mapper.NewMapper(schema)
	return errors
}

// ClearSchema removes the Schema from the DatabaseModel making it not valid
func (db *DatabaseModel) ClearSchema() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.schema = nil
	db.mapper = nil
}

// Client returns the DatabaseModel's client dbModel
func (db *DatabaseModel) Client() *ClientDBModel {
	return db.client
}

// Schema returns the DatabaseModel's schema
func (db *DatabaseModel) Schema() *ovsdb.DatabaseSchema {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.schema
}

// Mapper returns the DatabaseModel's mapper
func (db *DatabaseModel) Mapper() *mapper.Mapper {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.mapper
}
