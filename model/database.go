package model

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// A DatabaseModel represents libovsdb's metadata about the database.
// It's the result of combining the client's DatabaseModelRequest and the server's Schema
type DatabaseModel struct {
	valid   bool
	request *DatabaseModelRequest
	schema  *ovsdb.DatabaseSchema
	mapper  *mapper.Mapper
}

// NewDatabaseModel returns a new DatabaseModel
func NewDatabaseModel(schema *ovsdb.DatabaseSchema, request *DatabaseModelRequest) *DatabaseModel {
	return &DatabaseModel{
		valid:   true,
		request: request,
		schema:  schema,
		mapper:  mapper.NewMapper(schema),
	}
}

// NewPartialDatabaseModel returns a DatabaseModel what does not have a schema yet
func NewPartialDatabaseModel(request *DatabaseModelRequest) *DatabaseModel {
	return &DatabaseModel{
		valid:   false,
		request: request,
	}
}

// Valid returns whether the DatabaseModel is fully functional
func (db *DatabaseModel) Valid() bool {
	return db.valid
}

// SetSchema adds the Schema to the DatabaseModel making it valid if it was not before
func (db *DatabaseModel) SetSchema(schema *ovsdb.DatabaseSchema) []error {
	errors := db.request.validate(schema)
	if len(errors) > 0 {
		return errors
	}
	db.schema = schema
	db.mapper = mapper.NewMapper(schema)
	db.valid = true
	return errors
}

// ClearSchema removes the Schema from the DatabaseModel making it not valid
func (db *DatabaseModel) ClearSchema() {
	db.schema = nil
	db.mapper = nil
	db.valid = false
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

// NewModel returns a new instance of a model from a specific string
func (db DatabaseModel) NewModel(table string) (Model, error) {
	mtype, ok := db.request.types[table]
	if !ok {
		return nil, fmt.Errorf("table %s not found in database model", string(table))
	}
	model := reflect.New(mtype.Elem())
	return model.Interface().(Model), nil
}

// Types returns the DatabaseModel Types
// the DatabaseModel types is a map of reflect.Types indexed by string
// The reflect.Type is a pointer to a struct that contains 'ovs' tags
// as described above. Such pointer to struct also implements the Model interface
func (db DatabaseModel) Types() map[string]reflect.Type {
	return db.request.types
}

// FindTable returns the string associated with a reflect.Type or ""
func (db DatabaseModel) FindTable(mType reflect.Type) string {
	for table, tType := range db.request.types {
		if tType == mType {
			return table
		}
	}
	return ""
}
