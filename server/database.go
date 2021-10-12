package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// Database abstracts database operations from ovsdb
type Database interface {
	CreateDatabase(ctx context.Context, database string, model *ovsdb.DatabaseSchema) error
	Exists(ctx context.Context, database string) bool
	Commit(ctx context.Context, database string, id uuid.UUID, updates ovsdb.TableUpdates2) error
	CheckIndexes(ctx context.Context, database string, table string, m model.Model) error
	List(ctx context.Context, database, table string, conditions ...ovsdb.Condition) ([]model.Model, error)
	Get(ctx context.Context, database, table string, uuid string) (model.Model, error)
}

type inMemoryDatabase struct {
	databases map[string]*cache.TableCache
	models    map[string]*model.DBModel
	mutex     sync.RWMutex
}

func NewInMemoryDatabase(models map[string]*model.DBModel) Database {
	return &inMemoryDatabase{
		databases: make(map[string]*cache.TableCache),
		models:    models,
		mutex:     sync.RWMutex{},
	}
}

func (db *inMemoryDatabase) CreateDatabase(ctx context.Context, name string, schema *ovsdb.DatabaseSchema) error {
	_, span := tracer.Start(ctx, "CreateDatabase")
	defer span.End()
	db.mutex.Lock()
	defer db.mutex.Unlock()
	var mo *model.DBModel
	var ok bool
	if mo, ok = db.models[schema.Name]; !ok {
		return fmt.Errorf("no db model provided for schema with name %s", name)
	}
	database, err := cache.NewTableCache(schema, mo, nil)
	if err != nil {
		return nil
	}
	db.databases[name] = database
	return nil
}

func (db *inMemoryDatabase) Exists(ctx context.Context, name string) bool {
	_, span := tracer.Start(ctx, "Exists")
	defer span.End()
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	_, ok := db.databases[name]
	return ok
}

func (db *inMemoryDatabase) Commit(ctx context.Context, database string, id uuid.UUID, updates ovsdb.TableUpdates2) error {
	ctx, span := tracer.Start(ctx, "Commit")
	defer span.End()
	if !db.Exists(ctx, database) {
		return fmt.Errorf("db does not exist")
	}
	db.mutex.RLock()
	targetDb := db.databases[database]
	db.mutex.RLock()
	targetDb.Populate2(ctx, updates)
	return nil
}

func (db *inMemoryDatabase) CheckIndexes(ctx context.Context, database string, table string, m model.Model) error {
	_, span := tracer.Start(ctx, "CheckIndexes")
	defer span.End()
	if !db.Exists(ctx, database) {
		return nil
	}
	db.mutex.RLock()
	targetDb := db.databases[database]
	db.mutex.RLock()
	targetTable := targetDb.Table(table)
	return targetTable.IndexExists(m)
}

func (db *inMemoryDatabase) List(ctx context.Context, database, table string, conditions ...ovsdb.Condition) ([]model.Model, error) {
	_, span := tracer.Start(ctx, "List")
	defer span.End()
	if !db.Exists(ctx, database) {
		return nil, fmt.Errorf("db does not exist")
	}
	db.mutex.RLock()
	targetDb := db.databases[database]
	db.mutex.RLock()

	targetTable := targetDb.Table(table)
	if targetTable == nil {
		return nil, fmt.Errorf("table does not exist")
	}

	return targetTable.RowsByCondition(conditions)
}

func (db *inMemoryDatabase) Get(ctx context.Context, database, table string, uuid string) (model.Model, error) {
	_, span := tracer.Start(ctx, "Get")
	defer span.End()
	if !db.Exists(ctx, database) {
		return nil, fmt.Errorf("db does not exist")
	}
	db.mutex.RLock()
	targetDb := db.databases[database]
	db.mutex.RLock()

	targetTable := targetDb.Table(table)
	if targetTable == nil {
		return nil, fmt.Errorf("table does not exist")
	}
	return targetTable.Row(uuid), nil
}
