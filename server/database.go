package server

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

var (
	ErrNotImplemented = errors.New("not implemented")
)

// Database abstracts database operations from ovsdb
type Database interface {
	CreateDatabase(name string, model *ovsdb.DatabaseSchema) error
	Exists(name string) bool
	Transact(database string, operations []ovsdb.Operation) ([]ovsdb.OperationResult, ovsdb.TableUpdates)
	Select(database string, table string, where []ovsdb.Condition, columns []string) ovsdb.OperationResult
	Insert(database string, table string, uuidName string, row ovsdb.Row) (ovsdb.OperationResult, ovsdb.TableUpdates)
	Update(database, table string, where []ovsdb.Condition, row ovsdb.Row) (ovsdb.OperationResult, ovsdb.TableUpdates)
	Mutate(database, table string, where []ovsdb.Condition, mutations []ovsdb.Mutation) (ovsdb.OperationResult, ovsdb.TableUpdates)
	Delete(database, table string, where []ovsdb.Condition) (ovsdb.OperationResult, ovsdb.TableUpdates)
	Wait(database, table string, timeout int, conditions []ovsdb.Condition, columns []string, until string, rows []ovsdb.Row) ovsdb.OperationResult
	Commit(database, table string, durable bool) ovsdb.OperationResult
	Abort(database, table string) ovsdb.OperationResult
	Comment(database, table string, comment string) ovsdb.OperationResult
	Assert(database, table, lock string) ovsdb.OperationResult
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

func (db *inMemoryDatabase) CreateDatabase(name string, schema *ovsdb.DatabaseSchema) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	var mo *model.DBModel
	var ok bool
	if mo, ok = db.models[schema.Name]; !ok {
		return fmt.Errorf("no db model provided for schema with name %s", name)
	}
	database, err := cache.NewTableCache(schema, mo)
	if err != nil {
		return nil
	}
	db.databases[name] = database
	return nil
}

func (db *inMemoryDatabase) Exists(name string) bool {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	_, ok := db.databases[name]
	return ok
}

func (db *inMemoryDatabase) Transact(name string, operations []ovsdb.Operation) ([]ovsdb.OperationResult, ovsdb.TableUpdates) {
	results := []ovsdb.OperationResult{}
	updates := make(ovsdb.TableUpdates)
	for _, op := range operations {
		switch op.Op {
		case ovsdb.OperationInsert:
			r, tu := db.Insert(name, op.Table, op.UUIDName, op.Row)
			results = append(results, r)
			if tu != nil {
				updates.Merge(tu)
			}
		case ovsdb.OperationSelect:
			r := db.Select(name, op.Table, op.Where, op.Columns)
			results = append(results, r)
		case ovsdb.OperationUpdate:
			r, tu := db.Update(name, op.Table, op.Where, op.Row)
			results = append(results, r)
			if tu != nil {
				updates.Merge(tu)
			}
		case ovsdb.OperationMutate:
			r, tu := db.Mutate(name, op.Table, op.Where, op.Mutations)
			results = append(results, r)
			if tu != nil {
				updates.Merge(tu)
			}
		case ovsdb.OperationDelete:
			r, tu := db.Delete(name, op.Table, op.Where)
			results = append(results, r)
			if tu != nil {
				updates.Merge(tu)
			}
		case ovsdb.OperationWait:
			r := db.Wait(name, op.Table, op.Timeout, op.Where, op.Columns, op.Until, op.Rows)
			results = append(results, r)
		case ovsdb.OperationCommit:
			durable := op.Durable
			r := db.Commit(name, op.Table, *durable)
			results = append(results, r)
		case ovsdb.OperationAbort:
			r := db.Abort(name, op.Table)
			results = append(results, r)
		case ovsdb.OperationComment:
			r := db.Comment(name, op.Table, *op.Comment)
			results = append(results, r)
		case ovsdb.OperationAssert:
			r := db.Assert(name, op.Table, *op.Lock)
			results = append(results, r)
		default:
			return nil, updates
		}
	}
	return results, updates
}

func (db *inMemoryDatabase) Insert(database string, table string, rowUUID string, row ovsdb.Row) (ovsdb.OperationResult, ovsdb.TableUpdates) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var targetDb *cache.TableCache
	var ok bool
	if targetDb, ok = db.databases[database]; !ok {
		return ovsdb.OperationResult{
			Error: "database does not exist",
		}, nil
	}
	if rowUUID == "" {
		rowUUID = uuid.NewString()
	}
	model, err := targetDb.CreateModel(table, &row, rowUUID)
	if err != nil {
		return ovsdb.OperationResult{
			Error: err.Error(),
		}, nil
	}

	if t := targetDb.Table(table); t == nil {
		targetDb.Set(table, cache.NewRowCache(nil))
	}

	// check duplicates
	for _, existingUUID := range targetDb.Table(table).Rows() {
		existingRow := targetDb.Table(table).Row(existingUUID)
		if ok, err := targetDb.Mapper().EqualFields(table, model, existingRow); ok {
			return ovsdb.OperationResult{
				Error: fmt.Sprintf("constraint violation: %s", err),
			}, nil
		}
	}

	// insert in to db
	targetDb.Table(table).Create(rowUUID, model)

	resultRow, err := targetDb.Mapper().NewRow(table, model)
	if err != nil {
		return ovsdb.OperationResult{
			Error: err.Error(),
		}, nil
	}

	result := ovsdb.OperationResult{
		UUID: ovsdb.UUID{GoUUID: rowUUID},
	}
	return result, ovsdb.TableUpdates{
		table: {
			rowUUID: {
				New: &resultRow,
				Old: nil,
			},
		},
	}
}

func (db *inMemoryDatabase) Select(database string, table string, where []ovsdb.Condition, columns []string) ovsdb.OperationResult {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	var targetDb *cache.TableCache
	var ok bool
	if targetDb, ok = db.databases[database]; !ok {
		return ovsdb.OperationResult{
			Error: "database does not exist",
		}
	}

	if t := targetDb.Table(table); t == nil {
		targetDb.Set(table, cache.NewRowCache(nil))
	}

	schema := targetDb.Mapper().Schema.Table(table)
	var results []ovsdb.Row
	count := 0
	for _, uuid := range targetDb.Table(table).Rows() {
		row := targetDb.Table(table).Row(uuid)
		info, _ := mapper.NewMapperInfo(schema, row)
		match := false
		if len(where) == 0 {
			match = true
		} else {
			for _, condition := range where {
				field, _ := info.FieldByColumn(condition.Column)
				column := schema.Column(condition.Column)
				nativeValue, err := ovsdb.OvsToNative(column, condition.Value)
				if err != nil {
					panic(err)
				}
				ok, err := condition.Function.Evaluate(field, nativeValue)
				if err != nil {
					panic(err)
				}
				if ok {
					match = true
					count++
				}
			}
		}
		if match {
			resultRow, err := targetDb.Mapper().NewRow(table, row)
			if err != nil {
				panic(err)
			}
			results = append(results, resultRow)
		}
	}
	return ovsdb.OperationResult{
		Rows: results,
	}
}

func (db *inMemoryDatabase) Update(database, table string, where []ovsdb.Condition, row ovsdb.Row) (ovsdb.OperationResult, ovsdb.TableUpdates) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var targetDb *cache.TableCache
	var ok bool
	if targetDb, ok = db.databases[database]; !ok {
		return ovsdb.OperationResult{
			Error: "database does not exist",
		}, nil
	}

	if t := targetDb.Table(table); t == nil {
		targetDb.Set(table, cache.NewRowCache(nil))
	}

	schema := targetDb.Mapper().Schema.Table(table)
	tableUpdate := make(ovsdb.TableUpdate)
	count := 0
	for _, uuid := range targetDb.Table(table).Rows() {
		oldRow := targetDb.Table(table).Row(uuid)
		info, _ := mapper.NewMapperInfo(schema, oldRow)
		match := false
		if len(where) == 0 {
			match = true
		} else {
			for _, condition := range where {
				field, _ := info.FieldByColumn(condition.Column)
				column := schema.Column(condition.Column)
				nativeValue, err := ovsdb.OvsToNative(column, condition.Value)
				if err != nil {
					panic(err)
				}
				ok, err := condition.Function.Evaluate(field, nativeValue)
				if err != nil {
					panic(err)
				}
				if ok {
					match = true
					count++
				}
			}
		}
		if match {
			old := targetDb.Table(table).Row(uuid)
			oldRow, err := targetDb.Mapper().NewRow(table, old)
			if err != nil {
				panic(err)
			}
			newRow, err := targetDb.Mapper().NewRow(table, row)
			if err != nil {
				panic(err)
			}
			targetDb.Table(table).Update(uuid, row)
			tableUpdate.AddRowUpdate(uuid, &ovsdb.RowUpdate{
				Old: &oldRow,
				New: &newRow,
			})
		}
	}
	// FIXME: We need to filter the returned columns
	return ovsdb.OperationResult{
			Count: count,
		}, ovsdb.TableUpdates{
			table: tableUpdate,
		}
}

func (db *inMemoryDatabase) Mutate(database, table string, where []ovsdb.Condition, mutations []ovsdb.Mutation) (ovsdb.OperationResult, ovsdb.TableUpdates) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var targetDb *cache.TableCache
	var ok bool
	if targetDb, ok = db.databases[database]; !ok {
		return ovsdb.OperationResult{
			Error: "database does not exist",
		}, nil
	}

	if t := targetDb.Table(table); t == nil {
		targetDb.Set(table, cache.NewRowCache(nil))
	}

	schema := targetDb.Mapper().Schema.Table(table)
	tableUpdate := make(ovsdb.TableUpdate)
	count := 0
	for _, uuid := range targetDb.Table(table).Rows() {
		row := targetDb.Table(table).Row(uuid)
		info, _ := mapper.NewMapperInfo(schema, row)
		match := false
		if len(where) == 0 {
			match = true
		} else {
			for _, condition := range where {
				field, _ := info.FieldByColumn(condition.Column)
				column := schema.Column(condition.Column)
				nativeValue, err := ovsdb.OvsToNative(column, condition.Value)
				if err != nil {
					panic(err)
				}
				ok, err := condition.Function.Evaluate(field, nativeValue)
				if err != nil {
					panic(err)
				}
				if ok {
					match = true
					count++
				}
			}
		}
		if match {
			for _, m := range mutations {
				column := schema.Column(m.Column)
				nativeValue, err := ovsdb.OvsToNative(column, m.Value)
				if err != nil {
					panic(err)
				}
				if err := ovsdb.ValidateMutation(column, m.Mutator, nativeValue); err != nil {
					panic(err)
				}
				oldRow, err := targetDb.Mapper().NewRow(table, row)
				if err != nil {
					panic(err)
				}
				current, _ := info.FieldByColumn(m.Column)
				new := mutate(current, m.Mutator, nativeValue)
				if err := info.SetField(m.Column, new); err != nil {
					panic(err)
				}
				newRow, err := targetDb.Mapper().NewRow(table, row)
				if err != nil {
					panic(err)
				}
				tableUpdate.AddRowUpdate(uuid, &ovsdb.RowUpdate{
					Old: &oldRow,
					New: &newRow,
				})
			}
		}
	}
	return ovsdb.OperationResult{
			Count: count,
		}, ovsdb.TableUpdates{
			table: tableUpdate,
		}
}

func (db *inMemoryDatabase) Delete(database, table string, where []ovsdb.Condition) (ovsdb.OperationResult, ovsdb.TableUpdates) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var targetDb *cache.TableCache
	var ok bool
	if targetDb, ok = db.databases[database]; !ok {
		return ovsdb.OperationResult{
			Error: "database does not exist",
		}, nil
	}

	if t := targetDb.Table(table); t == nil {
		targetDb.Set(table, cache.NewRowCache(nil))
	}

	schema := targetDb.Mapper().Schema.Table(table)
	tableUpdate := make(ovsdb.TableUpdate)
	count := 0
	for _, uuid := range targetDb.Table(table).Rows() {
		row := targetDb.Table(table).Row(uuid)
		info, _ := mapper.NewMapperInfo(schema, row)
		match := false
		if len(where) == 0 {
			match = true
		} else {
			for _, condition := range where {
				field, _ := info.FieldByColumn(condition.Column)
				column := schema.Column(condition.Column)
				nativeValue, err := ovsdb.OvsToNative(column, condition.Value)
				if err != nil {
					panic(err)
				}
				ok, err := condition.Function.Evaluate(field, nativeValue)
				if err != nil {
					panic(err)
				}
				if ok {
					match = true
					count++
				}
			}
		}
		if match {
			oldRow, err := targetDb.Mapper().NewRow(table, row)
			if err != nil {
				panic(err)
			}
			targetDb.Table(table).Delete(uuid)
			tableUpdate.AddRowUpdate(uuid, &ovsdb.RowUpdate{
				Old: &oldRow,
				New: nil,
			})
		}
	}
	return ovsdb.OperationResult{
			Count: count,
		}, ovsdb.TableUpdates{
			table: tableUpdate,
		}
}

func (db *inMemoryDatabase) Wait(database, table string, timeout int, conditions []ovsdb.Condition, columns []string, until string, rows []ovsdb.Row) ovsdb.OperationResult {
	return ovsdb.OperationResult{Error: ErrNotImplemented.Error()}
}

func (db *inMemoryDatabase) Commit(database, table string, durable bool) ovsdb.OperationResult {
	return ovsdb.OperationResult{Error: ErrNotImplemented.Error()}
}

func (db *inMemoryDatabase) Abort(database, table string) ovsdb.OperationResult {
	return ovsdb.OperationResult{Error: ErrNotImplemented.Error()}
}

func (db *inMemoryDatabase) Comment(database, table string, comment string) ovsdb.OperationResult {
	return ovsdb.OperationResult{Error: ErrNotImplemented.Error()}
}

func (db *inMemoryDatabase) Assert(database, table, lock string) ovsdb.OperationResult {
	return ovsdb.OperationResult{Error: ErrNotImplemented.Error()}
}

func mutate(current interface{}, mutator ovsdb.Mutator, value interface{}) interface{} {
	switch current.(type) {
	case bool, string:
		return current
	}
	switch mutator {
	case ovsdb.MutateOperationInsert:
		switch current.(type) {
		case int, float64:
			return current
		}
		vc := reflect.ValueOf(current)
		vv := reflect.ValueOf(value)
		if vc.Kind() == reflect.Slice && vc.Type() == reflect.SliceOf(vv.Type()) {
			v := reflect.Append(vc, vv)
			return v.Interface()
		}
		if vc.Kind() == reflect.Slice && vv.Kind() == reflect.Slice {
			v := reflect.AppendSlice(vc, vv)
			return v.Interface()
		}
	case ovsdb.MutateOperationDelete:
		switch current.(type) {
		case int, float64:
			return current
		}
		vc := reflect.ValueOf(current)
		vv := reflect.ValueOf(value)
		if vc.Kind() == reflect.Slice && vc.Type() == reflect.SliceOf(vv.Type()) {
			v := removeFromSlice(vc, vv)
			return v.Interface()
		}
		if vc.Kind() == reflect.Slice && vv.Kind() == reflect.Slice {
			v := vc
			for i := 0; i < vv.Len(); i++ {
				v = removeFromSlice(v, vv.Index(i))
			}
			return v.Interface()
		}
	case ovsdb.MutateOperationAdd:
		if i, ok := current.(int); ok {
			v := value.(int)
			return i + v
		}
		if i, ok := current.(float64); ok {
			v := value.(float64)
			return i + v
		}
		if is, ok := current.([]int); ok {
			v := value.(int)
			for i, j := range is {
				is[i] = j + v
			}
			return is
		}
		if is, ok := current.([]float64); ok {
			v := value.(float64)
			for i, j := range is {
				is[i] = j + v
			}
			return is
		}
	case ovsdb.MutateOperationSubtract:
		if i, ok := current.(int); ok {
			v := value.(int)
			return i - v
		}
		if i, ok := current.(float64); ok {
			v := value.(float64)
			return i - v
		}
		if is, ok := current.([]int); ok {
			v := value.(int)
			for i, j := range is {
				is[i] = j - v
			}
			return is
		}
		if is, ok := current.([]float64); ok {
			v := value.(float64)
			for i, j := range is {
				is[i] = j - v
			}
			return is
		}
	case ovsdb.MutateOperationMultiply:
		if i, ok := current.(int); ok {
			v := value.(int)
			return i * v
		}
		if i, ok := current.(float64); ok {
			v := value.(float64)
			return i * v
		}
		if is, ok := current.([]int); ok {
			v := value.(int)
			for i, j := range is {
				is[i] = j * v
			}
			return is
		}
		if is, ok := current.([]float64); ok {
			v := value.(float64)
			for i, j := range is {
				is[i] = j * v
			}
			return is
		}
	case ovsdb.MutateOperationDivide:
		if i, ok := current.(int); ok {
			v := value.(int)
			return i / v
		}
		if i, ok := current.(float64); ok {
			v := value.(float64)
			return i / v
		}
		if is, ok := current.([]int); ok {
			v := value.(int)
			for i, j := range is {
				is[i] = j / v
			}
			return is
		}
		if is, ok := current.([]float64); ok {
			v := value.(float64)
			for i, j := range is {
				is[i] = j / v
			}
			return is
		}
	case ovsdb.MutateOperationModulo:
		if i, ok := current.(int); ok {
			v := value.(int)
			return i % v
		}
		if is, ok := current.([]int); ok {
			v := value.(int)
			for i, j := range is {
				is[i] = j % v
			}
			return is
		}
	}
	return current
}

func removeFromSlice(a, b reflect.Value) reflect.Value {
	for i := 0; i < a.Len(); i++ {
		if a.Index(i).Interface() == b.Interface() {
			v := reflect.AppendSlice(a.Slice(0, i), a.Slice(i+1, a.Len()))
			return v
		}
	}
	return a
}
