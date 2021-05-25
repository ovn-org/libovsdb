package client

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

const (
	opInsert string = "insert"
	opMutate string = "mutate"
	opUpdate string = "insert"
	opDelete string = "delete"
)

// API defines basic operations to interact with the database
type API interface {
	// List populates a slice of Models objects based on their type
	// The function parameter must be a pointer to a slice of Models
	// If the slice is null, the entire cache will be copied into the slice
	// If it has a capacity != 0, only 'capacity' elements will be filled in
	List(result interface{}) error

	// Create a Conditional API from a Function that is used to filter cached data
	// The function must accept a Model implementation and return a boolean. E.g:
	// ConditionFromFunc(func(l *LogicalSwitch) bool { return l.Enabled })
	WhereCache(predicate interface{}) ConditionalAPI

	// Create a ConditionalAPI from a Model's index data or a list of Conditions
	// where operations apply to elements that match any of the conditions
	// If no condition is given, it will match the values provided in model.Model according
	// to the database index.
	Where(model.Model, ...Condition) ConditionalAPI

	// Create a ConditionalAPI from a Model's index data or a list of Conditions
	// where operations apply to elements that match all the conditions
	WhereAll(model.Model, ...Condition) ConditionalAPI

	// Get retrieves a model from the cache
	// The way the object will be fetch depends on the data contained in the
	// provided model and the indexes defined in the associated schema
	// For more complex ways of searching for elements in the cache, the
	// preferred way is Where({condition}).List()
	Get(model.Model) error

	// Create returns the operation needed to add the model(s) to the Database
	// Only fields with non-default values will be added to the transaction
	// If the field associated with column "_uuid" has some content, it will be
	// treated as named-uuid
	Create(...model.Model) ([]ovsdb.Operation, error)
}

// ConditionalAPI is an interface used to perform operations that require / use Conditions
type ConditionalAPI interface {
	// List uses the condition to search on the cache and populates
	// the slice of Models objects based on their type
	List(result interface{}) error

	// Mutate returns the operations needed to perform the mutation specified
	// By the model and the list of Mutation objects
	// Depending on the Condition, it might return one or many operations
	Mutate(model.Model, ...Mutation) ([]ovsdb.Operation, error)

	// Update returns the operations needed to update any number of rows according
	// to the data in the given model.
	// By default, all the non-default values contained in model will be updated.
	// Optional fields can be passed (pointer to fields in the model) to select the
	// the fields to be updated
	Update(model.Model, ...interface{}) ([]ovsdb.Operation, error)

	// Delete returns the Operations needed to delete the models seleted via the condition
	Delete() ([]ovsdb.Operation, error)
}

// Mutation is a type that represents a OVSDB Mutation
type Mutation struct {
	// Pointer to the field of the model that shall be mutated
	Field interface{}
	// String representing the mutator (as per RFC7047)
	Mutator ovsdb.Mutator
	// Value to use in the mutation
	Value interface{}
}

// Condition is a type that represents a OVSDB Condition
type Condition struct {
	// Pointer to the field of the model where the operation applies
	Field interface{}
	// Condition function
	Function ovsdb.ConditionFunction
	// Value to use in the condition
	Value interface{}
}

// ErrWrongType is used to report the user provided parameter has the wrong type
type ErrWrongType struct {
	inputType reflect.Type
	reason    string
}

func (e *ErrWrongType) Error() string {
	return fmt.Sprintf("Wrong parameter type (%s): %s", e.inputType, e.reason)
}

// ErrNotFound is used to inform the object or table was not found in the cache
var ErrNotFound = errors.New("object not found")

// api struct implements both API and ConditionalAPI
// Where() can be used to create a ConditionalAPI api
type api struct {
	cache *TableCache
	cond  Conditional
}

// List populates a slice of Models given as parameter based on the configured Condition
func (a api) List(result interface{}) error {
	resultPtr := reflect.ValueOf(result)
	if resultPtr.Type().Kind() != reflect.Ptr {
		return &ErrWrongType{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	resultVal := reflect.Indirect(resultPtr)
	if resultVal.Type().Kind() != reflect.Slice {
		return &ErrWrongType{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	table, err := a.getTableFromModel(reflect.New(resultVal.Type().Elem()).Interface())
	if err != nil {
		return err
	}

	if a.cond != nil && a.cond.Table() != table {
		return &ErrWrongType{resultPtr.Type(),
			fmt.Sprintf("Table derived from input type (%s) does not match Table from Condition (%s)", table, a.cond.Table())}
	}

	tableCache := a.cache.Table(table)
	if tableCache == nil {
		return ErrNotFound
	}

	// If given a null slice, fill it in the cache table completely, if not, just up to
	// its capability
	if resultVal.IsNil() || resultVal.Cap() == 0 {
		resultVal.Set(reflect.MakeSlice(resultVal.Type(), 0, tableCache.Len()))
	}
	i := resultVal.Len()

	for _, row := range tableCache.Rows() {
		elem := tableCache.Row(row)
		if i >= resultVal.Cap() {
			break
		}

		if a.cond != nil {
			if matches, err := a.cond.Matches(elem); err != nil {
				return err
			} else if !matches {
				continue
			}
		}

		resultVal.Set(reflect.Append(resultVal, reflect.Indirect(reflect.ValueOf(elem))))
		i++
	}
	return nil
}

// Where returns a conditionalAPI based on a Condition list
func (a api) Where(model model.Model, cond ...Condition) ConditionalAPI {
	return newConditionalAPI(a.cache, a.conditionFromModel(false, model, cond...))
}

// Where returns a conditionalAPI based on a Condition list
func (a api) WhereAll(model model.Model, cond ...Condition) ConditionalAPI {
	return newConditionalAPI(a.cache, a.conditionFromModel(true, model, cond...))
}

// Where returns a conditionalAPI based a Predicate
func (a api) WhereCache(predicate interface{}) ConditionalAPI {
	return newConditionalAPI(a.cache, a.conditionFromFunc(predicate))
}

// Conditional interface implementation
// FromFunc returns a Condition from a function
func (a api) conditionFromFunc(predicate interface{}) Conditional {
	table, err := a.getTableFromFunc(predicate)
	if err != nil {
		return newErrorConditional(err)
	}

	condition, err := newPredicateConditional(table, a.cache, predicate)
	if err != nil {
		return newErrorConditional(err)
	}
	return condition
}

// FromModel returns a Condition from a model and a list of fields
func (a api) conditionFromModel(any bool, model model.Model, cond ...Condition) Conditional {
	var conditional Conditional
	var err error

	tableName, err := a.getTableFromModel(model)
	if tableName == "" {
		return newErrorConditional(err)
	}

	if len(cond) == 0 {
		conditional, err = newEqualityConditional(a.cache.mapper, tableName, any, model)
		if err != nil {
			conditional = newErrorConditional(err)
		}

	} else {
		conditional, err = newExplicitConditional(a.cache.mapper, tableName, any, model, cond...)
		if err != nil {
			conditional = newErrorConditional(err)
		}
	}
	return conditional
}

// Get is a generic Get function capable of returning (through a provided pointer)
// a instance of any row in the cache.
// 'result' must be a pointer to an Model that exists in the DBModel
//
// The way the cache is search depends on the fields already populated in 'result'
// Any table index (including _uuid) will be used for comparison
func (a api) Get(m model.Model) error {
	table, err := a.getTableFromModel(m)
	if err != nil {
		return err
	}

	tableCache := a.cache.Table(table)
	if tableCache == nil {
		return ErrNotFound
	}

	// If model contains _uuid value, we can access it via cache index
	mapperInfo, err := mapper.NewMapperInfo(a.cache.mapper.Schema.Table(table), m)
	if err != nil {
		return err
	}
	if uuid, err := mapperInfo.FieldByColumn("_uuid"); err != nil && uuid != nil {
		if found := tableCache.Row(uuid.(string)); found == nil {
			return ErrNotFound
		} else {
			reflect.ValueOf(m).Elem().Set(reflect.Indirect(reflect.ValueOf(found)))
			return nil
		}
	}

	// Look across the entire cache for table index equality
	for _, row := range tableCache.Rows() {
		elem := tableCache.Row(row)
		equal, err := a.cache.mapper.EqualFields(table, m, elem.(model.Model))
		if err != nil {
			return err
		}
		if equal {
			reflect.ValueOf(m).Elem().Set(reflect.Indirect(reflect.ValueOf(elem)))
			return nil
		}
	}
	return ErrNotFound
}

// Create is a generic function capable of creating any row in the DB
// A valud Model (pointer to object) must be provided.
func (a api) Create(models ...model.Model) ([]ovsdb.Operation, error) {
	var operations []ovsdb.Operation

	for _, model := range models {
		var namedUUID string
		var err error

		tableName, err := a.getTableFromModel(model)
		if err != nil {
			return nil, err
		}

		table := a.cache.mapper.Schema.Table(tableName)

		// Read _uuid field, and use it as named-uuid
		info, err := mapper.NewMapperInfo(table, model)
		if err != nil {
			return nil, err
		}
		if uuid, err := info.FieldByColumn("_uuid"); err == nil {
			namedUUID = uuid.(string)
		} else {
			return nil, err
		}

		row, err := a.cache.mapper.NewRow(tableName, model)
		if err != nil {
			return nil, err
		}

		operations = append(operations, ovsdb.Operation{
			Op:       opInsert,
			Table:    tableName,
			Row:      row,
			UUIDName: namedUUID,
		})
	}
	return operations, nil
}

// Mutate returns the operations needed to transform the one Model into another one
func (a api) Mutate(model model.Model, mutationObjs ...Mutation) ([]ovsdb.Operation, error) {
	var mutations []interface{}
	var operations []ovsdb.Operation

	if len(mutationObjs) < 1 {
		return nil, fmt.Errorf("At least one Mutation must be provided")
	}

	tableName := a.cache.dbModel.FindTable(reflect.ValueOf(model).Type())
	table := a.cache.mapper.Schema.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("schema error: table not found in Database Model for type %s", reflect.TypeOf(model))
	}

	conditions, err := a.cond.Generate()
	if err != nil {
		return nil, err
	}

	info, err := mapper.NewMapperInfo(table, model)
	if err != nil {
		return nil, err
	}

	for _, mobj := range mutationObjs {
		col, err := info.ColumnByPtr(mobj.Field)
		if err != nil {
			return nil, err
		}

		mutation, err := a.cache.mapper.NewMutation(tableName, model, col, mobj.Mutator, mobj.Value)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	for _, condition := range conditions {
		operations = append(operations,
			ovsdb.Operation{
				Op:        opMutate,
				Table:     tableName,
				Mutations: mutations,
				Where:     condition,
			},
		)
	}

	return operations, nil
}

// Update is a generic function capable of updating any field in any row in the database
// Additional fields can be passed (variadic opts) to indicate fields to be updated
func (a api) Update(model model.Model, fields ...interface{}) ([]ovsdb.Operation, error) {
	var operations []ovsdb.Operation
	table, err := a.getTableFromModel(model)
	if err != nil {
		return nil, err
	}

	conditions, err := a.cond.Generate()
	if err != nil {
		return nil, err
	}

	row, err := a.cache.mapper.NewRow(table, model, fields...)
	if err != nil {
		return nil, err
	}

	for _, condition := range conditions {
		operations = append(operations,
			ovsdb.Operation{
				Op:    opUpdate,
				Table: table,
				Row:   row,
				Where: condition,
			},
		)
	}
	return operations, nil
}

// Delete returns the Operation needed to delete the selected models from the database
func (a api) Delete() ([]ovsdb.Operation, error) {
	var operations []ovsdb.Operation
	conditions, err := a.cond.Generate()
	if err != nil {
		return nil, err
	}

	for _, condition := range conditions {
		operations = append(operations,
			ovsdb.Operation{
				Op:    opDelete,
				Table: a.cond.Table(),
				Where: condition,
			},
		)
	}

	return operations, nil
}

// getTableFromModel returns the table name from a Model object after performing
// type verifications on the model
func (a api) getTableFromModel(m interface{}) (string, error) {
	if _, ok := m.(model.Model); !ok {
		return "", &ErrWrongType{reflect.TypeOf(m), "Type does not implement Model interface"}
	}
	table := a.cache.dbModel.FindTable(reflect.TypeOf(m))
	if table == "" {
		return "", &ErrWrongType{reflect.TypeOf(m), "Model not found in Database Model"}
	}
	return table, nil
}

// getTableFromModel returns the table name from a the predicate after performing
// type verifications
func (a api) getTableFromFunc(predicate interface{}) (string, error) {
	predType := reflect.TypeOf(predicate)
	if predType == nil || predType.Kind() != reflect.Func {
		return "", &ErrWrongType{predType, "Expected function"}
	}
	if predType.NumIn() != 1 || predType.NumOut() != 1 || predType.Out(0).Kind() != reflect.Bool {
		return "", &ErrWrongType{predType, "Expected func(Model) bool"}
	}

	modelInterface := reflect.TypeOf((*model.Model)(nil)).Elem()
	modelType := predType.In(0)
	if !modelType.Implements(modelInterface) {
		return "", &ErrWrongType{predType,
			fmt.Sprintf("Type %s does not implement Model interface", modelType.String())}
	}

	table := a.cache.dbModel.FindTable(modelType)
	if table == "" {
		return "", &ErrWrongType{predType,
			fmt.Sprintf("Model %s not found in Database Model", modelType.String())}
	}
	return table, nil
}

// newAPI returns a new API to interact with the database
func newAPI(cache *TableCache) API {
	return api{
		cache: cache,
	}
}

// newConditionalAPI returns a new ConditionalAPI to interact with the database
func newConditionalAPI(cache *TableCache, cond Conditional) ConditionalAPI {
	return api{
		cache: cache,
		cond:  cond,
	}
}
