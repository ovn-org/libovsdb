package libovsdb

import (
	"errors"
	"fmt"
	"reflect"
)

// API defines basic operations to interact with the database
type API interface {
	// List populates a slice of Models objects based on their type
	// The function parameter must be a pointer to a slice of Models
	// If the slice is null, the entire cache will be copied into the slice
	// If it has a capacity != 0, only 'capacity' elements will be filled in
	List(result interface{}) error

	// Create a Condition from a Function that is used to filter cached data
	// The function must accept a Model implementation and return a boolean. E.g:
	// ConditionFromFunc(func(l *LogicalSwitch) bool { return l.Enabled })
	ConditionFromFunc(predicate interface{}) Condition

	// Create a Condition from a Model's data. It uses the database indexes
	// to search the most apropriate field to use for matches and conditions
	// Optionally, a list of fields can indicate an alternative index
	ConditionFromModel(Model, ...interface{}) Condition

	// Create a ConditionalAPI from a Condition
	Where(condition Condition) ConditionalAPI
}

// ConditionalAPI is an interface used to perform operations that require / use Conditions
type ConditionalAPI interface {
	API
}

// InputTypeError is used to report the user provided parameter has the wrong type
type InputTypeError struct {
	inputType reflect.Type
	reason    string
}

func (e *InputTypeError) Error() string {
	return fmt.Sprintf("Wrong parameter type (%s): %s", e.inputType, e.reason)
}

// ConditionError is a wrapper around an error that is used to
// indicate the error occurred during condition creation
type ConditionError struct {
	err string
}

func (c ConditionError) Error() string {
	return fmt.Sprintf("Condition Error: %s", c.err)
}
func (c ConditionError) String() string {
	return c.Error()
}

// ErrNotFound is used to inform the object or table was not found in the cache
var ErrNotFound = errors.New("Object not found")

// api struct implements both API and ConditionalAPI
// Where() can be used to create a ConditionalAPI api
type api struct {
	cache *TableCache
	cond  Condition
}

// List populates a slice of Models given as parameter based on the configured Condition
func (a api) List(result interface{}) error {
	resultPtr := reflect.ValueOf(result)
	if resultPtr.Type().Kind() != reflect.Ptr {
		return &InputTypeError{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	resultVal := reflect.Indirect(resultPtr)
	if resultVal.Type().Kind() != reflect.Slice {
		return &InputTypeError{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	table, err := a.getTableFromModel(reflect.New(resultVal.Type().Elem()).Interface())
	if err != nil {
		return err
	}

	if a.cond != nil && a.cond.Table() != table {
		return &InputTypeError{resultPtr.Type(),
			fmt.Sprintf("Table derived from input type (%s) does not match Table from Condition (%s)", table, a.cond.Table())}
	}

	tableCache := a.cache.Table(table)
	if tableCache == nil {
		return ErrNotFound
	}

	// If given a null slice, fill it in the cache table completely, if not, just up to
	// its capability
	if resultVal.IsNil() {
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

// Where returns a conditionalAPI based a Condition
func (a api) Where(condition Condition) ConditionalAPI {
	return newConditionalAPI(a.cache, condition)
}

// ConditionFactory interface implementation
// FromFunc returns a Condition from a function
func (a api) ConditionFromFunc(predicate interface{}) Condition {
	table, err := a.getTableFromFunc(predicate)
	if err != nil {
		return newErrorCondition(err)
	}

	condition, err := newPredicateCond(table, predicate)
	if err != nil {
		return newErrorCondition(err)
	}
	return condition
}

// FromModel returns a Condition from a model and a list of fields
func (a api) ConditionFromModel(model Model, fields ...interface{}) Condition {
	tableName, err := a.getTableFromModel(model)
	if tableName == "" {
		return newErrorCondition(err)
	}
	condition, err := newIndexCondition(a.cache.orm, tableName, model, fields...)
	if err != nil {
		return newErrorCondition(err)
	}
	return condition
}

// getTableFromModel returns the table name from a Model object after performing
// type verifications on the model
func (a api) getTableFromModel(model interface{}) (string, error) {
	if _, ok := model.(Model); !ok {
		return "", &InputTypeError{reflect.TypeOf(model), "Type does not implement Model interface"}
	}

	table := a.cache.dbModel.FindTable(reflect.TypeOf(model))
	if table == "" {
		return "", &InputTypeError{reflect.TypeOf(model), "Model not found in Database Model"}
	}

	return table, nil
}

// getTableFromModel returns the table name from a the predicate after performing
// type verifications
func (a api) getTableFromFunc(predicate interface{}) (string, error) {
	predType := reflect.TypeOf(predicate)
	if predType == nil || predType.Kind() != reflect.Func {
		return "", &InputTypeError{predType, "Expected function"}
	}
	if predType.NumIn() != 1 || predType.NumOut() != 1 || predType.Out(0).Kind() != reflect.Bool {
		return "", &InputTypeError{predType, "Expected func(Model) bool"}
	}

	modelInterface := reflect.TypeOf((*Model)(nil)).Elem()
	modelType := predType.In(0)
	if !modelType.Implements(modelInterface) {
		return "", &InputTypeError{predType,
			fmt.Sprintf("Type %s does not implement Model interface", modelType.String())}
	}

	table := a.cache.dbModel.FindTable(modelType)
	if table == "" {
		return "", &InputTypeError{predType,
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
func newConditionalAPI(cache *TableCache, cond Condition) ConditionalAPI {
	return api{
		cache: cache,
		cond:  cond,
	}
}
