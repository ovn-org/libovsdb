package libovsdb

import (
	"fmt"
	"reflect"
)

// Condition is the interface used by the ConditionalAPI to match on cache objects
// and generate operation conditions
type Condition interface {
	// matches returns true if a model matches the condition
	Matches(m Model) (bool, error)
	// returns the table that this condition is associated with
	Table() string
}

// indexCond uses the information available in a model to generate conditions
// The conditions are based on the equality of the first available index.
// The priority of indexes is: {user_provided fields}, uuid, {schema index}
type indexCond struct {
	orm       *orm
	tableName string
	model     Model
	fields    []interface{}
}

func (c *indexCond) Matches(m Model) (bool, error) {
	return c.orm.equalFields(c.tableName, c.model, m, c.fields...)
}

func (c *indexCond) Table() string {
	return c.tableName
}

// newIndexCondition creates a new indexCond
func newIndexCondition(orm *orm, table string, model Model, fields ...interface{}) (Condition, error) {
	return &indexCond{
		orm:       orm,
		tableName: table,
		model:     model,
		fields:    fields,
	}, nil
}

// predicateCond is a conditionFactory that calls a provided function pointer
// to match on models.
type predicateCond struct {
	tableName string
	predicate interface{}
}

// matches returns the result of the execution of the predicate
// Type verifications are not performed
func (c *predicateCond) Matches(model Model) (bool, error) {
	ret := reflect.ValueOf(c.predicate).Call([]reflect.Value{reflect.ValueOf(model)})
	return ret[0].Bool(), nil
}

func (c *predicateCond) Table() string {
	return c.tableName
}

// newIndexCondition creates a new predicateCond
func newPredicateCond(table string, predicate interface{}) (Condition, error) {
	return &predicateCond{
		tableName: table,
		predicate: predicate,
	}, nil
}

// errorCondition is a condition that encapsulates an error
// It is used to delay the reporting of errors from condition creation to method call
type errorCondition struct {
	err error
}

func (e *errorCondition) Matches(Model) (bool, error) {
	return false, e.err
}
func (e *errorCondition) Table() string {
	return ""
}

func newErrorCondition(err error) Condition {
	return &errorCondition{
		err: fmt.Errorf("ConditionError: %s", err.Error()),
	}
}
