package client

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/libovsdb/ovsdb"
)

// Conditional is the interface used by the ConditionalAPI to match on cache objects
// and generate ovsdb conditions
type Conditional interface {
	// Generate returns a list of conditions to be used in Operations
	Generate() ([]ovsdb.Condition, error)
	// matches returns true if a model matches the condition
	Matches(m Model) (bool, error)
	// returns the table that this condition is associated with
	Table() string
}

// indexConditional uses the information available in a model to generate conditions
// The conditions are based on the equality of the first available index.
// The priority of indexes is: {user_provided fields}, uuid, {schema index}
type indexConditional struct {
	orm       *orm
	tableName string
	model     Model
	fields    []interface{}
}

func (c *indexConditional) Matches(m Model) (bool, error) {
	return c.orm.equalFields(c.tableName, c.model, m, c.fields...)
}

func (c *indexConditional) Table() string {
	return c.tableName
}

// Generate returns a condition based on the model and the field pointers
func (c *indexConditional) Generate() ([]ovsdb.Condition, error) {
	condition, err := c.orm.newCondition(c.tableName, c.model, c.fields...)
	if err != nil {
		return nil, err
	}
	return condition, nil
}

// newIndexCondition creates a new indexConditional
func newIndexConditional(orm *orm, table string, model Model, fields ...interface{}) (Conditional, error) {
	return &indexConditional{
		orm:       orm,
		tableName: table,
		model:     model,
		fields:    fields,
	}, nil
}

// predicateConditional is a Conditional that calls a provided function pointer
// to match on models.
type predicateConditional struct {
	tableName string
	predicate interface{}
	cache     *TableCache
}

// matches returns the result of the execution of the predicate
// Type verifications are not performed
func (c *predicateConditional) Matches(model Model) (bool, error) {
	ret := reflect.ValueOf(c.predicate).Call([]reflect.Value{reflect.ValueOf(model)})
	return ret[0].Bool(), nil
}

func (c *predicateConditional) Table() string {
	return c.tableName
}

// generate returns a list of conditions that match, by _uuid equality, all the objects that
// match the predicate
func (c *predicateConditional) Generate() ([]ovsdb.Condition, error) {
	allConditions := make([]ovsdb.Condition, 0)
	tableCache := c.cache.Table(c.tableName)
	if tableCache == nil {
		return nil, ErrNotFound
	}
	for _, row := range tableCache.Rows() {
		elem := tableCache.Row(row)
		match, err := c.Matches(elem)
		if err != nil {
			return nil, err
		}
		if match {
			elemCond, err := c.cache.orm.newCondition(c.tableName, elem)
			if err != nil {
				return nil, err
			}
			allConditions = append(allConditions, elemCond...)
		}
	}
	return allConditions, nil
}

// newIndexCondition creates a new predicateConditional
func newPredicateConditional(table string, cache *TableCache, predicate interface{}) (Conditional, error) {
	return &predicateConditional{
		tableName: table,
		predicate: predicate,
		cache:     cache,
	}, nil
}

// errorConditiona  is a conditional that encapsulates an error
// It is used to delay the reporting of errors from conditional creation to API method call
type errorConditional struct {
	err error
}

func (e *errorConditional) Matches(Model) (bool, error) {
	return false, e.err
}

func (e *errorConditional) Table() string {
	return ""
}

func (e *errorConditional) Generate() ([]ovsdb.Condition, error) {
	return nil, e.err
}

func newErrorConditional(err error) Conditional {
	return &errorConditional{
		err: fmt.Errorf("conditionerror: %s", err.Error()),
	}
}
