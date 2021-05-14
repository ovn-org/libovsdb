package client

import (
	"fmt"
	"reflect"

	"github.com/ovn-org/libovsdb/ovsdb"
)

// ConditionFactory is the interface used by the ConditionalAPI to match on cache objects
// and generate operation conditions
type ConditionFactory interface {
	// Generate returns a list of conditions to be used in Operations
	Generate() ([]ovsdb.Condition, error)
	// matches returns true if a model matches the condition
	Matches(m Model) (bool, error)
	// returns the table that this condition is associated with
	Table() string
}

// equalityCond uses the information available in a model to generate conditions
// The conditions are based on the equality of the first available index.
// The priority of indexes is: uuid, {schema index}
type equalityCondFactory struct {
	orm       *orm
	tableName string
	model     Model
}

func (c *equalityCondFactory) Matches(m Model) (bool, error) {
	return c.orm.equalFields(c.tableName, c.model, m)
}

func (c *equalityCondFactory) Table() string {
	return c.tableName
}

// Generate returns a condition based on the model and the field pointers
func (c *equalityCondFactory) Generate() ([]ovsdb.Condition, error) {
	condition, err := c.orm.newEqualityCondition(c.tableName, c.model)
	if err != nil {
		return nil, err
	}
	return condition, nil
}

// newIndexCondition creates a new equalityCondFactory
func newEqualityConditionFactory(orm *orm, table string, model Model, fields ...interface{}) (ConditionFactory, error) {
	return &equalityCondFactory{
		orm:       orm,
		tableName: table,
		model:     model,
	}, nil
}

// explicitCondFactory generates conditions based on the provided Condition list
type explicitCondFactory struct {
	orm        *orm
	tableName  string
	model      Model
	conditions []Condition
}

func (c *explicitCondFactory) Matches(m Model) (bool, error) {
	return false, fmt.Errorf("Cannot perform Cache comparisons using explicit Conditions")
}

func (c *explicitCondFactory) Table() string {
	return c.tableName
}

// Generate returns a condition based on the model and the field pointers
func (c *explicitCondFactory) Generate() ([]ovsdb.Condition, error) {
	var result []ovsdb.Condition
	for _, cond := range c.conditions {
		ovsdbCond, err := c.orm.newCondition(c.tableName, c.model, cond)
		if err != nil {
			return nil, err
		}
		result = append(result, *ovsdbCond)
	}
	return result, nil
}

// newIndexCondition creates a new equalityCondFactory
func newExplicitConditionFactory(orm *orm, table string, model Model, cond ...Condition) (ConditionFactory, error) {
	return &explicitCondFactory{
		orm:        orm,
		tableName:  table,
		model:      model,
		conditions: cond,
	}, nil
}

// predicateCondFactory is a conditionFactory that calls a provided function pointer
// to match on models.
type predicateCondFactory struct {
	tableName string
	predicate interface{}
	cache     *TableCache
}

// matches returns the result of the execution of the predicate
// Type verifications are not performed
func (c *predicateCondFactory) Matches(model Model) (bool, error) {
	ret := reflect.ValueOf(c.predicate).Call([]reflect.Value{reflect.ValueOf(model)})
	return ret[0].Bool(), nil
}

func (c *predicateCondFactory) Table() string {
	return c.tableName
}

// generate returns a list of conditions that match, by _uuid equality, all the objects that
// match the predicate
func (c *predicateCondFactory) Generate() ([]ovsdb.Condition, error) {
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
			elemCond, err := c.cache.orm.newEqualityCondition(c.tableName, elem)
			if err != nil {
				return nil, err
			}
			allConditions = append(allConditions, elemCond...)
		}
	}
	return allConditions, nil
}

// newIndexCondition creates a new predicateCondFactory
func newPredicateConditionFactory(table string, cache *TableCache, predicate interface{}) (ConditionFactory, error) {
	return &predicateCondFactory{
		tableName: table,
		predicate: predicate,
		cache:     cache,
	}, nil
}

// errorCondFactoryition is a condition that encapsulates an error
// It is used to delay the reporting of errors from condition creation to method call
type errorConditionFactory struct {
	err error
}

func (e *errorConditionFactory) Matches(Model) (bool, error) {
	return false, e.err
}

func (e *errorConditionFactory) Table() string {
	return ""
}

func (e *errorConditionFactory) Generate() ([]ovsdb.Condition, error) {
	return nil, e.err
}

func newErrorConditionFactory(err error) ConditionFactory {
	return &errorConditionFactory{
		err: fmt.Errorf("conditionerror: %s", err.Error()),
	}
}
