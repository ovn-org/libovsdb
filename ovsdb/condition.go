package ovsdb

import (
	"encoding/json"
	"fmt"
)

type ConditionFunction string

const (
	ConditionLessThan           ConditionFunction = "<"
	ConditionLessThanOrEqual    ConditionFunction = "<="
	ConditionEqual              ConditionFunction = "=="
	ConditionNotEqual           ConditionFunction = "!="
	ConditionGreaterThan        ConditionFunction = ">"
	ConditionGreaterThanOrEqual ConditionFunction = ">="
	ConditionIncludes           ConditionFunction = "includes"
	ConditionExcludes           ConditionFunction = "excludes"
)

// Condition is described in RFC 7047: 5.1
type Condition struct {
	Column   string
	Function ConditionFunction
	Value    interface{}
}

// NewCondition returns a new condition
func NewCondition(column string, function ConditionFunction, value interface{}) Condition {
	return Condition{
		Column:   column,
		Function: function,
		Value:    value,
	}
}

// MarshalJSON marshals a condition to a 3 element JSON array
func (c Condition) MarshalJSON() ([]byte, error) {
	v := []interface{}{c.Column, c.Function, c.Value}
	return json.Marshal(v)
}

// UnmarshalJSON converts a 3 element JSON array to a Condition
func (c Condition) UnmarshalJSON(b []byte) error {
	var v []interface{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	if len(v) != 3 {
		return fmt.Errorf("expected a 3 element json array. there are %d elements", len(v))
	}
	c.Column = v[0].(string)
	function := ConditionFunction(v[1].(string))
	switch function {
	case ConditionEqual:
	case ConditionNotEqual:
	case ConditionIncludes:
	case ConditionExcludes:
	case ConditionGreaterThan:
	case ConditionGreaterThanOrEqual:
	case ConditionLessThan:
	case ConditionLessThanOrEqual:
		c.Function = function
	default:
		return fmt.Errorf("%s is not a valid function", function)
	}
	c.Value = v[2]
	return nil
}
