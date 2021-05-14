package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConditionMarshalJSON(t *testing.T) {
	type fields struct {
		Column   string
		Function ConditionFunction
		Value    interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			"test <",
			fields{"foo", ConditionLessThan, "bar"},
			`[ "foo", "<", "bar" ]`,
			false,
		},
		{
			"test <=",
			fields{"foo", ConditionLessThanOrEqual, "bar"},
			`[ "foo", "<=", "bar" ]`,
			false,
		},
		{
			"test >",
			fields{"foo", ConditionGreaterThan, "bar"},
			`[ "foo", ">", "bar" ]`,
			false,
		},
		{
			"test >=",
			fields{"foo", ConditionGreaterThanOrEqual, "bar"},
			`[ "foo", ">=", "bar" ]`,
			false,
		},
		{
			"test ==",
			fields{"foo", ConditionEqual, "bar"},
			`[ "foo", "==", "bar" ]`,
			false,
		},
		{
			"test !=",
			fields{"foo", ConditionNotEqual, "bar"},
			`[ "foo", "!=", "bar" ]`,
			false,
		},
		{
			"test includes",
			fields{"foo", ConditionIncludes, "bar"},
			`[ "foo", "includes", "bar" ]`,
			false,
		},
		{
			"test excludes",
			fields{"foo", ConditionExcludes, "bar"},
			`[ "foo", "excludes", "bar" ]`,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Condition{
				Column:   tt.fields.Column,
				Function: tt.fields.Function,
				Value:    tt.fields.Value,
			}
			got, err := c.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("Condition.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestCondition_UnmarshalJSON(t *testing.T) {
	type fields struct {
		Column   string
		Function ConditionFunction
		Value    interface{}
	}
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"success",
			fields{"foo", ConditionEqual, "bar"},
			args{[]byte(`[ "foo", "==", "bar" ]`)},
			false,
		},
		{
			"bad function",
			fields{},
			args{[]byte(`[ "foo", "baz", "bar" ]`)},
			true,
		},
		{
			"too many elements",
			fields{},
			args{[]byte(`[ "foo", "bar", "baz", "quuz" ]`)},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Condition{
				Column:   tt.fields.Column,
				Function: tt.fields.Function,
				Value:    tt.fields.Value,
			}
			if err := c.UnmarshalJSON(tt.args.b); (err != nil) != tt.wantErr {
				t.Errorf("Condition.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
