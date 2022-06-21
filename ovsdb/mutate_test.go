package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMutateAdd(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"add int",
			1,
			MutateOperationAdd,
			1,
			2,
		},
		{
			"add float",
			1.0,
			MutateOperationAdd,
			1.0,
			2.0,
		},
		{
			"add float set",
			[]float64{1.0, 2.0, 3.0},
			MutateOperationAdd,
			1.0,
			[]float64{2.0, 3.0, 4.0},
		},
		{
			"add int set float",
			[]int{1, 2, 3},
			MutateOperationAdd,
			1,
			[]int{2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, diff)
		})
	}
}

func TestMutateSubtract(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
	}{

		{
			"subtract int",
			1,
			MutateOperationSubtract,
			1,
			0,
		},
		{
			"subtract float",
			1.0,
			MutateOperationSubtract,
			1.0,
			0.0,
		},
		{
			"subtract float set",
			[]float64{1.0, 2.0, 3.0},
			MutateOperationSubtract,
			1.0,
			[]float64{0.0, 1.0, 2.0},
		},
		{
			"subtract int set",
			[]int{1, 2, 3},
			MutateOperationSubtract,
			1,
			[]int{0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, diff)
		})
	}
}

func TestMutateMultiply(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
	}{

		{
			"multiply int",
			1,
			MutateOperationMultiply,
			2,
			2,
		},
		{
			"multiply float",
			1.0,
			MutateOperationMultiply,
			2.0,
			2.0,
		},
		{
			"multiply float set",
			[]float64{1.0, 2.0, 3.0},
			MutateOperationMultiply,
			2.0,
			[]float64{2.0, 4.0, 6.0},
		},
		{
			"multiply int set",
			[]int{1, 2, 3},
			MutateOperationMultiply,
			2,
			[]int{2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, diff)
		})
	}
}

func TestMutateDivide(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"divide int",
			10,
			MutateOperationDivide,
			2,
			5,
		},
		{
			"divide float",
			1.0,
			MutateOperationDivide,
			2.0,
			0.5,
		},
		{
			"divide float set",
			[]float64{1.0, 2.0, 4.0},
			MutateOperationDivide,
			2.0,
			[]float64{0.5, 1.0, 2.0},
		},
		{
			"divide int set",
			[]int{10, 20, 30},
			MutateOperationDivide,
			5,
			[]int{2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, diff)
		})
	}
}

func TestMutateModulo(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"modulo int",
			3,
			MutateOperationModulo,
			2,
			1,
		},
		{
			"modulo int set",
			[]int{3, 5, 7},
			MutateOperationModulo,
			2,
			[]int{1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, diff)
		})
	}
}

func TestMutateInsert(t *testing.T) {
	var nilSlice []string
	var nilMap map[string]string
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
		diff    interface{}
	}{
		{
			"insert single string",
			[]string{"foo", "bar"},
			MutateOperationInsert,
			"baz",
			[]string{"foo", "bar", "baz"},
			"baz",
		},
		{
			"insert in to nil value",
			nil,
			MutateOperationInsert,
			[]string{"foo"},
			[]string{"foo"},
			[]string{"foo"},
		},
		{
			"insert in to nil slice",
			nilSlice,
			MutateOperationInsert,
			[]string{"foo"},
			[]string{"foo"},
			[]string{"foo"},
		},
		{
			"insert existing string",
			[]string{"foo", "bar", "baz"},
			MutateOperationInsert,
			"baz",
			[]string{"foo", "bar", "baz"},
			nil,
		},
		{
			"insert multiple string",
			[]string{"foo", "bar"},
			MutateOperationInsert,
			[]string{"baz", "quux", "foo"},
			[]string{"foo", "bar", "baz", "quux"},
			[]string{"baz", "quux"},
		},
		{
			"insert key value pairs",
			map[string]string{
				"foo": "bar",
			},
			MutateOperationInsert,
			map[string]string{
				"foo": "ignored",
				"baz": "quux",
			},
			map[string]string{
				"foo": "bar",
				"baz": "quux",
			},
			map[string]string{
				"baz": "quux",
			},
		},
		{
			"insert key value pairs on nil value",
			nil,
			MutateOperationInsert,
			map[string]string{
				"foo": "bar",
			},
			map[string]string{
				"foo": "bar",
			},
			map[string]string{
				"foo": "bar",
			},
		},
		{
			"insert key value pairs on nil map",
			nilMap,
			MutateOperationInsert,
			map[string]string{
				"foo": "bar",
			},
			map[string]string{
				"foo": "bar",
			},
			map[string]string{
				"foo": "bar",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.diff, diff)
		})
	}
}

func TestMutateDelete(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator Mutator
		value   interface{}
		want    interface{}
		diff    interface{}
	}{
		{
			"delete single string",
			[]string{"foo", "bar"},
			MutateOperationDelete,
			"bar",
			[]string{"foo"},
			"bar",
		},
		{
			"delete multiple string",
			[]string{"foo", "bar", "baz"},
			MutateOperationDelete,
			[]string{"bar", "baz"},
			[]string{"foo"},
			[]string{"bar", "baz"},
		},
		{
			"delete key value pairs",
			map[string]string{
				"foo": "bar",
				"baz": "quux",
			},
			MutateOperationDelete,
			map[string]string{
				"foo": "ignored",
				"baz": "quux",
			},
			map[string]string{
				"foo": "bar",
			},
			map[string]string{
				"baz": "quux",
			},
		},
		{
			"delete keys",
			map[string]string{
				"foo": "bar",
				"baz": "quux",
			},
			MutateOperationDelete,
			[]string{"foo"},
			map[string]string{
				"baz": "quux",
			},
			map[string]string{
				"foo": "bar",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, diff := Mutate(tt.current, tt.mutator, tt.value)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.diff, diff)
		})
	}
}
