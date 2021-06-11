package server

import (
	"reflect"
	"testing"

	"github.com/ovn-org/libovsdb/ovsdb"
)

func TestMutateAdd(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"add int",
			1,
			ovsdb.MutateOperationAdd,
			1,
			2,
		},
		{
			"add float",
			1.0,
			ovsdb.MutateOperationAdd,
			1.0,
			2.0,
		},
		{
			"add float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationAdd,
			1.0,
			[]float64{2.0, 3.0, 4.0},
		},
		{
			"add int set float",
			[]int{1, 2, 3},
			ovsdb.MutateOperationAdd,
			1,
			[]int{2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateSubtract(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{

		{
			"subtract int",
			1,
			ovsdb.MutateOperationSubtract,
			1,
			0,
		},
		{
			"subtract float",
			1.0,
			ovsdb.MutateOperationSubtract,
			1.0,
			0.0,
		},
		{
			"subtract float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationSubtract,
			1.0,
			[]float64{0.0, 1.0, 2.0},
		},
		{
			"subtract int set",
			[]int{1, 2, 3},
			ovsdb.MutateOperationSubtract,
			1,
			[]int{0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateMultiply(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{

		{
			"multiply int",
			1,
			ovsdb.MutateOperationMultiply,
			2,
			2,
		},
		{
			"multiply float",
			1.0,
			ovsdb.MutateOperationMultiply,
			2.0,
			2.0,
		},
		{
			"multiply float set",
			[]float64{1.0, 2.0, 3.0},
			ovsdb.MutateOperationMultiply,
			2.0,
			[]float64{2.0, 4.0, 6.0},
		},
		{
			"multiply int set",
			[]int{1, 2, 3},
			ovsdb.MutateOperationMultiply,
			2,
			[]int{2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateDivide(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"divide int",
			10,
			ovsdb.MutateOperationDivide,
			2,
			5,
		},
		{
			"divide float",
			1.0,
			ovsdb.MutateOperationDivide,
			2.0,
			0.5,
		},
		{
			"divide float set",
			[]float64{1.0, 2.0, 4.0},
			ovsdb.MutateOperationDivide,
			2.0,
			[]float64{0.5, 1.0, 2.0},
		},
		{
			"divide int set",
			[]int{10, 20, 30},
			ovsdb.MutateOperationDivide,
			5,
			[]int{2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateModulo(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"modulo int",
			3,
			ovsdb.MutateOperationModulo,
			2,
			1,
		},
		{
			"modulo int set",
			[]int{3, 5, 7},
			ovsdb.MutateOperationModulo,
			2,
			[]int{1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateInsert(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"insert single string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationInsert,
			"baz",
			[]string{"foo", "bar", "baz"},
		},
		{
			"insert multiple string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationInsert,
			[]string{"baz", "quux"},
			[]string{"foo", "bar", "baz", "quux"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMutateDelete(t *testing.T) {
	tests := []struct {
		name    string
		current interface{}
		mutator ovsdb.Mutator
		value   interface{}
		want    interface{}
	}{
		{
			"delete single string",
			[]string{"foo", "bar"},
			ovsdb.MutateOperationDelete,
			"bar",
			[]string{"foo"},
		},
		{
			"delete multiple string",
			[]string{"foo", "bar", "baz"},
			ovsdb.MutateOperationDelete,
			[]string{"bar", "baz"},
			[]string{"foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mutate(tt.current, tt.mutator, tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mutate() = %v, want %v", got, tt.want)
			}
		})
	}
}
