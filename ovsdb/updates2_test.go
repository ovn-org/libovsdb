package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRowUpdate2Merge(t *testing.T) {
	portsA, _ := NewOvsSet([]interface{}{"portA"})
	portsC, _ := NewOvsSet([]interface{}{"portC"})
	portsAC, _ := NewOvsSet([]interface{}{"portA", "portC"})

	mapPortsA, _ := NewOvsMap(map[interface{}]interface{}{"A": "portA"})
	mapPortsC, _ := NewOvsMap(map[interface{}]interface{}{"C": "portC"})
	mapPortsAC, _ := NewOvsMap(map[interface{}]interface{}{"A": "portA", "C": "portC"})

	tests := []struct {
		name     string
		initial  *RowUpdate2
		new      *RowUpdate2
		expected *RowUpdate2
	}{
		{
			"insert then modify",
			&RowUpdate2{Insert: &Row{"foo": "bar"}},
			&RowUpdate2{Modify: &Row{"foo": "baz"}},
			&RowUpdate2{Insert: &Row{"foo": "baz"}},
		},
		{
			"insert then delete",
			&RowUpdate2{Insert: &Row{"foo": "bar"}},
			&RowUpdate2{Delete: &Row{"foo": "bar"}},
			&RowUpdate2{Delete: &Row{"foo": "bar"}},
		},
		{
			"modify then delete",
			&RowUpdate2{Modify: &Row{"foo": "baz"}},
			&RowUpdate2{Delete: &Row{"foo": "baz"}},
			&RowUpdate2{Delete: &Row{"foo": "baz"}},
		},
		{
			"modify then modify",
			&RowUpdate2{Modify: &Row{"foo": "baz"}},
			&RowUpdate2{Modify: &Row{"bar": "quux"}},
			&RowUpdate2{Modify: &Row{"foo": "baz", "bar": "quux"}},
		},
		{
			"modify a set then modify a set again",
			&RowUpdate2{Modify: &Row{"ports": portsA}},
			&RowUpdate2{Modify: &Row{"ports": portsC}},
			&RowUpdate2{Modify: &Row{"ports": portsAC}},
		},
		{
			"modify a map then modify a map again",
			&RowUpdate2{Modify: &Row{"ports": mapPortsA}},
			&RowUpdate2{Modify: &Row{"ports": mapPortsC}},
			&RowUpdate2{Modify: &Row{"ports": mapPortsAC}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Merge(tt.new)
			assert.Equal(t, tt.expected, tt.initial)
		})
	}
}
