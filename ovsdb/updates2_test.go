package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRowUpdate2Merge(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Merge(tt.new)
			assert.Equal(t, tt.expected, tt.initial)
		})
	}
}
