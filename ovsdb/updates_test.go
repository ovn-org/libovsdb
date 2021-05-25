package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddTableUpdate(t *testing.T) {
	tests := []struct {
		name     string
		initial  TableUpdates
		table    string
		update   TableUpdate
		expected TableUpdates
	}{
		{
			"new table",
			TableUpdates{},
			"foo",
			TableUpdate{},
			TableUpdates{"foo": TableUpdate{}},
		},
		{
			"existing table",
			TableUpdates{
				"foo": {"bar": {Old: nil, New: nil}},
			},
			"foo",
			TableUpdate{"baz": {Old: nil, New: nil}},
			TableUpdates{
				"foo": {
					"bar": {Old: nil, New: nil},
					"baz": {Old: nil, New: nil},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddTableUpdate(tt.table, tt.update)
			assert.Equal(t, tt.expected, tt.initial)
		})
	}
}

func TestAddRowUpdate(t *testing.T) {
	tests := []struct {
		name     string
		initial  TableUpdate
		uuid     string
		update   *RowUpdate
		expected TableUpdate
	}{
		{
			"new row",
			TableUpdate{},
			"foo",
			&RowUpdate{},
			TableUpdate{"foo": {}},
		},
		{
			"update existing row",
			TableUpdate{
				"foo": {
					Old: nil,
					New: &Row{Fields: map[string]interface{}{"foo": "bar"}},
				},
			},
			"foo",
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
				New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
			},
			TableUpdate{
				"foo": {
					Old: nil,
					New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddRowUpdate(tt.uuid, tt.update)
			assert.Equal(t, tt.expected, tt.initial)
		})
	}
}

func TestAddRowUpdateMerge(t *testing.T) {
	tests := []struct {
		name     string
		initial  *RowUpdate
		new      *RowUpdate
		expected *RowUpdate
	}{
		{
			"insert then modify",
			&RowUpdate{
				Old: nil,
				New: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
				New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
			},
			&RowUpdate{
				Old: nil,
				New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
			},
		},
		{
			"insert then delete",
			&RowUpdate{
				New: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
				New: nil,
			},
		},
		{
			"modify then delete",
			&RowUpdate{
				New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "baz"}},
			},
			&RowUpdate{
				Old: &Row{Fields: map[string]interface{}{"foo": "baz"}},
				New: nil,
			},
		},
		{
			"modify then modify",
			&RowUpdate{
				New: &Row{Fields: map[string]interface{}{"foo": "baz"}},
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
			&RowUpdate{
				New: &Row{Fields: map[string]interface{}{"foo": "quux"}},
				Old: &Row{Fields: map[string]interface{}{"foo": "baz"}},
			},
			&RowUpdate{
				New: &Row{Fields: map[string]interface{}{"foo": "quux"}},
				Old: &Row{Fields: map[string]interface{}{"foo": "bar"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Merge(tt.new)
			assert.Equal(t, tt.expected, tt.initial)
		})
	}
}

func TestRowUpdateInsert(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.True(t, u1.Insert())
	assert.False(t, u2.Insert())
	assert.False(t, u3.Insert())
}

func TestRowUpdateModify(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.False(t, u1.Modify())
	assert.True(t, u2.Modify())
	assert.False(t, u3.Modify())
}

func TestRowUpdateDelete(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.False(t, u1.Delete())
	assert.False(t, u2.Delete())
	assert.True(t, u3.Delete())
}
