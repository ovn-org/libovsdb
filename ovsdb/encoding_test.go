package ovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var validUUIDStr0 = `00000000-0000-0000-0000-000000000000`
var validUUIDStr1 = `11111111-1111-1111-1111-111111111111`
var validUUID0 = UUID{GoUUID: validUUIDStr0}
var validUUID1 = UUID{GoUUID: validUUIDStr1}

func TestMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected string
	}{
		{
			"empty map",
			map[string]string{},
			`["map",[]]`,
		},
		{
			"single element map",
			map[string]string{`v0`: `k0`},
			`["map",[["v0","k0"]]]`,
		},
		{
			"multiple element map",
			map[string]string{`v0`: `k0`, `v1`: `k1`},
			`["map",[["v0","k0"],["v1","k1"]]]`,
		},
	}
	for _, tt := range tests {
		m, err := NewOvsMap(tt.input)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(m)
		assert.Nil(t, err)
		// Compare unmarshalled data since the order of the elements of the map might not
		// have been preserved
		var expectedSlice []interface{}
		var jsonSlice []interface{}
		err = json.Unmarshal([]byte(tt.expected), &expectedSlice)
		assert.Nil(t, err)
		err = json.Unmarshal(jsonStr, &jsonSlice)
		assert.Nil(t, err)
		assert.Equal(t, expectedSlice[0], jsonSlice[0], "they should both start with 'map'")
		assert.ElementsMatch(t, expectedSlice[1].([]interface{}), jsonSlice[1].([]interface{}), "they should have the same elements\n")

		var res OvsMap
		err = json.Unmarshal(jsonStr, &res)
		assert.Nil(t, err)
		assert.Equal(t, m, res, "they should be equal\n")
	}
}

func TestSet(t *testing.T) {
	var x *int
	var y *string
	tests := []struct {
		name     string
		setType  string
		input    interface{}
		expected string
	}{
		{
			"empty set",
			TypeString,
			[]string{},
			`["set",[]]`,
		},
		{
			"string",
			TypeString,
			`aa`,
			`"aa"`,
		},
		{
			"bool",
			TypeBoolean,
			false,
			`false`,
		},
		{
			"float 64",
			TypeReal,
			float64(10),
			`10`,
		},
		{
			"float",
			TypeReal,
			10.2,
			`10.2`,
		},
		{
			"string slice",
			TypeString,
			[]string{`aa`},
			`"aa"`,
		},
		{
			"string slice with multiple elements",
			TypeString,
			[]string{`aa`, `bb`},
			`["set",["aa","bb"]]`,
		},
		{
			"float slice",
			TypeString,
			[]float64{10.2, 15.4},
			`["set",[10.2,15.4]]`,
		},
		{
			"empty uuid",
			TypeUUID,
			[]UUID{},
			`["set",[]]`,
		},
		{
			"uuid",
			TypeUUID,
			UUID{GoUUID: `aa`},
			`["named-uuid","aa"]`,
		},
		{
			"uuid slice single element",
			TypeUUID,
			[]UUID{{GoUUID: `aa`}},
			`["named-uuid","aa"]`,
		},
		{
			"uuid slice multiple elements",
			TypeUUID,
			[]UUID{{GoUUID: `aa`}, {GoUUID: `bb`}},
			`["set",[["named-uuid","aa"],["named-uuid","bb"]]]`,
		},
		{
			"valid uuid",
			TypeUUID,
			validUUID0,
			fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
		},
		{
			"valid uuid set single element",
			TypeUUID,
			[]UUID{validUUID0},
			fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
		},
		{
			"valid uuid set multiple elements",
			TypeUUID,
			[]UUID{validUUID0, validUUID1},
			fmt.Sprintf(`["set",[["uuid","%v"],["uuid","%v"]]]`, validUUIDStr0, validUUIDStr1),
		},
		{
			name:     "nil pointer of valid *int type",
			setType:  TypeInteger,
			input:    x,
			expected: `["set",[]]`,
		},
		{
			name:     "nil pointer of valid *string type",
			setType:  TypeString,
			input:    y,
			expected: `["set",[]]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set, err := NewOvsSet(tt.setType, tt.input)
			assert.Nil(t, err)
			jsonStr, err := json.Marshal(set)
			assert.Nil(t, err)
			assert.JSONEqf(t, tt.expected, string(jsonStr), "they should be equal\n")

			var res OvsSet
			err = json.Unmarshal(jsonStr, &res)
			assert.Nil(t, err)
			assert.Equal(t, set.GoSet, res.GoSet, "they should have the same elements\n")
		})
	}
}
