package libovsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type modelA struct {
	UUID string `ovs:"_uuid"`
}

type modelB struct {
	UID string `ovs:"_uuid"`
	Foo string `ovs:"bar"`
	Bar string `ovs:"baz"`
}

type modelInvalid struct {
	Foo string
}

func TestDBModel(t *testing.T) {
	type Test struct {
		name  string
		obj   map[string]Model
		valid bool
	}

	tests := []Test{
		{
			name:  "valid",
			obj:   map[string]Model{"Test_A": &modelA{}},
			valid: true,
		},
		{
			name: "valid_multiple",
			obj: map[string]Model{"Test_A": &modelA{},
				"Test_B": &modelB{}},
			valid: true,
		},
		{
			name:  "invalid",
			obj:   map[string]Model{"INVALID": &modelInvalid{}},
			valid: false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("TestNewModel_%s", tt.name), func(t *testing.T) {
			db, err := NewDBModel(tt.name, tt.obj)
			if tt.valid {
				assert.Nil(t, err)
				assert.Len(t, db.Types(), len(tt.obj))
				assert.Equal(t, tt.name, db.Name())
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestNewModel(t *testing.T) {
	db, err := NewDBModel("testTable", map[string]Model{"Test_A": &modelA{}, "Test_B": &modelB{}})
	assert.Nil(t, err)
	_, err = db.newModel("Unknown")
	assert.NotNilf(t, err, "Creating model from unknown table should fail")
	model, err := db.newModel("Test_A")
	assert.Nilf(t, err, "Creating model from valid table should succeed")
	assert.IsTypef(t, model, &modelA{}, "model creation should return the apropriate type")
}

func TestSetUUID(t *testing.T) {
	var err error
	a := modelA{}
	err = modelSetUUID(&a, "foo")
	assert.Nilf(t, err, "Setting UUID should succeed")
	assert.Equal(t, "foo", a.UUID)
	b := modelB{}
	err = modelSetUUID(&b, "foo")
	assert.Nilf(t, err, "Setting UUID should succeed")
	assert.Equal(t, "foo", b.UID)

}
