package testhelpers

import (
	"github.com/stretchr/testify/assert"

	"github.com/ovn-org/libovsdb/ovsdb"
)

func MakeOvsSet(t assert.TestingT, set interface{}) ovsdb.OvsSet {
	oSet, err := ovsdb.NewOvsSet(set)
	assert.Nil(t, err)
	return oSet
}

func MakeOvsMap(t assert.TestingT, m interface{}) ovsdb.OvsMap {
	oMap, err := ovsdb.NewOvsMap(m)
	assert.Nil(t, err)
	return oMap
}
