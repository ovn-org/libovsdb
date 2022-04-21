package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithTable(t *testing.T) {
	client, err := newOVSDBClient(defDB)
	assert.NoError(t, err)
	m := newMonitor()
	opt := WithTable(&OpenvSwitch{})

	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
}

func TestWithTableAndFields(t *testing.T) {
	client, err := newOVSDBClient(defDB)
	assert.NoError(t, err)
	m := newMonitor()
	ovs := OpenvSwitch{}
	opt := WithTable(&ovs, &ovs.Bridges, &ovs.CurCfg)

	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
	assert.Equal(t, &ovs, m.Tables[0].Model)
	assert.Equal(t, 2, len(m.Tables[0].Fields))
}
