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
