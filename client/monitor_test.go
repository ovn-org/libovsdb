package client

import (
	"testing"

	"github.com/ovn-org/libovsdb/model"
	"github.com/stretchr/testify/assert"

	. "github.com/ovn-org/libovsdb/test"
)

func TestWithTable(t *testing.T) {
	dbModel, err := FullDatabaseModel()
	assert.NoError(t, err)
	client, err := newOVSDBClient(dbModel)
	assert.NoError(t, err)
	m := newMonitor()
	opt := WithTable(&OpenvSwitch{})

	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
}

func populateClientModel(t *testing.T, client *ovsdbClient) {
	s, err := Schema()
	assert.NoError(t, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	assert.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	assert.Empty(t, errs)
	client.primaryDB().model = dbModel
	assert.NoError(t, err)
}

func TestWithTableAndFields(t *testing.T) {
	dbModel, err := FullDatabaseModel()
	assert.NoError(t, err)
	client, err := newOVSDBClient(dbModel)
	assert.NoError(t, err)
	populateClientModel(t, client)

	m := newMonitor()
	ovs := OpenvSwitch{}
	opt := WithTable(&ovs, &ovs.Bridges, &ovs.CurCfg)
	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
	assert.ElementsMatch(t, []string{"bridges", "cur_cfg"}, m.Tables[0].Fields)
}
