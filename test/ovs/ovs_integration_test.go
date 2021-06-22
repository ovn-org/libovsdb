package ovs

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// OVSIntegrationSuite runs tests against a real Open vSwitch instance
type OVSIntegrationSuite struct {
	suite.Suite
	pool     *dockertest.Pool
	resource *dockertest.Resource
	client   client.Client
}

func (suite *OVSIntegrationSuite) SetupSuite() {
	var err error
	suite.pool, err = dockertest.NewPool("")
	require.NoError(suite.T(), err)

	tag := os.Getenv("OVS_IMAGE_TAG")
	if tag == "" {
		tag = "latest"
	}

	options := &dockertest.RunOptions{
		Repository:   "libovsdb/ovs",
		Tag:          tag,
		ExposedPorts: []string{"6640/tcp"},
		Tty:          true,
	}
	hostConfig := func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	}

	suite.resource, err = suite.pool.RunWithOptions(options, hostConfig)
	require.NoError(suite.T(), err)

	// set expiry to 30 seconds so containers are cleaned up on test panic
	err = suite.resource.Expire(30)
	require.NoError(suite.T(), err)

	// let the container start before we attempt connection
	time.Sleep(5 * time.Second)

	err = suite.pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		endpoint := fmt.Sprintf("tcp::%s", suite.resource.GetPort("6640/tcp"))
		ovs, err := client.NewOVSDBClient(defDB, client.WithEndpoint(endpoint))
		if err != nil {
			return err
		}
		err = ovs.Connect(ctx)
		if err != nil {
			return err
		}
		suite.client = ovs
		return nil
	})
	require.NoError(suite.T(), err)

	// give ovsdb-server some time to start up

	err = suite.client.MonitorAll(nil)
	require.NoError(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TearDownSuite() {
	if suite.client != nil {
		suite.client.Disconnect()
		suite.client = nil
	}
	err := suite.pool.Purge(suite.resource)
	require.NoError(suite.T(), err)
}

func TestOVSIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(OVSIntegrationSuite))
}

// bridgeType is the simplified ORM model of the Bridge table
type bridgeType struct {
	UUID        string            `ovsdb:"_uuid"`
	Name        string            `ovsdb:"name"`
	OtherConfig map[string]string `ovsdb:"other_config"`
	ExternalIds map[string]string `ovsdb:"external_ids"`
	Ports       []string          `ovsdb:"ports"`
	Status      map[string]string `ovsdb:"status"`
}

// ovsType is the simplified ORM model of the Bridge table
type ovsType struct {
	UUID    string   `ovsdb:"_uuid"`
	Bridges []string `ovsdb:"bridges"`
}

var defDB, _ = model.NewDBModel("Open_vSwitch", map[string]model.Model{
	"Open_vSwitch": &ovsType{},
	"Bridge":       &bridgeType{}})

func (suite *OVSIntegrationSuite) TestConnectReconnectIntegration() {
	assert.True(suite.T(), suite.client.Connected())
	err := suite.client.Echo()
	require.NoError(suite.T(), err)

	// make another connect call, this should return without error as we're already connected
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = suite.client.Connect(ctx)
	require.NoError(suite.T(), err)

	disconnectNotification := suite.client.DisconnectNotify()
	notified := make(chan struct{})
	ready := make(chan struct{})

	go func() {
		ready <- struct{}{}
		<-disconnectNotification
		notified <- struct{}{}
	}()

	<-ready
	suite.client.Disconnect()

	select {
	case <-notified:
		// got notification
	case <-time.After(5 * time.Second):
		suite.T().Fatal("expected a disconnect notification but didn't receive one")
	}

	assert.Equal(suite.T(), false, suite.client.Connected())

	err = suite.client.Echo()
	require.EqualError(suite.T(), err, client.ErrNotConnected.Error())

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = suite.client.Connect(ctx)
	require.NoError(suite.T(), err)

	err = suite.client.Echo()
	assert.NoError(suite.T(), err)

	err = suite.client.MonitorAll(nil)
	require.NoError(suite.T(), err)
}
func (suite *OVSIntegrationSuite) TestInsertTransactIntegration() {
	bridgeName := "gopher-br7"

	// NamedUUID is used to add multiple related Operations in a single Transact operation
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: bridgeName,
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := suite.client.Create(&br)
	require.NoError(suite.T(), err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := suite.client.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationInsert,
			Value:   []string{namedUUID},
		})
	require.NoError(suite.T(), err)

	operations := append(insertOp, mutateOp...)
	reply, err := suite.client.Transact(operations...)
	require.NoError(suite.T(), err)

	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			suite.T().Error(oe)
		}
		suite.T().Fatal(err)
	}
}

func (suite *OVSIntegrationSuite) TestInsertAndDeleteTransactIntegration() {
	bridgeName := "gopher-br5"
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: bridgeName,
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := suite.client.Create(&br)
	require.NoError(suite.T(), err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := suite.client.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationInsert,
			Value:   []string{namedUUID},
		})
	require.NoError(suite.T(), err)

	operations := append(insertOp, mutateOp...)
	reply, err := suite.client.Transact(operations...)
	require.NoError(suite.T(), err)

	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			suite.T().Error(oe)
		}
		suite.T().Fatal(err)
	}
	bridgeUUID := reply[0].UUID.GoUUID

	deleteOp, err := suite.client.Where(&bridgeType{Name: bridgeName}).Delete()
	require.NoError(suite.T(), err)

	delMutateOp, err := suite.client.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationDelete,
			Value:   []string{bridgeUUID},
		})

	require.NoError(suite.T(), err)

	delOperations := append(deleteOp, delMutateOp...)
	delReply, err := suite.client.Transact(delOperations...)
	require.NoError(suite.T(), err)

	delOperationErrs, err := ovsdb.CheckOperationResults(delReply, delOperations)
	if err != nil {
		for _, oe := range delOperationErrs {
			suite.T().Error(oe)
		}
		suite.T().Fatal(err)
	}
}

func (suite *OVSIntegrationSuite) TestTableSchemaValidationIntegration() {
	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   ovsdb.Row(map[string]interface{}{"name": "docker-ovs"}),
	}
	_, err := suite.client.Transact(operation)
	assert.Error(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TestColumnSchemaInRowValidationIntegration() {
	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   ovsdb.Row(map[string]interface{}{"name": "docker-ovs", "invalid_column": "invalid_column"}),
	}

	_, err := suite.client.Transact(operation)
	assert.Error(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TestColumnSchemaInMultipleRowsValidationIntegration() {
	invalidBridge := ovsdb.Row(map[string]interface{}{"invalid_column": "invalid_column"})
	bridge := ovsdb.Row(map[string]interface{}{"name": "docker-ovs"})
	rows := []ovsdb.Row{invalidBridge, bridge}

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	_, err := suite.client.Transact(operation)
	assert.Error(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TestColumnSchemaValidationIntegration() {
	operation := ovsdb.Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	_, err := suite.client.Transact(operation)
	assert.Error(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TestMonitorCancelIntegration() {
	monitorID := "f1b2ca48-aad7-11e7-abc4-cec278b6b50a"

	requests := make(map[string]ovsdb.MonitorRequest)
	requests["Bridge"] = ovsdb.MonitorRequest{
		Columns: []string{"name"},
		Select:  ovsdb.NewDefaultMonitorSelect(),
	}

	err := suite.client.Monitor(monitorID,
		suite.client.NewTableMonitor(&ovsType{}),
		suite.client.NewTableMonitor(&bridgeType{}),
	)
	require.NoError(suite.T(), err)

	err = suite.client.MonitorCancel(monitorID)
	assert.NoError(suite.T(), err)
}

func (suite *OVSIntegrationSuite) TestInsertDuplicateTransactIntegration() {
	// NamedUUID is used to add multiple related Operations in a single Transact operation
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: "br-dup",
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := suite.client.Create(&br)
	require.NoError(suite.T(), err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := suite.client.WhereCache(func(*ovsType) bool { return true }).
		Mutate(&ovsRow, model.Mutation{
			Field:   &ovsRow.Bridges,
			Mutator: ovsdb.MutateOperationInsert,
			Value:   []string{namedUUID},
		})
	require.NoError(suite.T(), err)

	operations := append(insertOp, mutateOp...)
	reply, err := suite.client.Transact(operations...)
	require.NoError(suite.T(), err)

	operationErrs, err := ovsdb.CheckOperationResults(reply, operations)
	if err != nil {
		for _, oe := range operationErrs {
			suite.T().Error(oe)
		}
		suite.T().Fatal(err)
	}

	reply2, err := suite.client.Transact(operations...)
	require.NoError(suite.T(), err)

	_, err = ovsdb.CheckOperationResults(reply2, operations)
	assert.Error(suite.T(), err)
	assert.IsType(suite.T(), &ovsdb.ConstraintViolation{}, err)
}
