package server

import (
	"testing"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
)

func TestMonitorFilter(t *testing.T) {
	monitor := monitor{
		request: map[string]*ovsdb.MonitorRequest{
			"Bridge": {
				Columns: []string{"name"},
				Select:  ovsdb.NewDefaultMonitorSelect(),
			},
		},
	}
	bridgeRow := ovsdb.Row{
		"_uuid": "foo",
		"name":  "bar",
	}
	bridgeRowWithIDs := ovsdb.Row{
		"_uuid":        "foo",
		"name":         "bar",
		"external_ids": map[string]string{"foo": "bar"},
	}
	tests := []struct {
		name     string
		update   ovsdb.TableUpdates
		expected ovsdb.TableUpdates
	}{
		{
			"not filtered",
			ovsdb.TableUpdates{
				"Bridge": ovsdb.TableUpdate{
					"foo": &ovsdb.RowUpdate{
						Old: nil, New: &bridgeRow,
					},
				},
			},
			ovsdb.TableUpdates{
				"Bridge": ovsdb.TableUpdate{
					"foo": &ovsdb.RowUpdate{
						Old: nil, New: &bridgeRow,
					},
				},
			},
		},
		{
			"removed table",
			ovsdb.TableUpdates{
				"Open_vSwitch": ovsdb.TableUpdate{
					"foo": &ovsdb.RowUpdate{
						Old: nil, New: &bridgeRow,
					},
				},
			},
			ovsdb.TableUpdates{},
		},
		{
			"removed column",
			ovsdb.TableUpdates{
				"Bridge": ovsdb.TableUpdate{
					"foo": &ovsdb.RowUpdate{
						Old: nil, New: &bridgeRowWithIDs,
					},
				},
			},
			ovsdb.TableUpdates{
				"Bridge": ovsdb.TableUpdate{
					"foo": &ovsdb.RowUpdate{
						Old: nil, New: &bridgeRow,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor.filter(tt.update)
			assert.Equal(t, tt.expected, tt.update)
		})
	}
}
