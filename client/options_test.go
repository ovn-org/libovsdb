package client

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithTLSConfig(t *testing.T) {
	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	opts := &options{}
	fn := WithTLSConfig(config)
	err := fn(opts)
	require.Nil(t, err)
	assert.Equal(t, config, opts.tlsConfig)
}

func TestNewOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want *options
	}{
		{
			"no endpoints",
			[]Option{},
			&options{
				endpoints: []string{defaultUnixEndpoint},
			},
		},
		{
			"single endpoints",
			[]Option{WithEndpoint("ssl:192.168.1.1:6443")},
			&options{
				endpoints: []string{"ssl:192.168.1.1:6443"},
			},
		},
		{
			"multiple endpoints",
			[]Option{WithEndpoint("ssl:192.168.1.1:6443"), WithEndpoint("ssl:192.168.1.2:6443")},
			&options{
				endpoints: []string{"ssl:192.168.1.1:6443", "ssl:192.168.1.2:6443"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newOptions(tt.opts...)
			require.Nil(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWithEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     []string
		wantErr  bool
	}{
		{
			"default unix",
			"unix:",
			[]string{defaultUnixEndpoint},
			false,
		},
		{
			"default tcp",
			"tcp:",
			[]string{defaultTCPEndpoint},
			false,
		},
		{
			"default ssl",
			"ssl:",
			[]string{defaultSSLEndpoint},
			false,
		},
		{
			"invalid",
			"foo : ",
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &options{}
			fn := WithEndpoint(tt.endpoint)
			err := fn(opts)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.Nil(t, err)
			}
			assert.Equal(t, tt.want, opts.endpoints)
		})
	}
}