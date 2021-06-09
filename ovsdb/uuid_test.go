package ovsdb

import "testing"

func TestUUIDIsNamed(t *testing.T) {
	tests := []struct {
		name string
		uuid string
		want bool
	}{
		{
			"named",
			"foo",
			true,
		},
		{
			"named",
			aUUID0,
			false,
		},
		{
			"empty",
			"",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNamed(tt.uuid); got != tt.want {
				t.Errorf("UUID.Named() = %v, want %v", got, tt.want)
			}
		})
	}
}
