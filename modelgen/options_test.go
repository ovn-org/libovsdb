package modelgen

import (
	"reflect"
	"testing"
)

func TestWithDryRun(t *testing.T) {
	tests := []struct {
		name   string
		call   bool
		dryRun bool
	}{
		{
			"call",
			true,
			true,
		},
		{
			"not call",
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &options{}
			if tt.call {
				fn := WithDryRun()
				_ = fn(opts)
			}
			if got := opts.dryRun; !reflect.DeepEqual(got, tt.dryRun) {
				t.Errorf("WithDryRun() = %v, want %v", got, tt.dryRun)
			}
		})
	}
}

func Test_newOptions(t *testing.T) {
	type args struct {
		opts []Option
	}
	tests := []struct {
		name    string
		args    args
		want    *options
		wantErr bool
	}{
		{
			"With DryRun",
			args{opts: []Option{WithDryRun()}},
			&options{dryRun: true},
			false,
		},
		{
			"Without DryRun",
			args{opts: []Option{}},
			&options{dryRun: false},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newOptions(tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("newOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newOptions() got = %v, want %v", got, tt.want)
			}
		})
	}
}
