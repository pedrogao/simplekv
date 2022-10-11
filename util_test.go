package simplekv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_sizeof(t *testing.T) {
	type args struct {
		v any
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "pedro",
			args: args{
				v: "pedro",
			},
			want: 5,
		},
		{
			name: "s1",
			args: args{
				v: "1",
			},
			want: 1,
		},
		{
			name: "s12",
			args: args{
				v: "12",
			},
			want: 2,
		},
		{
			name: "1",
			args: args{
				v: uint64(1),
			},
			want: 1,
		},
		{
			name: "12",
			args: args{
				v: uint64(12),
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, sizeof(tt.args.v), "sizeof(%v)", tt.args.v)
		})
	}
}
