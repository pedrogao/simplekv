package simplekv

import (
	"testing"
)

func TestMurmur332(t *testing.T) {
	type args struct {
		key  []byte
		seed uint32
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "1",
			args: args{
				key:  []byte("pedro"),
				seed: 1,
			},
			want: 3591849244,
		},
		{
			name: "2",
			args: args{
				key:  []byte("pedro"),
				seed: 2,
			},
			want: 4262892875,
		},
		{
			name: "3",
			args: args{
				key:  []byte("pedro"),
				seed: 3,
			},
			want: 1556225608,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Murmur332(tt.args.key, tt.args.seed); got != tt.want {
				t.Errorf("Murmur332() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkMurmur332(b *testing.B) {
	// BenchmarkMurmur332-12    	193289504	         6.416 ns/op
	for i := 0; i < b.N; i++ {
		Murmur332([]byte("pedro"), 3)
	}
}

func BenchmarkMurmur333(b *testing.B) {
	// BenchmarkMurmur333-12    	228767322	         5.148 ns/op
	for i := 0; i < b.N; i++ {
		Murmur332([]byte("pedro"), 10)
	}
}
