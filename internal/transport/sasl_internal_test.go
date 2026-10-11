package transport

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      []byte
		n       uint32
		want    []byte
		wantErr error
	}{
		{name: "empty frame", in: nil, n: 0, want: []byte{}},
		{name: "exact", in: []byte("abc"), n: 3, want: []byte("abc")},
		{name: "leaves the rest unread", in: []byte("abcdef"), n: 3, want: []byte("abc")},
		// A header announcing the full frame limit followed by a short
		// stream: an error, and no up-front allocation of the announced size.
		{name: "announced size never arrives", in: []byte("abc"), n: saslMaxFrame, wantErr: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := readFrame(bytes.NewReader(tt.in), tt.n)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestReadFrame_GrowsWithData proves memory follows the bytes received, not
// the length a server announces.
func TestReadFrame_GrowsWithData(t *testing.T) {
	allocs := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = readFrame(bytes.NewReader([]byte("abc")), saslMaxFrame)
		}
	})
	assert.Less(t, allocs.AllocedBytesPerOp(), int64(1<<20))
}
