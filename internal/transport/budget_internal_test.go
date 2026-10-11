package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBudgetTransport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		limit   int64
		reads   []int // sizes of successive Read calls
		wantN   int   // bytes read in total before the first error
		wantErr bool
	}{
		{"within budget", 10, []int{4, 6}, 10, false},
		{"read truncated at budget", 4, []int{8}, 4, false},
		{"over budget", 4, []int{4, 1}, 4, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf := thrift.NewTMemoryBuffer()
			_, _ = buf.Write(make([]byte, 64))
			b := newBudgetTransport(buf, tt.limit)
			total := 0
			var err error
			for _, n := range tt.reads {
				var got int
				got, err = b.Read(make([]byte, n))
				total += got
				if err != nil {
					break
				}
			}
			assert.Equal(t, tt.wantN, total)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			var pe thrift.TProtocolException
			require.True(t, errors.As(err, &pe), "want a TProtocolException, got %v", err)
			assert.Equal(t, thrift.SIZE_LIMIT, pe.TypeId())
		})
	}
}

// TestBudgetTransport_FlushResets covers that each reply gets a fresh
// budget: the count restarts when the next request is flushed.
func TestBudgetTransport_FlushResets(t *testing.T) {
	t.Parallel()
	buf := thrift.NewTMemoryBuffer()
	_, _ = buf.Write(make([]byte, 64))
	b := newBudgetTransport(buf, 4)

	_, err := b.Read(make([]byte, 4))
	require.NoError(t, err)
	_, err = b.ReadByte()
	require.Error(t, err)
	assert.Equal(t, uint64(0), b.RemainingBytes())

	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, uint64(4), b.RemainingBytes())
	_, err = b.ReadByte()
	require.NoError(t, err)
}

func TestBudgetTransport_DefaultLimit(t *testing.T) {
	t.Parallel()
	b := newBudgetTransport(thrift.NewTMemoryBuffer(), 0)
	assert.Equal(t, int64(DefaultMaxResponseBytes), b.limit)
	assert.Equal(t, int32(DefaultMaxResponseBytes), maxMessageSize(0))
	assert.Equal(t, int32(1<<31-1), maxMessageSize(1<<40))
}
