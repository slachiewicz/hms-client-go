package transport

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/apache/thrift/lib/go/thrift"
)

// DefaultMaxResponseBytes is the default cap on the bytes one reply may
// occupy on the wire: 100 MiB, libthrift's DEFAULT_MAX_MESSAGE_SIZE and the
// Java HiveMetaStoreClient's thrift.max.message.size default.
const DefaultMaxResponseBytes = thrift.DEFAULT_MAX_MESSAGE_SIZE

// budgetTransport caps the bytes the protocol may read for one reply. Go
// Thrift checks MaxMessageSize per string and per container, not per
// message, and a TSocket does not know how many bytes remain, so without
// this a server can stream a reply of any length -- a list of a hundred
// million one-byte structs costs it 100 MB on the wire and the client
// gigabytes of heap. The count resets on Flush: a TStandardClient flushes
// each request before it reads that request's reply.
type budgetTransport struct {
	thrift.TTransport
	limit int64
	read  int64
}

// newBudgetTransport wraps t so that no single reply read through it may
// exceed limit bytes; limit <= 0 selects DefaultMaxResponseBytes.
func newBudgetTransport(t thrift.TTransport, limit int64) *budgetTransport {
	if limit <= 0 {
		limit = DefaultMaxResponseBytes
	}
	return &budgetTransport{TTransport: t, limit: limit}
}

// maxMessageSize returns the TConfiguration.MaxMessageSize for a protocol
// reading through a budget of limit bytes, so the size Thrift allows any
// one string or container never exceeds the whole reply's budget.
func maxMessageSize(limit int64) int32 {
	if limit <= 0 {
		limit = DefaultMaxResponseBytes
	}
	return int32(min(limit, math.MaxInt32))
}

func (b *budgetTransport) errTooLarge() error {
	return thrift.NewTProtocolExceptionWithType(thrift.SIZE_LIMIT,
		fmt.Errorf("hms: response exceeds %d bytes (see WithMaxResponseBytes)", b.limit))
}

// Read reads at most the bytes left in the current reply's budget.
func (b *budgetTransport) Read(p []byte) (int, error) {
	left := b.limit - b.read
	if left <= 0 {
		return 0, b.errTooLarge()
	}
	if int64(len(p)) > left {
		p = p[:left]
	}
	n, err := b.TTransport.Read(p)
	b.read += int64(n)
	return n, err
}

// budgetTransport is a TRichTransport, so TBinaryProtocol uses its
// ReadByte directly rather than wrapping it in a RichTransport whose
// per-byte reads would each allocate.
var _ thrift.TRichTransport = (*budgetTransport)(nil)

// ReadByte reads one byte against the budget, through the wrapped
// transport's own ReadByte when it has one.
func (b *budgetTransport) ReadByte() (byte, error) {
	if b.read >= b.limit {
		return 0, b.errTooLarge()
	}
	br, ok := b.TTransport.(io.ByteReader)
	if !ok {
		var v [1]byte
		_, err := io.ReadFull(b.TTransport, v[:])
		if err == nil {
			b.read++
		}
		return v[0], err
	}
	c, err := br.ReadByte()
	if err == nil {
		b.read++
	}
	return c, err
}

// WriteByte writes c through the wrapped transport's own WriteByte when it
// has one. Writes do not count against the budget.
func (b *budgetTransport) WriteByte(c byte) error {
	if bw, ok := b.TTransport.(io.ByteWriter); ok {
		return bw.WriteByte(c)
	}
	_, err := b.Write([]byte{c})
	return err
}

// WriteString writes s through the wrapped transport's own WriteString
// when it has one.
func (b *budgetTransport) WriteString(s string) (int, error) {
	return io.WriteString(b.TTransport, s)
}

// Flush sends the buffered request and starts a fresh budget for its reply.
func (b *budgetTransport) Flush(ctx context.Context) error {
	b.read = 0
	return b.TTransport.Flush(ctx)
}

// RemainingBytes reports the smaller of the budget left and what the
// wrapped transport knows remains, so Thrift rejects an oversized string
// or container from its declared length, before reading or allocating it.
func (b *budgetTransport) RemainingBytes() uint64 {
	left := uint64(max(b.limit-b.read, 0))
	return min(left, b.TTransport.RemainingBytes())
}

// SetTConfiguration passes cfg on to the wrapped transport.
func (b *budgetTransport) SetTConfiguration(cfg *thrift.TConfiguration) {
	thrift.PropagateTConfiguration(b.TTransport, cfg)
}
