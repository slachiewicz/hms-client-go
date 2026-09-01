package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

// SASL negotiation status codes (Java TSaslTransport wire format).
const (
	saslStart    byte = 1
	saslOK       byte = 2
	saslBad      byte = 3
	saslError    byte = 4
	saslComplete byte = 5
)

// saslMaxFrame is the maximum negotiation or data frame size accepted from
// the peer.
const saslMaxFrame = 64 << 20

// saslPlain wraps a thrift.TTransport with SASL PLAIN framing: Open()
// performs the SASL PLAIN handshake, and once complete every Read/Write is
// carried in length-prefixed data frames (see NewSaslPlain).
type saslPlain struct {
	inner    thrift.TTransport
	user     string
	password string
	wbuf     bytes.Buffer
	rbuf     bytes.Reader
	open     bool
}

// NewSaslPlain wraps inner in a thrift.TTransport that authenticates with
// SASL PLAIN (RFC 4616) on Open, using the Java TSaslTransport wire format:
// every negotiation message is a 1 byte status, a 4 byte big-endian length,
// and a payload; after the handshake completes, every data frame is a 4
// byte big-endian length and a payload with no status byte.
func NewSaslPlain(inner thrift.TTransport, user, password string) thrift.TTransport {
	return &saslPlain{inner: inner, user: user, password: password}
}

// Open opens the inner transport if needed, then performs the SASL PLAIN
// handshake: START with mechanism "PLAIN", then OK with the RFC 4616
// initial response. It returns an error describing the server's rejection
// message on BAD or ERROR, and propagates any inner Open or I/O failure
// unwrapped.
func (s *saslPlain) Open() error {
	if !s.inner.IsOpen() {
		if err := s.inner.Open(); err != nil {
			return err
		}
	}
	if err := s.sendNegotiate(saslStart, []byte("PLAIN")); err != nil {
		return err
	}
	initial := []byte("\x00" + s.user + "\x00" + s.password)
	if err := s.sendNegotiate(saslOK, initial); err != nil {
		return err
	}
	status, payload, err := s.recvNegotiate()
	if err != nil {
		return err
	}
	switch status {
	case saslComplete:
		s.open = true
		return nil
	case saslBad, saslError:
		return fmt.Errorf("hms: sasl plain rejected: %s", payload)
	default:
		return fmt.Errorf("hms: unexpected sasl status %d", status)
	}
}

// sendNegotiate writes one negotiation frame (status, length, payload) to
// the inner transport and flushes it.
func (s *saslPlain) sendNegotiate(status byte, payload []byte) error {
	n := len(payload)
	// gosec G115: len() never returns < 0, but the check is defensive.
	if n < 0 || n > saslMaxFrame {
		return errors.New("hms: sasl frame too large")
	}
	hdr := [5]byte{status}
	binary.BigEndian.PutUint32(hdr[1:], uint32(n))
	if _, err := s.inner.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.inner.Write(payload); err != nil {
		return err
	}
	return s.inner.Flush(context.Background())
}

// recvNegotiate reads one negotiation frame (status, length, payload) from
// the inner transport.
func (s *saslPlain) recvNegotiate() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(s.inner, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > saslMaxFrame {
		return 0, nil, errors.New("hms: sasl frame too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(s.inner, payload); err != nil {
		return 0, nil, err
	}
	return hdr[0], payload, nil
}

// IsOpen reports whether the SASL handshake completed and the inner
// transport is still open.
func (s *saslPlain) IsOpen() bool { return s.open && s.inner.IsOpen() }

// Close closes the inner transport.
func (s *saslPlain) Close() error {
	s.open = false
	return s.inner.Close()
}

// Read fills p from the current data frame, reading a new frame from the
// inner transport when the current one is exhausted. Zero-length frames are
// skipped without being reported as EOF.
func (s *saslPlain) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for s.rbuf.Len() == 0 {
		var hdr [4]byte
		if _, err := io.ReadFull(s.inner, hdr[:]); err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n > saslMaxFrame {
			return 0, errors.New("hms: sasl frame too large")
		}
		frame := make([]byte, n)
		if _, err := io.ReadFull(s.inner, frame); err != nil {
			return 0, err
		}
		s.rbuf.Reset(frame)
		// Continue looping if this is a zero-length frame to read the next one.
		if n > 0 {
			break
		}
	}
	return s.rbuf.Read(p)
}

// Write buffers p; it is sent as a single data frame on the next Flush.
func (s *saslPlain) Write(p []byte) (int, error) { return s.wbuf.Write(p) }

// Flush sends the buffered write as one length-prefixed data frame and
// flushes the inner transport. Empty frames are never emitted; if the write
// buffer is empty, Flush just calls inner.Flush.
func (s *saslPlain) Flush(ctx context.Context) error {
	n := s.wbuf.Len()
	// gosec G115: Len() never returns < 0, but the check is defensive.
	if n < 0 || n > saslMaxFrame {
		return errors.New("hms: sasl frame too large")
	}
	if n == 0 {
		// Empty frames are never emitted.
		return s.inner.Flush(ctx)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(n))
	if _, err := s.inner.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.inner.Write(s.wbuf.Bytes()); err != nil {
		return err
	}
	s.wbuf.Reset()
	return s.inner.Flush(ctx)
}

// RemainingBytes reports the number of bytes left unread in the current
// data frame.
func (s *saslPlain) RemainingBytes() uint64 {
	n := s.rbuf.Len()
	if n < 0 {
		return 0
	}
	return uint64(n)
}
