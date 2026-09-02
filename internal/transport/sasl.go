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

// saslMech is one SASL client mechanism, driven by saslTransport's
// negotiation loop. PLAIN (plainMech) and GSSAPI (gssapiMech) differ only
// in the tokens they exchange; the framing, the status bytes, and the
// post-handshake data frames are common to both.
type saslMech interface {
	// Name is the mechanism name sent in the START frame.
	Name() string
	// Step advances the mechanism by one round. The first call passes a nil
	// challenge and returns the mechanism's initial response; every later
	// call passes the payload of a server OK frame. done reports that the
	// returned response is the mechanism's last token, which the driver
	// sends with status COMPLETE rather than OK. A mechanism that has no
	// further token to send but still expects the server's COMPLETE (SASL
	// PLAIN, whose one-shot initial response the server answers directly)
	// reports done false and never has Step called again.
	Step(ctx context.Context, challenge []byte) (response []byte, done bool, err error)
	// Complete reports that the mechanism has finished authenticating and
	// that a COMPLETE from the server may therefore be believed. It is not
	// the same as Step's done: SASL PLAIN is complete once it has sent its
	// initial response even though that response goes out with status OK.
	// The driver rejects a COMPLETE arriving before this holds, so a peer
	// cannot cut a multi-round mechanism short -- answering GSSAPI's AP_REQ
	// with COMPLETE would otherwise skip mutual authentication and still
	// hand back a usable transport.
	Complete() bool
}

// saslTransport wraps a thrift.TTransport with SASL framing: Open()
// performs the handshake for its mechanism, and once complete every
// Read/Write is carried in length-prefixed data frames (see NewSaslPlain
// and NewSaslGSSAPI).
type saslTransport struct {
	inner thrift.TTransport
	mech  saslMech
	wbuf  bytes.Buffer
	rbuf  bytes.Reader
	open  bool
}

// SaslTransport is a thrift.TTransport that also exposes the SASL
// handshake so a caller that already has a context in hand (see
// DialBinary) can bound it, instead of Open()'s unconditional
// context.Background().
type SaslTransport interface {
	thrift.TTransport
	// OpenContext performs the same handshake as Open, but threads ctx
	// through to every Flush the handshake issues, so a deadline or
	// cancellation on ctx interrupts the handshake's writes.
	OpenContext(ctx context.Context) error
}

// plainMech is the SASL PLAIN (RFC 4616) client mechanism: a single
// initial response carrying the credentials, which the server answers with
// COMPLETE or BAD.
type plainMech struct {
	user     string
	password string
	sent     bool
}

// Name returns the SASL mechanism name sent in the START frame.
func (*plainMech) Name() string { return "PLAIN" }

// Step returns the RFC 4616 initial response on its first call. PLAIN has
// no further rounds, so a challenge from the server is a protocol error.
func (m *plainMech) Step(_ context.Context, challenge []byte) ([]byte, bool, error) {
	if challenge != nil {
		return nil, false, errors.New("hms: sasl plain: unexpected challenge from the server")
	}
	m.sent = true
	return []byte("\x00" + m.user + "\x00" + m.password), false, nil
}

// Complete reports whether the initial response has been sent. PLAIN has
// exactly one round, so the server may answer it with COMPLETE; before it,
// a COMPLETE is a protocol violation.
func (m *plainMech) Complete() bool { return m.sent }

// NewSaslPlain wraps inner in a SaslTransport that authenticates with SASL
// PLAIN (RFC 4616) on Open, using the Java TSaslTransport wire format: every
// negotiation message is a 1 byte status, a 4 byte big-endian length, and a
// payload; after the handshake completes, every data frame is a 4 byte
// big-endian length and a payload with no status byte.
func NewSaslPlain(inner thrift.TTransport, user, password string) SaslTransport {
	return &saslTransport{inner: inner, mech: &plainMech{user: user, password: password}}
}

// Open performs the SASL handshake with context.Background(), satisfying
// thrift.TTransport. Callers that hold a context should call OpenContext
// instead.
func (s *saslTransport) Open() error {
	return s.OpenContext(context.Background())
}

// OpenContext opens the inner transport if needed, then runs the SASL
// handshake for s's mechanism, mirroring Java's TSaslClientTransport: a
// START frame naming the mechanism, a frame carrying the mechanism's
// initial response, then a round of challenge/response frames until the
// server answers COMPLETE. Each response goes out with status COMPLETE
// when it is the mechanism's last token and OK otherwise, exactly as
// TSaslTransport.open does.
//
// It returns an error describing the server's rejection message on BAD or
// ERROR, and propagates any inner Open or I/O failure (including ctx
// cancellation, once the caller has arranged for that to unblock the
// underlying I/O) unwrapped.
func (s *saslTransport) OpenContext(ctx context.Context) error {
	if !s.inner.IsOpen() {
		if err := s.inner.Open(); err != nil {
			return err
		}
	}
	name := s.mech.Name()
	if err := s.sendNegotiate(ctx, saslStart, []byte(name)); err != nil {
		return err
	}
	resp, done, err := s.mech.Step(ctx, nil)
	if err != nil {
		return err
	}
	if err := s.sendNegotiate(ctx, statusFor(done), resp); err != nil {
		return err
	}
	for {
		status, payload, err := s.recvNegotiate()
		if err != nil {
			return err
		}
		switch status {
		case saslComplete:
			if !s.mech.Complete() {
				return fmt.Errorf("hms: sasl %s: server completed before the mechanism finished", name)
			}
			s.open = true
			return nil
		case saslBad, saslError:
			return fmt.Errorf("hms: sasl %s rejected: %s", name, payload)
		case saslOK:
			if done {
				return fmt.Errorf("hms: sasl %s: server sent a challenge after the mechanism completed", name)
			}
			resp, done, err = s.mech.Step(ctx, payload)
			if err != nil {
				return err
			}
			if err := s.sendNegotiate(ctx, statusFor(done), resp); err != nil {
				return err
			}
		default:
			return fmt.Errorf("hms: unexpected sasl status %d", status)
		}
	}
}

// statusFor maps a mechanism's done flag to the negotiation status its
// response is sent with.
func statusFor(done bool) byte {
	if done {
		return saslComplete
	}
	return saslOK
}

// sendNegotiate writes one negotiation frame (status, length, payload) to
// the inner transport and flushes it with ctx.
func (s *saslTransport) sendNegotiate(ctx context.Context, status byte, payload []byte) error {
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
	return s.inner.Flush(ctx)
}

// recvNegotiate reads one negotiation frame (status, length, payload) from
// the inner transport.
func (s *saslTransport) recvNegotiate() (byte, []byte, error) {
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
func (s *saslTransport) IsOpen() bool { return s.open && s.inner.IsOpen() }

// Close closes the inner transport.
func (s *saslTransport) Close() error {
	s.open = false
	return s.inner.Close()
}

// Read fills p from the current data frame, reading a new frame from the
// inner transport when the current one is exhausted. Zero-length frames are
// skipped without being reported as EOF.
func (s *saslTransport) Read(p []byte) (int, error) {
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
func (s *saslTransport) Write(p []byte) (int, error) { return s.wbuf.Write(p) }

// Flush sends the buffered write as one length-prefixed data frame and
// flushes the inner transport. Empty frames are never emitted; if the write
// buffer is empty, Flush just calls inner.Flush.
func (s *saslTransport) Flush(ctx context.Context) error {
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
func (s *saslTransport) RemainingBytes() uint64 {
	n := s.rbuf.Len()
	if n < 0 {
		return 0
	}
	return uint64(n)
}
