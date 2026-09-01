package transport_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

// testMaxFrame bounds the frame sizes the fake-server helpers below will
// encode, mirroring saslPlain's own saslMaxFrame guard.
const testMaxFrame = 64 << 20

// readNegotiate reads one SASL negotiation frame (1 byte status, 4 byte
// big-endian length, payload) from r.
func readNegotiate(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return hdr[0], payload, nil
}

// writeNegotiate writes one SASL negotiation frame to w.
func writeNegotiate(w io.Writer, status byte, payload []byte) error {
	n := len(payload)
	if n < 0 || n > testMaxFrame {
		return errors.New("payload length out of range")
	}
	hdr := make([]byte, 5+n)
	hdr[0] = status
	binary.BigEndian.PutUint32(hdr[1:5], uint32(n))
	copy(hdr[5:], payload)
	_, err := w.Write(hdr)
	return err
}

// readDataFrame reads one post-handshake data frame (4 byte big-endian
// length, payload; no status byte) from r.
func readDataFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// writeDataFrame writes one post-handshake data frame to w.
func writeDataFrame(w io.Writer, payload []byte) error {
	n := len(payload)
	if n < 0 || n > testMaxFrame {
		return errors.New("payload length out of range")
	}
	hdr := make([]byte, 4+n)
	binary.BigEndian.PutUint32(hdr[:4], uint32(n))
	copy(hdr[4:], payload)
	_, err := w.Write(hdr)
	return err
}

func TestSaslPlain_HandshakeAndDataRoundTrip(t *testing.T) {
	t.Parallel()
	clientPipeEnd, serverPipeEnd := net.Pipe()
	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		status, payload, err := readNegotiate(serverPipeEnd)
		if err != nil {
			errs <- err
			return
		}
		if status != 1 {
			errs <- errors.New("expected START status")
			return
		}
		if string(payload) != "PLAIN" {
			errs <- errors.New("expected PLAIN mechanism, got " + string(payload))
			return
		}

		status, payload, err = readNegotiate(serverPipeEnd)
		if err != nil {
			errs <- err
			return
		}
		if status != 2 {
			errs <- errors.New("expected OK status")
			return
		}
		want := "\x00alice\x00s3cret"
		if string(payload) != want {
			errs <- errors.New("expected initial response " + want + ", got " + string(payload))
			return
		}

		if err := writeNegotiate(serverPipeEnd, 5, nil); err != nil {
			errs <- err
			return
		}

		frame, err := readDataFrame(serverPipeEnd)
		if err != nil {
			errs <- err
			return
		}
		if string(frame) != "hello" {
			errs <- errors.New("expected data frame hello, got " + string(frame))
			return
		}

		if err := writeDataFrame(serverPipeEnd, []byte("world")); err != nil {
			errs <- err
			return
		}
	}()

	inner := thrift.NewTSocketFromConnConf(clientPipeEnd, &thrift.TConfiguration{})
	sasl := transport.NewSaslPlain(inner, "alice", "s3cret")

	require.NoError(t, sasl.Open())
	defer func() { _ = sasl.Close() }()

	n, err := sasl.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	require.NoError(t, sasl.Flush(context.Background()))

	buf := make([]byte, 5)
	_, err = io.ReadFull(sasl, buf)
	require.NoError(t, err)
	assert.Equal(t, "world", string(buf))

	require.NoError(t, <-errs)
}

func TestSaslPlain_OpenRejectedByServer(t *testing.T) {
	t.Parallel()
	clientPipeEnd, serverPipeEnd := net.Pipe()
	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		if _, _, err := readNegotiate(serverPipeEnd); err != nil {
			errs <- err
			return
		}
		if _, _, err := readNegotiate(serverPipeEnd); err != nil {
			errs <- err
			return
		}
		if err := writeNegotiate(serverPipeEnd, 3, []byte("auth failed")); err != nil {
			errs <- err
			return
		}
	}()

	inner := thrift.NewTSocketFromConnConf(clientPipeEnd, &thrift.TConfiguration{})
	sasl := transport.NewSaslPlain(inner, "alice", "wrong")

	err := sasl.Open()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth failed")

	require.NoError(t, <-errs)
}

// failOpenTransport is a thrift.TTransport whose Open always fails, used to
// prove saslPlain.Open propagates a failure from the inner transport.
type failOpenTransport struct {
	thrift.TTransport
}

func (failOpenTransport) IsOpen() bool { return false }

func (failOpenTransport) Open() error { return errors.New("inner open failed") }

func TestSaslPlain_OpenPropagatesInnerOpenFailure(t *testing.T) {
	t.Parallel()
	sasl := transport.NewSaslPlain(failOpenTransport{}, "alice", "s3cret")

	err := sasl.Open()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inner open failed")
}
