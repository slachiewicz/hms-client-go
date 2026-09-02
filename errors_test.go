package hms_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hms "github.com/slachiewicz/hms-client-go"
	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

func TestWrapError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   error
		want error
	}{
		{"nil", nil, nil},
		{"no such object", &hive_metastore.NoSuchObjectException{Message: "db x"}, hms.ErrNotFound},
		{"already exists", &hive_metastore.AlreadyExistsException{Message: "db x"}, hms.ErrAlreadyExists},
		{"invalid operation", &hive_metastore.InvalidOperationException{Message: "op"}, hms.ErrInvalidOperation},
		{"invalid object", &hive_metastore.InvalidObjectException{Message: "obj"}, hms.ErrInvalidOperation},
		{"invalid input", &hive_metastore.InvalidInputException{Message: "in"}, hms.ErrInvalidOperation},
		{"meta", &hive_metastore.MetaException{Message: "boom"}, hms.ErrMeta},
		{"unknown method", thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "get_partitions_req"), hms.ErrNotSupported},
		// The frame-desync class of TApplicationException means the
		// shared connection's framing is corrupted, not merely that this
		// one call failed, so it must classify as ErrUnavailable (which
		// makes do() discard the conn) rather than the generic ErrMeta
		// every other TApplicationException gets below.
		{"bad sequence id", thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "x"), hms.ErrUnavailable},
		{"invalid message type", thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "x"), hms.ErrUnavailable},
		{"protocol error", thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, "x"), hms.ErrUnavailable},
		{"wrong method name", thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "x"), hms.ErrUnavailable},
		{"other app exception", thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "x"), hms.ErrMeta},
		{"eof", io.EOF, hms.ErrUnavailable},
		{"econnrefused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, hms.ErrUnavailable},
		{"deadline", context.DeadlineExceeded, hms.ErrUnavailable},
		{"canceled", context.Canceled, hms.ErrUnavailable},
		{"thrift transport exception", thrift.NewTTransportException(thrift.END_OF_FILE, "eof"), hms.ErrUnavailable},
		// TLS handshake failures (WithTLS, SPEC §3.1/§3.2) implement
		// neither net.Error nor thrift.TTransportException; SPEC §7
		// classifies a connection failure as ErrUnavailable, same as a
		// plain dial error.
		{"tls certificate verification error", &tls.CertificateVerificationError{Err: errors.New("x509: certificate signed by unknown authority")}, hms.ErrUnavailable},
		{"x509 hostname error", x509.HostnameError{Certificate: &x509.Certificate{}, Host: "metastore.example.com"}, hms.ErrUnavailable},
		// classify must pass through an error that already wraps one of
		// this package's own sentinels (e.g. ErrNotSupported returned
		// directly by resolveCat's catalog probe) unchanged, rather than
		// falling through to the ErrMeta default.
		{"wrapped ErrNotSupported stays ErrNotSupported", fmt.Errorf("resolveCat: %w", hms.ErrNotSupported), hms.ErrNotSupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hms.WrapError("get_table", tt.in)
			if tt.want == nil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			assert.ErrorIs(t, got, tt.want)
			assert.ErrorIs(t, got, tt.in, "original error must remain reachable")
			assert.Contains(t, got.Error(), "get_table: ")
		})
	}
}

// TestWrapError_NoDoublePrefix covers the fix for wrapError re-wrapping an
// error a call site already wrapped (e.g. CreateDatabase's own
// wrapAs("create_database", ...), wrapped again by do's wrapError(op, err)
// at the end of its retry loop): the op must not be prefixed twice, and the
// inner call's own sentinel must survive rather than being replaced by
// classify's ErrMeta default for an *hmsError it doesn't otherwise recognize.
func TestWrapError_NoDoublePrefix(t *testing.T) {
	t.Parallel()
	inner := hms.WrapError("create_database", errors.New("boom"))
	outer := hms.WrapError("create_database", inner)

	require.Error(t, outer)
	assert.Same(t, inner, outer)
	assert.Equal(t, 1, strings.Count(outer.Error(), "create_database: "))
}

// TestMessage covers G2: Message(err) must return the server-side
// exception's Message field, not the Go struct dump its own Error() method
// produces (e.g. "NoSuchObjectException({Message:db x not found})"),
// whether the exception is the error directly or wrapped further down the
// chain; a thrift.TApplicationException, which has no Message field, falls
// back to its own (already clean) Error() text; anything else falls back
// to its own Error() text too.
func TestMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   error
		want string
	}{
		{"nil", nil, ""},
		{"no such object extracts Message field", &hive_metastore.NoSuchObjectException{Message: "db x not found"}, "db x not found"},
		{"meta exception extracts Message field", &hive_metastore.MetaException{Message: "boom"}, "boom"},
		{"txn aborted extracts Message field", &hive_metastore.TxnAbortedException{Message: "txn 1 aborted"}, "txn 1 aborted"},
		{
			"exception wrapped further down the chain still extracts Message field",
			fmt.Errorf("get_table_req: %w", &hive_metastore.NoSuchObjectException{Message: "db x not found"}),
			"db x not found",
		},
		{
			"application exception falls back to its own Error text",
			thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "get_partitions_req"),
			thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "get_partitions_req").Error(),
		},
		{"plain error falls back to its own Error text", errors.New("boom"), "boom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hms.Message(tt.in))
		})
	}
}

// TestHmsError_ErrorIsClean covers hmsError.Error() using Message(): the
// text after "<op>: " must be the exception's own Message field, never the
// Go struct dump its default Error()/String() would print.
func TestHmsError_ErrorIsClean(t *testing.T) {
	t.Parallel()
	err := hms.WrapError("get_table_req", &hive_metastore.NoSuchObjectException{Message: "db x not found"})
	require.Error(t, err)
	assert.Equal(t, "get_table_req: db x not found", err.Error())
	assert.NotContains(t, err.Error(), "NoSuchObjectException(")
}

func TestIsUnknownMethod(t *testing.T) {
	t.Parallel()
	assert.True(t, hms.IsUnknownMethod(thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "x")))
	assert.False(t, hms.IsUnknownMethod(thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "x")))
	assert.False(t, hms.IsUnknownMethod(errors.New("x")))
	assert.False(t, hms.IsUnknownMethod(nil))
}
