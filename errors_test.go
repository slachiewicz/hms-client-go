package hms_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

func TestIsUnknownMethod(t *testing.T) {
	t.Parallel()
	assert.True(t, hms.IsUnknownMethod(thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "x")))
	assert.False(t, hms.IsUnknownMethod(thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "x")))
	assert.False(t, hms.IsUnknownMethod(errors.New("x")))
	assert.False(t, hms.IsUnknownMethod(nil))
}
