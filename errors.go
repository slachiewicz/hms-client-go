package hms

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// ErrNotFound is returned when NoSuchObjectException is received from the metastore.
var ErrNotFound = errors.New("hms: object not found")

// ErrAlreadyExists is returned when AlreadyExistsException is received from the metastore.
var ErrAlreadyExists = errors.New("hms: object already exists")

// ErrInvalidOperation is returned when InvalidOperationException, InvalidObjectException,
// or InvalidInputException is received from the metastore.
var ErrInvalidOperation = errors.New("hms: invalid operation")

// ErrMeta is returned when MetaException or any other server-side failure is received
// from the metastore, or as a defensive default for unmapped error types.
var ErrMeta = errors.New("hms: metastore error")

// ErrUnavailable is returned when the metastore connection fails, an I/O error occurs,
// or a context is cancelled or exceeds its deadline.
var ErrUnavailable = errors.New("hms: metastore unavailable")

// ErrNotSupported is returned when an RPC method or feature is not supported by the
// connected metastore (e.g., UNKNOWN_METHOD from the server).
var ErrNotSupported = errors.New("hms: not supported by this metastore")

type hmsError struct {
	op       string
	sentinel error
	cause    error
}

func (e *hmsError) Error() string   { return e.op + ": " + e.cause.Error() }
func (e *hmsError) Unwrap() []error { return []error{e.sentinel, e.cause} }

// wrapError wraps an error with operation context and maps it to a sentinel error.
func wrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &hmsError{op: op, sentinel: classify(err), cause: err}
}

// classify maps Thrift exceptions and transport errors to sentinel error types.
func classify(err error) error {
	var (
		noSuch    *hive_metastore.NoSuchObjectException
		exists    *hive_metastore.AlreadyExistsException
		invOp     *hive_metastore.InvalidOperationException
		invObj    *hive_metastore.InvalidObjectException
		invIn     *hive_metastore.InvalidInputException
		meta      *hive_metastore.MetaException
		appErr    thrift.TApplicationException
		transport thrift.TTransportException
		netErr    net.Error
	)
	switch {
	case errors.As(err, &noSuch):
		return ErrNotFound
	case errors.As(err, &exists):
		return ErrAlreadyExists
	case errors.As(err, &invOp), errors.As(err, &invObj), errors.As(err, &invIn):
		return ErrInvalidOperation
	case errors.As(err, &meta):
		return ErrMeta
	case isUnknownMethod(err):
		return ErrNotSupported
	case errors.As(err, &appErr):
		return ErrMeta
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.As(err, &transport), errors.As(err, &netErr):
		return ErrUnavailable
	}
	return ErrMeta
}

func isUnknownMethod(err error) bool {
	var appErr thrift.TApplicationException
	return errors.As(err, &appErr) && appErr.TypeId() == thrift.UNKNOWN_METHOD
}
