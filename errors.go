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

// wrapAs wraps err as op with sentinel forced by the caller instead of
// classify, for the rare case where the call site already knows the right
// sentinel and err's shape (e.g. a URI-parsing error from New) gives
// classify nothing to recognize it by. Keeping this in errors.go, alongside
// wrapError, means hmsError is never constructed anywhere else.
func wrapAs(op string, sentinel, err error) error {
	if err == nil {
		return nil
	}
	return &hmsError{op: op, sentinel: sentinel, cause: err}
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
	case isDesyncError(err):
		return ErrUnavailable
	case errors.As(err, &appErr):
		return ErrMeta
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.As(err, &transport), errors.As(err, &netErr):
		return ErrUnavailable
	// The remaining cases pass an error through unchanged when it already
	// is (or wraps) one of this package's own sentinels, e.g. ErrNotSupported
	// returned directly by resolveCat's catalog probe, so re-wrapping via
	// wrapError does not misclassify it as ErrMeta.
	case errors.Is(err, ErrNotFound):
		return ErrNotFound
	case errors.Is(err, ErrAlreadyExists):
		return ErrAlreadyExists
	case errors.Is(err, ErrInvalidOperation):
		return ErrInvalidOperation
	case errors.Is(err, ErrNotSupported):
		return ErrNotSupported
	case errors.Is(err, ErrUnavailable):
		return ErrUnavailable
	case errors.Is(err, ErrMeta):
		return ErrMeta
	}
	return ErrMeta
}

func isUnknownMethod(err error) bool {
	var appErr thrift.TApplicationException
	return errors.As(err, &appErr) && appErr.TypeId() == thrift.UNKNOWN_METHOD
}

// isDesyncError reports whether err is a TApplicationException of the
// frame-desync class: BAD_SEQUENCE_ID, INVALID_MESSAGE_TYPE_EXCEPTION,
// PROTOCOL_ERROR, or WRONG_METHOD_NAME. Any of these means the shared
// connection's read/write framing is itself corrupted -- not merely that
// this one call failed -- so the conn must be discarded and never reused:
// classify(err) == ErrUnavailable makes do() discard it (see do's doc
// comment) and, for an idempotent read, retry on a fresh conn, rather than
// treating the desync as an ordinary server-side MetaException that would
// leave the same broken conn in the pool for the next caller.
func isDesyncError(err error) bool {
	var appErr thrift.TApplicationException
	if !errors.As(err, &appErr) {
		return false
	}
	switch appErr.TypeId() {
	case thrift.BAD_SEQUENCE_ID, thrift.INVALID_MESSAGE_TYPE_EXCEPTION, thrift.PROTOCOL_ERROR, thrift.WRONG_METHOD_NAME:
		return true
	default:
		return false
	}
}
