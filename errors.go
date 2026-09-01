package hms

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// Sentinel errors. Every error returned by Client matches exactly one of
// these with errors.Is; the original Thrift exception stays reachable via
// errors.Unwrap / errors.As.
var (
	ErrNotFound         = errors.New("hms: object not found")
	ErrAlreadyExists    = errors.New("hms: object already exists")
	ErrInvalidOperation = errors.New("hms: invalid operation")
	ErrMeta             = errors.New("hms: metastore error")
	ErrUnavailable      = errors.New("hms: metastore unavailable")
	ErrNotSupported     = errors.New("hms: not supported by this metastore")
)

type hmsError struct {
	op       string
	sentinel error
	cause    error
}

func (e *hmsError) Error() string   { return e.op + ": " + e.cause.Error() }
func (e *hmsError) Unwrap() []error { return []error{e.sentinel, e.cause} }

func wrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &hmsError{op: op, sentinel: classify(err), cause: err}
}

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
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return ErrUnavailable
	}
	return ErrMeta
}

func isUnknownMethod(err error) bool {
	var appErr thrift.TApplicationException
	return errors.As(err, &appErr) && appErr.TypeId() == thrift.UNKNOWN_METHOD
}
