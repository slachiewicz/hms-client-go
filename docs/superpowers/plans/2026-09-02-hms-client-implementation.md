# hms-client-go Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a pure-Go Apache Hive Metastore client (package `hms`) that talks to Hive 2.3, 3.x and 4.x over binary Thrift TCP and, for Hive 4, Thrift-over-HTTP, with catalog support, HA failover, and context-safe I/O.

**Architecture:** A thin idiomatic wrapper (`hms`) over the committed generated Thrift bindings in `gen/hive_metastore`. Context binding happens in a one-method `thrift.TClient` wrapper around every connection. Version differences are handled by per-connection fallback caching keyed on `TApplicationException(UNKNOWN_METHOD)`. Wire behaviour is tested against an in-process fake server built from the generated processor, from which RPCs can be deleted to simulate older Hive versions.

**Tech Stack:** Go 1.26, `github.com/apache/thrift v0.24.0`, `github.com/stretchr/testify` (`assert` + `require`), `golangci-lint` v2, `govulncheck`.

**Spec:** `SPEC.md` (canonical API, compatibility matrix §2, fallback rules §2.3, transport §3, HA §4, errors §7). Layout and design notes: `PLAN.md`. Repo rules: `AGENTS.md`.

## Global Constraints

- `go 1.26.0` floor in `go.mod`; CI toolchain 1.27.0. No Cgo. No new dependencies beyond `github.com/apache/thrift v0.24.0` and `github.com/stretchr/testify` without asking.
- Public package is `hms` at the module root. **No generated Thrift type in any exported identifier.** Conversion only in `convert.go`.
- **Never store `*hive_metastore.ThriftHiveMetastoreClient` (or a struct holding it) in an interface-reachable field.** Bind the methods you use into `func` fields on `conn` (Task 7). Binary-size invariant, AGENTS.md #5.
- Every network call honours `context.Context` deadlines and cancellation on the underlying socket (Task 3).
- `catName` is never written on the wire to a server without catalog support (Task 7 `applyCat`).
- Tests: external `_test` package, table-driven, `t.Parallel()` on parents and subtests, testify. Fallback and wire tests use the fake server from Task 6, never mocks of the generated client.
- `make check` must pass before every commit: `gofmt -l .` empty, `go vet ./...`, `go test -short -race ./...`, `golangci-lint run ./...`, `govulncheck`.
- Commit messages: imperative subject, no session URLs, no AI trailers. Commit after each task.
- `gen/` is never hand-edited.

## Generated API cheat sheet (from `gen/hive_metastore`, verified)

```go
hive_metastore.NewThriftHiveMetastoreClient(c thrift.TClient) *ThriftHiveMetastoreClient
hive_metastore.NewThriftHiveMetastoreProcessor(h ThriftHiveMetastore) *ThriftHiveMetastoreProcessor // has ProcessorMap() map[string]thrift.TProcessorFunction

// client methods used (receiver *ThriftHiveMetastoreClient)
GetCatalogs(ctx) (*GetCatalogsResponse, error)                 // .Names []string
GetCatalog(ctx, *GetCatalogRequest) (*GetCatalogResponse, error)   // req.Name; resp.Catalog
CreateCatalog(ctx, *CreateCatalogRequest) error                    // req.Catalog *Catalog
DropCatalog(ctx, *DropCatalogRequest) error                        // req.Name
GetAllDatabases(ctx) ([]string, error)
GetDatabase(ctx, name string) (*Database, error)
GetDatabaseReq(ctx, *GetDatabaseRequest) (*Database, error)        // Name *string, CatalogName *string  (Hive 4 only; NOT used, see Task 7)
CreateDatabase(ctx, *Database) error
DropDatabase(ctx, name string, deleteData, cascade bool) error
GetAllTables(ctx, dbName string) ([]string, error)
GetTableReq(ctx, *GetTableRequest) (*GetTableResult_, error)       // DbName, TblName, CatName *string; result.Table
GetTableObjectsByNameReq(ctx, *GetTablesRequest) (*GetTablesResult_, error) // DbName, TblNames, CatName *string; result.Tables
CreateTable(ctx, *Table) error
AlterTable(ctx, dbName, tblName string, newTbl *Table) error
DropTable(ctx, dbName, name string, deleteData bool) error
GetPartitions(ctx, dbName, tblName string, maxParts int16) ([]*Partition, error)
GetPartitionsReq(ctx, *PartitionsRequest) (*PartitionsResponse, error) // CatName *string, DbName, TblName, MaxParts int16; resp.Partitions
GetPartitionNames(ctx, dbName, tblName string, maxParts int16) ([]string, error)
AddPartitionsReq(ctx, *AddPartitionsRequest) (*AddPartitionsResult_, error) // DbName, TblName, Parts, IfNotExists, NeedResult_, CatName *string
AlterPartitions(ctx, dbName, tblName string, parts []*Partition) error
AlterPartitionsReq(ctx, *AlterPartitionsRequest) (*AlterPartitionsResponse, error) // CatName *string, DbName, TableName, Partitions
DropPartition(ctx, dbName, tblName string, partVals []string, deleteData bool) (bool, error)
GetConfigValue(ctx, name, defaultValue string) (string, error)

// exceptions (all have .Message string): *NoSuchObjectException, *AlreadyExistsException,
// *InvalidOperationException, *InvalidObjectException, *InvalidInputException, *MetaException
// thrift.TApplicationException has TypeId() int32; thrift.UNKNOWN_METHOD == 1

// generated struct fields (pointer = optional on the wire)
Catalog{Name string; Description *string; LocationUri string; CreateTime *int32}
Database{Name, Description, LocationUri string; Parameters map[string]string; OwnerName *string; OwnerType *PrincipalType; CatalogName *string; CreateTime *int32}
Table{TableName, DbName, Owner string; CreateTime, LastAccessTime, Retention int32; Sd *StorageDescriptor; PartitionKeys []*FieldSchema; Parameters map[string]string; ViewOriginalText, ViewExpandedText, TableType string; CatName *string; OwnerType PrincipalType}
Partition{Values []string; DbName, TableName string; CreateTime, LastAccessTime int32; Sd *StorageDescriptor; Parameters map[string]string; CatName *string}
StorageDescriptor{Cols []*FieldSchema; Location, InputFormat, OutputFormat string; Compressed bool; NumBuckets int32; SerdeInfo *SerDeInfo; BucketCols []string; SortCols []*Order; Parameters map[string]string; StoredAsSubDirectories *bool}
FieldSchema{Name, Type, Comment string}; SerDeInfo{Name, SerializationLib string; Parameters map[string]string}; Order{Col string; Order int32}
PrincipalType_USER=1, PrincipalType_ROLE=2, PrincipalType_GROUP=3
```

Thrift library (0.24.0): `thrift.NewTSocketFromConnConf(conn net.Conn, cfg *thrift.TConfiguration) *thrift.TSocket`, `thrift.NewTBufferedTransport(t, 8192)`, `thrift.NewTBinaryProtocolConf(t, cfg)`, `thrift.NewTStandardClient(in, out TProtocol) *TStandardClient` (implements `TClient`), `thrift.NewTHttpClientWithOptions(url, thrift.THttpClientOptions{Client: *http.Client})` returning `TTransport` with `SetHeader`, `thrift.NewTServerSocket(addr)`, `thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)` with `Listen()`, `Serve()`, `Stop()`, `thrift.NewTBufferedTransportFactory(8192)`, `thrift.NewTBinaryProtocolFactoryConf(cfg)`.

`thrift.TClient` is:
```go
type TClient interface {
    Call(ctx context.Context, method string, args, result TStruct) (ResponseMeta, error)
}
```

---

## File map

| File | Responsibility |
|---|---|
| `.github/workflows/ci.yml` | `make check` on push/PR (Task 1) |
| `errors.go` | Sentinels, `wrapError`, `isUnknownMethod` (Task 2) |
| `internal/transport/uri.go` | Parse comma-separated endpoint list into `[]Endpoint` (Task 3) |
| `internal/transport/ctxclient.go` | `ContextClient`: `thrift.TClient` wrapper binding ctx to the socket (Task 3) |
| `internal/transport/binary.go` | `DialBinary`: TCP dial, optional SASL PLAIN, buffered binary protocol (Tasks 3, 4) |
| `internal/transport/sasl.go` | SASL PLAIN `thrift.TTransport` (Task 4) |
| `internal/transport/http.go` | `NewHTTP`: THttpClient with headers and default path (Task 5) |
| `internal/hmstest/server.go` | In-process fake HMS built on the generated processor (Task 6) |
| `types.go` | Exported clean structs and enums (Task 7) |
| `convert.go` | Generated ↔ clean conversions (Tasks 7, 8, 9) |
| `options.go` | `Option`, `CatalogOption`, `config` (Task 7) |
| `conn.go` | `conn`: bound `func` fields, per-connection fallback cache (Task 7) |
| `client.go` | `Client`, `New`, `Close`, `call` helper, catalog + database ops (Task 7) |
| `table.go` | Table ops (Task 8) |
| `partition.go` | Partition ops with Rules 3, 4, 5 (Task 9) |
| `formats.go` | Iceberg / Delta / Hudi builders (Task 10) |
| `internal/ha/cluster.go` | Endpoint health, cooldown, sticky-active selection (Task 11) |
| `client.go` (modify) | Retry/failover loop in `call` (Task 11) |
| `test/integration_test.go`, `test/docker/hive-2.3.9/Dockerfile`, `.github/workflows/integration.yml` | Docker matrix (Task 12) |

---

### Task 1: CI workflow and doc alignment

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `PLAN.md` §3.2 (first bullet list), `AGENTS.md` invariant 3

**Interfaces:** none.

- [ ] **Step 1: Write the workflow**

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
permissions:
  contents: read
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27.0"
          cache: true
      - name: Install golangci-lint
        run: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v2.13.2
      - name: make check
        run: make check
```

- [ ] **Step 2: Align the context-binding design text**

In `PLAN.md` §3.2 replace the two bullets describing `ctxsocket.go` + `ctxprotocol.go` with:

```
* `internal/transport/ctxclient.go` wraps `thrift.TClient`. Its single method `Call(ctx, ...)` receives the request context, so it is the natural binding point: before delegating it sets `net.Conn` read/write deadlines from `ctx.Deadline()` (fallback: configured socket timeout) and registers `context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })`, releasing the stop handle on return. Wrapping `TProtocol` would need the same logic repeated across ~40 methods; wrapping `TClient` needs it once.
```

In `AGENTS.md` invariant 3 replace "Binding happens at the `TProtocol` layer (PLAN §3.2)" with "Binding happens in the `thrift.TClient` wrapper (PLAN §3.2)". Also update the layout tree in `PLAN.md` §2: replace `ctxsocket.go` and `ctxprotocol.go` lines with `ctxclient.go  # thrift.TClient wrapper binding ctx deadlines/cancel to the socket` and `binary.go     # TCP dial + buffered binary protocol`, and rename `sasl_plain.go` to `sasl.go`, add `uri.go`.

- [ ] **Step 3: Verify locally**

Run: `make check`
Expected: `✓ All checks passed`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml PLAN.md AGENTS.md
git commit -m "Add CI workflow and bind context at the TClient layer"
```

---

### Task 2: Sentinel errors and exception mapping

**Files:**
- Create: `errors.go`, `errors_test.go`

**Interfaces:**
- Produces:
  ```go
  var ErrNotFound, ErrAlreadyExists, ErrInvalidOperation, ErrMeta, ErrUnavailable, ErrNotSupported error
  func wrapError(op string, err error) error      // unexported; maps generated exceptions + transport errors
  func isUnknownMethod(err error) bool            // unexported; TApplicationException with TypeId()==thrift.UNKNOWN_METHOD
  ```
  `wrapError` returns `nil` for `nil`. The returned error's `Error()` is `"<op>: <original message>"` and `errors.Is` matches the sentinel. `errors.Unwrap` twice reaches the original error (sentinel is joined with `fmt.Errorf("%w: %w")` style via a small `hmsError` type).

- [ ] **Step 1: Write the failing tests**

`errors_test.go` (package `hms_test`; the unexported helpers are exercised through an `export_test.go` file in package `hms` that re-exports them as `WrapError` and `IsUnknownMethod`):

```go
package hms_test

import (
	"context"
	"errors"
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
		{"other app exception", thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "x"), hms.ErrMeta},
		{"eof", io.EOF, hms.ErrUnavailable},
		{"econnrefused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, hms.ErrUnavailable},
		{"deadline", context.DeadlineExceeded, hms.ErrUnavailable},
		{"canceled", context.Canceled, hms.ErrUnavailable},
		{"thrift transport exception", thrift.NewTTransportException(thrift.END_OF_FILE, "eof"), hms.ErrUnavailable},
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
```

`export_test.go` (package `hms`):

```go
package hms

var (
	WrapError       = wrapError
	IsUnknownMethod = isUnknownMethod
)
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./... -run 'TestWrapError|TestIsUnknownMethod'`
Expected: compile error, `undefined: hms.WrapError`.

- [ ] **Step 3: Implement**

```go
// errors.go
package hms

import (
	"context"
	"errors"
	"fmt"
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

func (e *hmsError) Error() string { return e.op + ": " + e.cause.Error() }
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
	return fmt.Errorf("%w", ErrMeta)
}

func isUnknownMethod(err error) bool {
	var appErr thrift.TApplicationException
	return errors.As(err, &appErr) && appErr.TypeId() == thrift.UNKNOWN_METHOD
}
```

Note: the final `return fmt.Errorf("%w", ErrMeta)` is a defensive default; simplify to `return ErrMeta` (the implementer should remove the `fmt` import if unused).

- [ ] **Step 4: Run tests**

Run: `make check`
Expected: all pass, `✓ All checks passed`.

- [ ] **Step 5: Commit**

```bash
git add errors.go errors_test.go export_test.go
git commit -m "Add sentinel errors and Thrift exception mapping"
```

---

### Task 3: URI parsing, context-bound TClient, binary dialer

**Files:**
- Create: `internal/transport/uri.go`, `internal/transport/uri_test.go`, `internal/transport/ctxclient.go`, `internal/transport/ctxclient_test.go`, `internal/transport/binary.go`, `internal/transport/binary_test.go`

**Interfaces:**
- Produces:
  ```go
  package transport

  type Scheme string
  const (SchemeThrift Scheme = "thrift"; SchemeHTTP Scheme = "http"; SchemeHTTPS Scheme = "https")

  type Endpoint struct { Scheme Scheme; Host string /* host:port, port defaulted to 9083 for thrift */; URL string /* full URL for http(s), path defaulted to /metastore */ }
  func ParseEndpoints(uris string) ([]Endpoint, error)   // comma-separated; all must share one scheme; empty -> error

  // ContextClient binds per-call ctx deadline/cancel to conn and delegates to inner.
  type ContextClient struct { /* unexported */ }
  func NewContextClient(inner thrift.TClient, conn net.Conn, timeout time.Duration) *ContextClient
  func (c *ContextClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error)

  type BinaryConfig struct { Timeout time.Duration; PlainUser, PlainPassword string /* both empty = NOSASL */ }
  type Conn struct { Client thrift.TClient; Close func() error }
  func DialBinary(ctx context.Context, hostPort string, cfg BinaryConfig) (*Conn, error)
  ```
  `DialBinary` in this task ignores `PlainUser`; Task 4 adds SASL.

- [ ] **Step 1: Write failing URI tests**

```go
package transport_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/hms-client-go/internal/transport"
)

func TestParseEndpoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    []transport.Endpoint
		wantErr string
	}{
		{"single thrift default port", "thrift://hms1", []transport.Endpoint{{Scheme: "thrift", Host: "hms1:9083"}}, ""},
		{"two thrift", "thrift://hms1:9083, thrift://hms2:9084", []transport.Endpoint{{Scheme: "thrift", Host: "hms1:9083"}, {Scheme: "thrift", Host: "hms2:9084"}}, ""},
		{"http default path", "http://hms1:8080", []transport.Endpoint{{Scheme: "http", Host: "hms1:8080", URL: "http://hms1:8080/metastore"}}, ""},
		{"https custom path", "https://hms1/custom", []transport.Endpoint{{Scheme: "https", Host: "hms1", URL: "https://hms1/custom"}}, ""},
		{"mixed schemes", "thrift://a:9083,http://b", nil, "mixed"},
		{"bad scheme", "ftp://a", nil, "scheme"},
		{"empty", "", nil, "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := transport.ParseEndpoints(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
```

- [ ] **Step 2: Implement `uri.go`**

```go
package transport

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

type Scheme string

const (
	SchemeThrift Scheme = "thrift"
	SchemeHTTP   Scheme = "http"
	SchemeHTTPS  Scheme = "https"
)

const (
	DefaultBinaryPort = "9083"
	DefaultHTTPPath   = "/metastore"
)

type Endpoint struct {
	Scheme Scheme
	Host   string // host:port for thrift; host[:port] for http(s)
	URL    string // full URL, http(s) only
}

func ParseEndpoints(uris string) ([]Endpoint, error) {
	var out []Endpoint
	for _, raw := range strings.Split(uris, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("hms: parse endpoint %q: %w", raw, err)
		}
		ep := Endpoint{Scheme: Scheme(u.Scheme), Host: u.Host}
		switch ep.Scheme {
		case SchemeThrift:
			if _, _, err := net.SplitHostPort(u.Host); err != nil {
				ep.Host = net.JoinHostPort(u.Host, DefaultBinaryPort)
			}
		case SchemeHTTP, SchemeHTTPS:
			if u.Path == "" || u.Path == "/" {
				u.Path = DefaultHTTPPath
			}
			ep.URL = u.String()
		default:
			return nil, fmt.Errorf("hms: unsupported scheme %q in %q", u.Scheme, raw)
		}
		if len(out) > 0 && out[0].Scheme != ep.Scheme {
			return nil, fmt.Errorf("hms: mixed schemes in endpoint list %q", uris)
		}
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, errors.New("hms: empty endpoint list")
	}
	return out, nil
}
```

- [ ] **Step 3: Write failing ContextClient tests**

The test uses a hand-rolled `thrift.TClient` stub whose `Call` performs a blocking `conn.Read` on a `net.Pipe` so deadlines and cancellation are observable.

```go
func TestContextClient_DeadlineFromContext(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer server.Close()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := cc.Call(ctx, "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second, "deadline must come from ctx, not the 1h fallback")
	var ne net.Error
	assert.True(t, errors.As(err, &ne) && ne.Timeout())
}

func TestContextClient_CancelClosesInFlightRead(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer server.Close()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := cc.Call(ctx, "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

func TestContextClient_FallbackTimeoutWhenNoDeadline(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer server.Close()
	inner := &blockingReadClient{conn: client}
	cc := transport.NewContextClient(inner, client, 40*time.Millisecond)
	start := time.Now()
	_, err := cc.Call(context.Background(), "get_table", nil, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

type blockingReadClient struct{ conn net.Conn }

func (b *blockingReadClient) Call(_ context.Context, _ string, _, _ thrift.TStruct) (thrift.ResponseMeta, error) {
	buf := make([]byte, 1)
	_, err := b.conn.Read(buf)
	return thrift.ResponseMeta{}, err
}
```

- [ ] **Step 4: Implement `ctxclient.go`**

```go
package transport

import (
	"context"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// ContextClient is a thrift.TClient that binds each call's context to the
// underlying net.Conn: the deadline is copied to the socket and cancellation
// closes the in-flight read/write by moving the deadline to now.
type ContextClient struct {
	inner   thrift.TClient
	conn    net.Conn
	timeout time.Duration
}

func NewContextClient(inner thrift.TClient, conn net.Conn, timeout time.Duration) *ContextClient {
	return &ContextClient{inner: inner, conn: conn, timeout: timeout}
}

func (c *ContextClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	deadline, ok := ctx.Deadline()
	if !ok && c.timeout > 0 {
		deadline = time.Now().Add(c.timeout)
	}
	if !deadline.IsZero() {
		_ = c.conn.SetDeadline(deadline)
	} else {
		_ = c.conn.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.SetDeadline(time.Now()) })
	defer stop()

	meta, err := c.inner.Call(ctx, method, args, result)
	if err != nil && ctx.Err() != nil {
		// Report the context error so callers see Canceled/DeadlineExceeded.
		return meta, ctx.Err()
	}
	return meta, err
}
```

- [ ] **Step 5: Write failing DialBinary test**

Start a `thrift.TSimpleServer4` on `127.0.0.1:0` serving `fb303` only (a `struct{ fb303.FacebookService }` handler that overrides `GetStatus` to return `fb303.FbStatus_ALIVE`), dial it, and call `fb303.NewFacebookServiceClient(conn.Client).GetStatus(ctx)`. Also test that dialing a closed port returns an error wrapping `syscall.ECONNREFUSED` and that a ctx that is already cancelled fails the dial.

- [ ] **Step 6: Implement `binary.go`**

```go
package transport

import (
	"context"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

const bufferSize = 8192

type BinaryConfig struct {
	Timeout       time.Duration
	PlainUser     string
	PlainPassword string
}

type Conn struct {
	Client thrift.TClient
	Close  func() error
}

func DialBinary(ctx context.Context, hostPort string, cfg BinaryConfig) (*Conn, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	raw, err := d.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return nil, err
	}
	tcfg := &thrift.TConfiguration{SocketTimeout: cfg.Timeout, ConnectTimeout: cfg.Timeout}
	var trans thrift.TTransport = thrift.NewTSocketFromConnConf(raw, tcfg)
	// Task 4 inserts SASL PLAIN here when cfg.PlainUser != "".
	trans = thrift.NewTBufferedTransport(trans, bufferSize)
	proto := thrift.NewTBinaryProtocolConf(trans, tcfg)
	std := thrift.NewTStandardClient(proto, proto)
	return &Conn{
		Client: NewContextClient(std, raw, cfg.Timeout),
		Close:  trans.Close,
	}, nil
}
```

`TBinaryProtocolConf` defaults to strict read and strict write, which is what HMS expects.

- [ ] **Step 7: Run gate and commit**

Run: `make check` → `✓ All checks passed`

```bash
git add internal/transport
git commit -m "Add endpoint parsing, context-bound TClient, and binary dialer"
```

---

### Task 4: SASL PLAIN transport

**Files:**
- Create: `internal/transport/sasl.go`, `internal/transport/sasl_test.go`
- Modify: `internal/transport/binary.go` (insert SASL wrap)

**Interfaces:**
- Produces: `func NewSaslPlain(inner thrift.TTransport, user, password string) thrift.TTransport`. Implements `thrift.TTransport`; `Open()` runs the handshake.

Wire format (Java `TSaslTransport`): every negotiation message is `1 byte status | 4 byte big-endian length | payload`. Status codes: `START=1, OK=2, BAD=3, ERROR=4, COMPLETE=5`. Client sends `START` with payload `"PLAIN"`, then `OK` with the SASL PLAIN initial response `"\x00" + user + "\x00" + password`. Server replies `COMPLETE` (empty payload) on success, `BAD` or `ERROR` with a message otherwise. After `COMPLETE`, every data frame in both directions is `4 byte big-endian length | payload`, no status byte. Maximum frame 64 MiB.

- [ ] **Step 1: Write failing tests**

Use `net.Pipe()`. A goroutine plays the server: read status+len+payload, assert `START`/`"PLAIN"`, read `OK`/initial response, assert bytes equal `"\x00alice\x00s3cret"`, write `COMPLETE`. Then the test writes `[]byte("hello")` through the transport, `Flush`es, and the server goroutine must receive a frame `00 00 00 05 h e l l o`; it replies with a `"world"` frame and the client `Read`s `world`. Second test: server replies `BAD` with `"auth failed"` → `Open()` returns an error containing `auth failed`. Third test: `Open()` on a transport whose inner `Open()` fails propagates the error.

- [ ] **Step 2: Implement `sasl.go`**

```go
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

const (
	saslStart    byte = 1
	saslOK       byte = 2
	saslBad      byte = 3
	saslError    byte = 4
	saslComplete byte = 5
	saslMaxFrame      = 64 << 20
)

type saslPlain struct {
	inner    thrift.TTransport
	user     string
	password string
	wbuf     bytes.Buffer
	rbuf     bytes.Reader
	open     bool
}

func NewSaslPlain(inner thrift.TTransport, user, password string) thrift.TTransport {
	return &saslPlain{inner: inner, user: user, password: password}
}

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

func (s *saslPlain) sendNegotiate(status byte, payload []byte) error {
	hdr := [5]byte{status}
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := s.inner.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.inner.Write(payload); err != nil {
		return err
	}
	return s.inner.Flush(context.Background())
}

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

func (s *saslPlain) IsOpen() bool { return s.open && s.inner.IsOpen() }
func (s *saslPlain) Close() error  { s.open = false; return s.inner.Close() }

func (s *saslPlain) Read(p []byte) (int, error) {
	if s.rbuf.Len() == 0 {
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
	}
	return s.rbuf.Read(p)
}

func (s *saslPlain) Write(p []byte) (int, error) { return s.wbuf.Write(p) }

func (s *saslPlain) Flush(ctx context.Context) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(s.wbuf.Len()))
	if _, err := s.inner.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.inner.Write(s.wbuf.Bytes()); err != nil {
		return err
	}
	s.wbuf.Reset()
	return s.inner.Flush(ctx)
}

func (s *saslPlain) RemainingBytes() uint64 { return uint64(s.rbuf.Len()) }
```

Check that `thrift.TTransport` in 0.24 has no further methods (`go doc github.com/apache/thrift/lib/go/thrift.TTransport`); add any missing ones as pass-throughs.

- [ ] **Step 3: Wire into `DialBinary`**

Replace the `// Task 4 inserts SASL` comment with:

```go
	if cfg.PlainUser != "" {
		trans = NewSaslPlain(trans, cfg.PlainUser, cfg.PlainPassword)
		if err := trans.Open(); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
```

- [ ] **Step 4: Gate and commit**

Run: `make check` → pass.

```bash
git add internal/transport
git commit -m "Add SASL PLAIN framing for LDAP/CUSTOM metastore auth"
```

---

### Task 5: Thrift-over-HTTP transport

**Files:**
- Create: `internal/transport/http.go`, `internal/transport/http_test.go`

**Interfaces:**
- Produces:
  ```go
  type HTTPConfig struct {
      Client      *http.Client        // nil -> http.DefaultClient clone with Timeout
      Timeout     time.Duration
      BearerToken string              // JWT mode
      User        string              // x-actor-username when BearerToken == ""; defaults to os user
      Headers     map[string]string   // extra, e.g. Knox
      UserAgent   string              // set by hms from debug.ReadBuildInfo
  }
  func NewHTTP(ctx context.Context, rawURL string, cfg HTTPConfig) (*Conn, error)
  ```

- [ ] **Step 1: Write failing tests**

Use `httptest.NewServer`. The handler records `r.Header` and `r.URL.Path`, then serves a canned fb303 `getStatus` reply by running a `fb303.NewFacebookServiceProcessor(handler)` over `thrift.NewStreamTransport(r.Body, w)` with `thrift.NewTBinaryProtocolConf`. Assertions:
- path is `/metastore` when URL has no path;
- `Content-Type` and `Accept` are `application/x-thrift`;
- `User-Agent` equals `cfg.UserAgent`;
- with `BearerToken: "tok"`: `Authorization: Bearer tok` present and `x-actor-username` absent;
- without token and `User: "alice"`: `x-actor-username: alice`, no `Authorization`;
- extra header `X-Knox: 1` forwarded;
- ctx with 20ms timeout against a handler that sleeps 200ms returns an error (`ctx.Err()`-wrapping).

- [ ] **Step 2: Implement `http.go`**

```go
package transport

import (
	"context"
	"net/http"
	"os/user"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type HTTPConfig struct {
	Client      *http.Client
	Timeout     time.Duration
	BearerToken string
	User        string
	Headers     map[string]string
	UserAgent   string
}

func NewHTTP(_ context.Context, rawURL string, cfg HTTPConfig) (*Conn, error) {
	hc := cfg.Client
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}
	t, err := thrift.NewTHttpClientWithOptions(rawURL, thrift.THttpClientOptions{Client: hc})
	if err != nil {
		return nil, err
	}
	h := t.(*thrift.THttpClient)
	h.SetHeader("Content-Type", "application/x-thrift")
	h.SetHeader("Accept", "application/x-thrift")
	if cfg.UserAgent != "" {
		h.SetHeader("User-Agent", cfg.UserAgent)
	}
	if cfg.BearerToken != "" {
		h.SetHeader("Authorization", "Bearer "+cfg.BearerToken)
	} else {
		h.SetHeader("x-actor-username", userOrDefault(cfg.User))
	}
	for k, v := range cfg.Headers {
		h.SetHeader(k, v)
	}
	tcfg := &thrift.TConfiguration{}
	proto := thrift.NewTBinaryProtocolConf(t, tcfg)
	return &Conn{Client: thrift.NewTStandardClient(proto, proto), Close: t.Close}, nil
}

func userOrDefault(u string) string {
	if u != "" {
		return u
	}
	if cur, err := user.Current(); err == nil && cur.Username != "" {
		return cur.Username
	}
	return "hms-client-go"
}
```

`THttpClient.Flush(ctx)` already builds the request with `ctx`, so no `ContextClient` wrapper is needed for HTTP.

- [ ] **Step 3: Gate and commit**

```bash
make check
git add internal/transport
git commit -m "Add Thrift-over-HTTP transport with Hive 4 headers"
```

---

### Task 6: In-process fake HMS server (`internal/hmstest`)

**Files:**
- Create: `internal/hmstest/server.go`, `internal/hmstest/handler.go`, `internal/hmstest/server_test.go`

**Interfaces:**
- Produces:
  ```go
  package hmstest

  type Version int
  const (Hive23 Version = iota; Hive31; Hive40)

  type Server struct { /* unexported */ }
  func Start(t testing.TB, v Version, opts ...Option) *Server   // registers t.Cleanup(Stop)
  func (s *Server) Addr() string                                // "127.0.0.1:port"
  func (s *Server) URI() string                                 // "thrift://127.0.0.1:port"
  func (s *Server) Stop()
  func (s *Server) Calls() []string                             // RPC names in order, thread-safe copy
  func (s *Server) LastArgs(method string) any                  // last args struct for method (e.g. *hive_metastore.GetTableRequest)
  func (s *Server) Store() *Store                               // in-memory state

  type Option func(*config)
  func WithoutRPC(names ...string) Option                       // delete from processor map -> UNKNOWN_METHOD
  func WithFailNext(n int) Option                               // close the accepted socket before replying for the next n RPCs (used in Task 11)

  type Store struct {
      mu         sync.Mutex
      Catalogs   map[string]*hive_metastore.Catalog
      Databases  map[string]*hive_metastore.Database            // key "cat.db"
      Tables     map[string]*hive_metastore.Table               // key "cat.db.tbl"
      Partitions map[string][]*hive_metastore.Partition         // key "cat.db.tbl"
      Config     map[string]string
  }
  ```
  Version semantics: `Hive23` deletes `get_catalogs, get_catalog, create_catalog, drop_catalog, alter_partitions_req, get_partitions_req` from the processor map. `Hive31` deletes `alter_partitions_req, get_partitions_req`. `Hive40` deletes nothing. `Hive23` handlers also **reject** any request whose `CatName != nil` by returning `MetaException{Message: "unexpected catName"}`; this is how tests prove catName is absent on the wire. `Hive31`/`Hive40` treat `nil` CatName as `"hive"`.

- [ ] **Step 1: Write `handler.go`**

```go
package hmstest

type handler struct {
	hive_metastore.ThriftHiveMetastore // nil; only overridden methods are ever routed (others are deleted or never called)
	v     Version
	store *Store
	rec   *recorder
}
```

Implement, on `*handler`, exactly these methods with the generated signatures, each calling `h.rec.record(name, args)` first:

`GetStatus` (returns `fb303.FbStatus_ALIVE`), `GetConfigValue`, `GetCatalogs`, `GetCatalog`, `CreateCatalog`, `DropCatalog`, `GetAllDatabases`, `GetDatabase`, `CreateDatabase`, `DropDatabase`, `GetAllTables`, `GetTableReq`, `GetTableObjectsByNameReq`, `CreateTable`, `AlterTable`, `DropTable`, `GetPartitions`, `GetPartitionsReq`, `GetPartitionNames`, `AddPartitionsReq`, `AlterPartitions`, `AlterPartitionsReq`, `DropPartition`.

Semantics: missing object → `&hive_metastore.NoSuchObjectException{Message: ...}`; create on existing → `&hive_metastore.AlreadyExistsException{}`; `AddPartitionsReq` with `IfNotExists == false` and existing values → `AlreadyExistsException`; with `IfNotExists == true` skip duplicates; `DropPartition` returns `(true, nil)` when removed, `NoSuchObjectException` when absent; `DropDatabase` with `cascade == false` and tables present → `InvalidOperationException`; `GetPartitionNames` formats `k1=v1/k2=v2` from the table's `PartitionKeys` and the partition `Values`; `GetPartitions*` with `maxParts >= 0` truncates. Keys: `cat(catName) + "." + db` where `cat` returns `"hive"` for nil on Hive31/Hive40 and returns an error on Hive23 when non-nil.

`GetConfigValue` returns `store.Config[name]` or `defaultValue`. `Start` pre-populates `Config["hive.metastore.version"]` with `"2.3.9"`, `"3.1.3"` or `"4.0.1"`.

- [ ] **Step 2: Write `server.go`**

```go
func Start(t testing.TB, v Version, opts ...Option) *Server {
	t.Helper()
	cfg := config{}
	for _, o := range opts { o(&cfg) }
	store := NewStore()
	rec := &recorder{}
	proc := hive_metastore.NewThriftHiveMetastoreProcessor(&handler{v: v, store: store, rec: rec})
	for _, name := range removedRPCs(v) { delete(proc.ProcessorMap(), name) }
	for _, name := range cfg.without { delete(proc.ProcessorMap(), name) }

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &Server{ln: ln, rec: rec, store: store, failNext: cfg.failNext}
	go s.serve(proc)
	t.Cleanup(s.Stop)
	return s
}
```

`serve` accepts connections in a loop; per connection: `trans := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(conn, cfg), 8192)`, `proto := thrift.NewTBinaryProtocolConf(trans, cfg)`, then loop `ok, err := proc.Process(ctx, proto, proto)` until `!ok || err != nil`. When `failNext > 0` (atomic), decrement and `conn.Close()` **after** reading the message header but before replying: implement by wrapping `proto` in a small `TProtocol` that overrides only `ReadMessageBegin` to close the conn and return `io.EOF` when the counter is positive (embedding `thrift.TProtocol` makes this a 1-method override).

Use a plain accept loop, not `TSimpleServer`, so `Stop` can close the listener and all conns deterministically (track conns in a `sync.Map`).

- [ ] **Step 3: Write `server_test.go`**

Dial each version with `transport.DialBinary`, wrap in `hive_metastore.NewThriftHiveMetastoreClient`, and assert:
- `Hive40`: `GetCatalogs` returns `["hive"]`;
- `Hive23`: `GetCatalogs` returns a `thrift.TApplicationException` with `TypeId() == thrift.UNKNOWN_METHOD`;
- `Hive23`: `GetTableReq` with `CatName: ptr("hive")` returns `*MetaException`; with nil `CatName` and no table returns `*NoSuchObjectException`;
- `Calls()` records `"get_catalogs"` and `LastArgs("get_table_req")` is the request struct.
- `WithoutRPC("get_config_value")` makes `GetConfigValue` fail with UNKNOWN_METHOD.

- [ ] **Step 4: Gate and commit**

```bash
make check
git add internal/hmstest
git commit -m "Add in-process fake HMS server for version and wire tests"
```

---

### Task 7: Core client: types, options, conn, call helper, catalog and database ops

**Files:**
- Create: `types.go`, `options.go`, `conn.go`, `client.go`, `convert.go`, `client_test.go`, `catalog_test.go`, `database_test.go`, `version.go`

**Interfaces:**
- Produces (exact, from SPEC §5):
  ```go
  type PrincipalType int  // PrincipalUser=1, PrincipalRole=2, PrincipalGroup=3; String()
  type TableType string    // TableTypeManaged="MANAGED_TABLE", TableTypeExternal="EXTERNAL_TABLE", TableTypeVirtualView="VIRTUAL_VIEW", TableTypeMaterializedView="MATERIALIZED_VIEW"
  type Catalog struct{ Name, Description, LocationURI string }
  type Database struct{ CatalogName, Name, Description, LocationURI string; Parameters map[string]string; OwnerName string; OwnerType PrincipalType }
  type FieldSchema struct{ Name, Type, Comment string }
  type SerDeInfo struct{ Name, SerializationLib string; Parameters map[string]string }
  type Order struct{ Column string; Order int32 }
  type StorageDescriptor struct{ Columns []*FieldSchema; Location, InputFormat, OutputFormat string; Compressed bool; NumBuckets int32; SerDe *SerDeInfo; BucketColumns []string; SortColumns []*Order; Parameters map[string]string; StoredAsSubDirectories bool }
  type Table struct{ ... as SPEC §5.4 ... }
  type Partition struct{ ... as SPEC §5.5 ... }
  type HiveVersion struct{ Major, Minor, Patch int; Raw string }; func (v HiveVersion) String() string; func ParseHiveVersion(s string) (HiveVersion, error)

  type Option func(*config)
  // WithCatalog, WithTimeout, WithMaxRetries, WithRandomEndpointOrder, WithPoolSize, WithHTTPClient, WithHTTPHeaders, WithBearerToken, WithUser, WithPlainAuth  (SPEC §5.1)
  type CatalogOption func(*catalogOpts); func InCatalog(name string) CatalogOption

  type Client struct{ /* unexported */ }
  func New(ctx context.Context, uris string, opts ...Option) (*Client, error)
  func (c *Client) Close() error
  func (c *Client) GetConfigValue(ctx, name, defaultValue string) (string, error)
  func (c *Client) ServerVersion(ctx) (HiveVersion, error)
  func (c *Client) GetCatalogs / GetCatalog / CreateCatalog / DropCatalog        (SPEC §5.2)
  func (c *Client) GetAllDatabases / GetDatabase / CreateDatabase / DropDatabase (SPEC §5.3)
  ```
- Internal contract used by Tasks 8, 9, 11:
  ```go
  // conn.go
  type conn struct {
      close func() error
      // bound generated methods (func fields; never the generated client itself)
      getCatalogs   func(context.Context) (*hive_metastore.GetCatalogsResponse, error)
      ... one func field per RPC in the cheat sheet ...
      fallback sync.Map // method name -> bool (true = use legacy)
      catalogs catalogSupport // unknown / yes / no, probed lazily
  }
  func newConn(ctx context.Context, ep transport.Endpoint, cfg *config) (*conn, error)
  func (cn *conn) useLegacy(method string) bool
  func (cn *conn) markLegacy(method string)
  func (cn *conn) supportsCatalogs(ctx context.Context) (bool, error)   // probes get_catalogs once; UNKNOWN_METHOD -> false

  // client.go
  func (c *Client) call(ctx context.Context, op string, fn func(ctx context.Context, cn *conn) error) error
  //   acquires a conn from the pool, runs fn, wraps the error with wrapError(op, err),
  //   discards the conn (and does not return it to the pool) when errors.Is(err, ErrUnavailable).
  //   Task 11 adds the retry/failover loop here.
  func (c *Client) resolveCat(ctx context.Context, cn *conn, opts []CatalogOption) (cat *string, err error)
  //   returns nil when the effective catalog is "hive" and the conn lacks catalog support;
  //   returns ErrNotSupported when a non-default catalog is requested on such a conn;
  //   otherwise returns a pointer to the effective catalog name (per-call InCatalog overrides WithCatalog, default "hive").
  ```

- [ ] **Step 1: Write failing tests for New/Close/GetConfigValue/ServerVersion**

```go
func TestNew_ConnectsAndReadsVersion(t *testing.T) {
	t.Parallel()
	srv := hmstest.Start(t, hmstest.Hive40)
	c, err := hms.New(context.Background(), srv.URI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	v, err := c.ServerVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, hms.HiveVersion{Major: 4, Minor: 0, Patch: 1, Raw: "4.0.1"}, v)
}

func TestNew_RefusedEndpoint(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "thrift://127.0.0.1:1")
	require.ErrorIs(t, err, hms.ErrUnavailable)
}

func TestNew_BadURI(t *testing.T) {
	t.Parallel()
	_, err := hms.New(context.Background(), "ftp://x")
	require.Error(t, err)
}
```

`ServerVersion` calls `GetConfigValue(ctx, "hive.metastore.version", "")`; when the value is empty it falls back to `"metastore.version"`; if both are empty it returns `HiveVersion{}` with `ErrNotSupported`. (Hive 2.3 and 3.1 answer `hive.metastore.version`; Hive 4 answers both.)

- [ ] **Step 2: Write failing catalog tests**

```go
func TestCatalogs(t *testing.T) {
	t.Parallel()
	t.Run("hive40 round trip", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive40)
		c := mustNew(t, srv.URI())
		ctx := context.Background()
		require.NoError(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark", Description: "d", LocationURI: "s3://b/"}))
		names, err := c.GetCatalogs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"hive", "spark"}, names)
		got, err := c.GetCatalog(ctx, "spark")
		require.NoError(t, err)
		assert.Equal(t, &hms.Catalog{Name: "spark", Description: "d", LocationURI: "s3://b/"}, got)
		require.ErrorIs(t, c.CreateCatalog(ctx, &hms.Catalog{Name: "spark"}), hms.ErrAlreadyExists)
		require.NoError(t, c.DropCatalog(ctx, "spark", false))
		_, err = c.GetCatalog(ctx, "spark")
		require.ErrorIs(t, err, hms.ErrNotFound)
		require.ErrorIs(t, c.DropCatalog(ctx, "spark", false), hms.ErrNotFound)
		require.NoError(t, c.DropCatalog(ctx, "spark", true))
	})
	t.Run("hive23 not supported", func(t *testing.T) {
		t.Parallel()
		srv := hmstest.Start(t, hmstest.Hive23)
		c := mustNew(t, srv.URI())
		_, err := c.GetCatalogs(context.Background())
		require.ErrorIs(t, err, hms.ErrNotSupported)
	})
}
```

- [ ] **Step 3: Write failing database tests**

Table-driven over `[]hmstest.Version{Hive23, Hive31, Hive40}`:
- create → `GetAllDatabases` contains it → `GetDatabase` returns equal struct (with `CatalogName: "hive"` filled on all versions);
- on `Hive23`, `srv.LastArgs("create_database").(*hive_metastore.Database).CatalogName` is `nil`; on `Hive31`/`Hive40` it is `"hive"`;
- `GetDatabase(ctx, "x", hms.InCatalog("spark"))` on `Hive23` → `ErrNotSupported` and **no** `get_database` call recorded;
- `DropDatabase(..., cascade=false)` with a table → `ErrInvalidOperation`; with `ifExists=true` on a missing db → `nil`; with `ifExists=false` → `ErrNotFound`.

Note `GetDatabase` on Hive 3/4 with a non-default catalog: the generated `get_database(name)` has no catalog parameter. Use the Hive convention: pass `name` as `"@" + cat + "#" + db` when `cat != "hive"` (`MetaStoreUtils.prependCatalogToDbName`). On `"hive"` pass the bare name. Test this: on `Hive40`, `GetDatabase(ctx, "db", InCatalog("spark"))` records `get_database` with name `"@spark#db"`; the fake handler must parse that prefix.

- [ ] **Step 4: Implement `types.go`, `options.go`, `conn.go`, `client.go`, `convert.go`, `version.go`**

Key code for `conn.go`:

```go
type catalogSupport int32 // 0 unknown, 1 yes, 2 no

type conn struct {
	close func() error
	fallback sync.Map
	catalogs atomic.Int32

	getCatalogs func(context.Context) (*hive_metastore.GetCatalogsResponse, error)
	getCatalog  func(context.Context, *hive_metastore.GetCatalogRequest) (*hive_metastore.GetCatalogResponse, error)
	// ... every RPC from the cheat sheet
}

func newConn(ctx context.Context, ep transport.Endpoint, cfg *config) (*conn, error) {
	var tc *transport.Conn
	var err error
	switch ep.Scheme {
	case transport.SchemeThrift:
		tc, err = transport.DialBinary(ctx, ep.Host, transport.BinaryConfig{Timeout: cfg.timeout, PlainUser: cfg.plainUser, PlainPassword: cfg.plainPassword})
	default:
		tc, err = transport.NewHTTP(ctx, ep.URL, transport.HTTPConfig{Client: cfg.httpClient, Timeout: cfg.timeout, BearerToken: cfg.bearerToken, User: cfg.user, Headers: cfg.httpHeaders, UserAgent: userAgent()})
	}
	if err != nil {
		return nil, err
	}
	g := hive_metastore.NewThriftHiveMetastoreClient(tc.Client) // local only; never stored
	return &conn{
		close:       tc.Close,
		getCatalogs: g.GetCatalogs,
		getCatalog:  g.GetCatalog,
		// ...
	}, nil
}

func (cn *conn) supportsCatalogs(ctx context.Context) (bool, error) {
	switch cn.catalogs.Load() {
	case 1: return true, nil
	case 2: return false, nil
	}
	_, err := cn.getCatalogs(ctx)
	switch {
	case err == nil:
		cn.catalogs.Store(1); return true, nil
	case isUnknownMethod(err):
		cn.catalogs.Store(2); return false, nil
	default:
		return false, err
	}
}
```

`userAgent()` in `version.go`: `"hms-client-go/" + version` where version comes from `debug.ReadBuildInfo()` `Deps` entry for this module path, else `Main.Version`, else `"devel"`.

Pool in `client.go`: a buffered `chan *conn` of size `poolSize` (default 4) holding idle conns plus an `atomic.Int32` of total conns; `acquire` takes an idle conn or dials a new one when under the limit, otherwise blocks on the channel honouring `ctx.Done()`; `release` returns it or closes it if the client is closed. `New` dials one conn eagerly so bad endpoints fail fast (this is where `TestNew_RefusedEndpoint` gets its error).

`convert.go` naming: `catalogFromThrift/catalogToThrift`, `databaseFromThrift/databaseToThrift(db *Database, cat *string)`, `fieldSchema*`, `serDe*`, `storage*`. Pointer-to-string helpers `ptr(s string) *string` and `deref(p *string) string`. Nil maps convert to nil; times convert `int32` seconds ↔ `time.Unix(int64, 0)` and zero ↔ `time.Time{}`.

- [ ] **Step 5: Gate and commit**

```bash
make check
git add types.go options.go conn.go client.go convert.go version.go *_test.go
git commit -m "Add hms client core with catalog and database operations"
```

---

### Task 8: Table operations

**Files:**
- Create: `table.go`, `table_test.go`
- Modify: `convert.go` (add `tableFromThrift`, `tableToThrift(t *Table, cat *string)`), `conn.go` (bind `getAllTables`, `getTableReq`, `getTableObjectsByNameReq`, `createTable`, `alterTable`, `dropTable` if not already bound in Task 7)

**Interfaces:**
- Produces SPEC §5.4 methods exactly. `GetTables` chunks names by `chunkSize` (package const `defaultChunkSize = 1000`; test uses `hms.SetChunkSizeForTest` from `export_test.go` to set 2).

- [ ] **Step 1: Write failing tests** (table-driven over the three versions)

- create Iceberg-like table with `Storage`, `PartitionKeys`, `Parameters` → `GetTable` returns an equal struct with `CatalogName: "hive"`, `CreateTime` non-zero (fake sets `time.Now().Unix()` when zero);
- `Hive23`: `srv.LastArgs("get_table_req").(*hive_metastore.GetTableRequest).CatName == nil`; `Hive40`: `== "hive"`;
- `GetTable` missing → `ErrNotFound`;
- `GetAllTables` lists names; `GetTables` with 5 names and chunk size 2 records three `get_table_objects_by_name_req` calls and returns 5 tables in request order, skipping names the server does not know;
- `AlterTable` renames `Parameters["x"]` and `GetTable` reflects it;
- `DropTable(ifExists=false)` missing → `ErrNotFound`; `ifExists=true` → nil; `deleteData` is forwarded (`LastArgs("drop_table")` args struct field `DeleteData`).

- [ ] **Step 2: Implement `table.go`**

```go
func (c *Client) GetTable(ctx context.Context, dbName, tableName string, opts ...CatalogOption) (*Table, error) {
	var out *Table
	err := c.call(ctx, "get_table", func(ctx context.Context, cn *conn) error {
		cat, err := c.resolveCat(ctx, cn, opts)
		if err != nil {
			return err
		}
		res, err := cn.getTableReq(ctx, &hive_metastore.GetTableRequest{DbName: dbName, TblName: tableName, CatName: cat})
		if err != nil {
			return err
		}
		out = tableFromThrift(res.Table)
		return nil
	})
	return out, err
}
```

Repeat the shape for the other five. `DropTable` with `ifExists` maps `ErrNotFound` to `nil` after `call` returns (check `errors.Is`).

- [ ] **Step 3: Gate and commit**

```bash
make check
git add table.go table_test.go convert.go conn.go export_test.go
git commit -m "Add table operations"
```

---

### Task 9: Partition operations with fallbacks

**Files:**
- Create: `partition.go`, `partition_test.go`
- Modify: `convert.go` (`partitionFromThrift`, `partitionToThrift(p *Partition, cat *string)`), `conn.go` (bind partition RPCs)

**Interfaces:** SPEC §5.5 exactly. Fallback helper on `conn`:

```go
// tryReq runs req; on UNKNOWN_METHOD it records the fallback and runs legacy.
// Subsequent calls on this conn go straight to legacy.
func (cn *conn) tryReq(ctx context.Context, method string, req, legacy func(context.Context) error) error {
	if cn.useLegacy(method) {
		return legacy(ctx)
	}
	err := req(ctx)
	if isUnknownMethod(err) {
		cn.markLegacy(method)
		return legacy(ctx)
	}
	return err
}
```

- [ ] **Step 1: Write failing tests** (table-driven over the three versions)

- `AddPartitions` 5 partitions with chunk size 2 → three `add_partitions_req` calls; `ifNotExists=false` on duplicate → `ErrAlreadyExists`; `ifNotExists=true` → nil and no duplicates in `Store()`;
- `GetPartitions(maxParts=-1)` returns all; `maxParts=2` returns 2; on `Hive23`/`Hive31` the recorded calls are `get_partitions_req` **once** then `get_partitions` (fallback cached: a second `GetPartitions` call on the same client records `get_partitions` only); on `Hive40` only `get_partitions_req`;
- `GetPartitionNames` returns `["dt=2024-01-01/region=eu", ...]`;
- `AlterPartitions` on `Hive23`/`Hive31` falls back to `alter_partitions`; on `Hive40` uses `alter_partitions_req`; parameters change is visible via `GetPartitions`;
- `DropPartition(ifExists=false)` missing → `ErrNotFound`; `ifExists=true` → nil;
- `maxParts` above `math.MaxInt16` is clamped: `LastArgs("get_partitions_req").(*hive_metastore.PartitionsRequest).MaxParts == math.MaxInt16`.

- [ ] **Step 2: Implement `partition.go`** following the Task 8 shape with `cn.tryReq` for `GetPartitions` (`"get_partitions_req"`) and `AlterPartitions` (`"alter_partitions_req"`). Clamp helper:

```go
func clampParts(n int) int16 {
	switch {
	case n < 0: return -1
	case n > math.MaxInt16: return math.MaxInt16
	default: return int16(n)
	}
}
```

- [ ] **Step 3: Gate and commit**

```bash
make check
git add partition.go partition_test.go convert.go conn.go
git commit -m "Add partition operations with legacy RPC fallbacks"
```

---

### Task 10: Lakehouse format builders

**Files:**
- Create: `formats.go`, `formats_test.go`

**Interfaces:**
```go
func NewIcebergTable(dbName, tableName, location, metadataLocation string, cols []*FieldSchema) *Table
func SetIcebergMetadataLocation(t *Table, newLocation string)   // moves current to previous_metadata_location
func NewDeltaTable(dbName, tableName, location string, cols []*FieldSchema) *Table
func NewHudiTable(dbName, tableName, location string, cols []*FieldSchema, partitionKeys []*FieldSchema) *Table
```

- [ ] **Step 1: Write failing tests** asserting every class name and parameter from SPEC §6 verbatim, `TableType == TableTypeExternal`, `Parameters["EXTERNAL"] == "TRUE"`, and that `SetIcebergMetadataLocation` rotates `metadata_location` into `previous_metadata_location`. Round-trip one Iceberg table through the `Hive40` fake and compare.

- [ ] **Step 2: Implement** with package-level `const` for every class name (`IcebergStorageHandler = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"`, ...). Iceberg tables set `Parameters["storage_handler"]`, `Parameters["table_type"] = "ICEBERG"`, `Parameters["metadata_location"]`, and the SerDe/input/output classes in `Storage`.

- [ ] **Step 3: Gate and commit**

```bash
make check
git add formats.go formats_test.go
git commit -m "Add Iceberg, Delta and Hudi table builders"
```

---

### Task 11: HA cluster and retry

**Files:**
- Create: `internal/ha/cluster.go`, `internal/ha/cluster_test.go`, `ha_test.go`
- Modify: `client.go` (`call`, `New`, pool: conns are now per-endpoint), `options.go` (`WithMaxRetries`, `WithRandomEndpointOrder` take effect)

**Interfaces:**
```go
package ha

type Cluster struct { /* unexported */ }
func New(n int, random bool, now func() time.Time) *Cluster
func (c *Cluster) Pick() (idx int, ok bool)               // sticky active; skips endpoints in cooldown; ok=false when all cooling
func (c *Cluster) MarkFailed(idx int)                     // exponential backoff 1s..30s with full jitter; advances active to the next healthy
func (c *Cluster) MarkHealthy(idx int)                    // resets backoff
func (c *Cluster) Cooling() []int                         // for the recovery probe
```

Client-side rule (SPEC §4.2): a call is retried on another endpoint only while `attempts < maxRetries` and the failure classifies as `ErrUnavailable`. Non-idempotent RPCs (`op` in `create_*`, `add_partitions*`, `drop_*`, `alter_*`) are retried only when the error came from dialing (conn acquisition), never after `fn` started. Idempotent ops (`get_*`) are retried in both cases.

- [ ] **Step 1: Write failing `ha` unit tests** using an injected fake clock: `Pick` returns 0; `MarkFailed(0)` → `Pick` returns 1; after 1s (first backoff) with `MarkFailed` on both, `Pick` is `ok=false` until clock advances; `MarkHealthy` restores; random order yields a permutation containing all indexes.

- [ ] **Step 2: Write failing client failover tests** (`ha_test.go`)

- two `Hive40` fakes, `srv1` with `hmstest.WithFailNext(1)`: `New(ctx, srv1.URI()+","+srv2.URI())` then `GetAllDatabases` succeeds and `srv2.Calls()` contains `"get_all_databases"`;
- `srv1` stopped before `New`: `New` succeeds on `srv2`;
- both stopped: `New` returns `ErrUnavailable`;
- non-idempotent: `srv1` with `WithFailNext(1)` and `WithMaxRetries(3)`: `CreateDatabase` returns `ErrUnavailable` and `srv2.Calls()` does **not** contain `"create_database"`;
- `WithMaxRetries(1)`: `GetAllDatabases` against failing `srv1` returns `ErrUnavailable` without touching `srv2`.

- [ ] **Step 3: Implement** `cluster.go` and the retry loop:

```go
func (c *Client) call(ctx context.Context, op string, fn func(context.Context, *conn) error) error {
	idempotent := strings.HasPrefix(op, "get_")
	var last error
	for attempt := 0; attempt < c.cfg.maxRetries; attempt++ {
		idx, ok := c.cluster.Pick()
		if !ok {
			return wrapError(op, errors.Join(ErrUnavailable, last))
		}
		cn, err := c.acquire(ctx, idx)
		if err != nil {
			c.cluster.MarkFailed(idx)
			last = err
			continue // dial failures are always retryable
		}
		err = fn(ctx, cn)
		if err == nil {
			c.release(idx, cn)
			c.cluster.MarkHealthy(idx)
			return nil
		}
		if errors.Is(classify(err), ErrUnavailable) && ctx.Err() == nil {
			c.discard(idx, cn)
			c.cluster.MarkFailed(idx)
			last = err
			if idempotent {
				continue
			}
		} else {
			c.release(idx, cn)
		}
		return wrapError(op, err)
	}
	return wrapError(op, last)
}
```

Recovery probe: goroutine started by `New`, ticking every 30s (`cfg.probeInterval`, test sets 20ms), for each `Cooling()` index dial and call `getStatus` (bind `fb303` `GetStatus` on `conn`); on success `MarkHealthy`. Stopped by `Close` via a `context.CancelFunc`. Test: `srv1` restarted on the same port is not possible, so test the probe via `MarkFailed(1)` on a healthy `srv2` and assert `Pick` returns 1 again within 500ms.

- [ ] **Step 4: Gate and commit**

```bash
make check
git add internal/ha client.go options.go ha_test.go
git commit -m "Add endpoint failover with backoff and recovery probe"
```

---

### Task 12: Docker integration matrix

**Files:**
- Create: `test/integration_test.go` (build tag `integration`), `test/docker/hive-2.3.9/Dockerfile`, `.github/workflows/integration.yml`
- Modify: `Makefile` (`test-docker` already exists), `PLAN.md` Slice 5 checkboxes

**Interfaces:** consumes the public `hms` API only.

- [ ] **Step 1: Dockerfile for Hive 2.3.9**

```dockerfile
FROM eclipse-temurin:8-jre
ARG HIVE_VERSION=2.3.9
ARG HADOOP_VERSION=2.10.2
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt \
 && curl -fsSL https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz | tar -xz -C /opt
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION} HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH
RUN $HIVE_HOME/bin/schematool -dbType derby -initSchema
EXPOSE 9083
CMD ["hive", "--service", "metastore", "-p", "9083"]
```

- [ ] **Step 2: Integration test**

Environment variable `HMS_URIS` selects the target (e.g. `thrift://localhost:9083`); `HMS_EXPECT_VERSION` (`2.3`, `3.1`, `4.0`) selects expectations. The workflow starts the container (`apache/hive:3.1.3`, `apache/hive:4.0.1`, `apache/hive:4.2.1` with `SERVICE_NAME=metastore`; the 2.3.9 image built from the Dockerfile) and waits for port 9083. One test function per SPEC area: databases, tables (Iceberg/Delta/Hudi builders), partitions (add 1500 → chunking, get with maxParts, alter, drop), catalogs (`ErrNotSupported` on 2.3, round-trip on 3.1/4.x), fallback (on 2.3/3.1 `GetPartitions` succeeds and a second call is fast), HTTP mode on `4.2.1` (`hive.metastore.server.thrift.transport.mode=http` via `HIVE_SITE_CONF_*`... verify the env-var convention in the `apache/hive` image README before writing it; if unsupported, mount a `hive-site.xml`).

- [ ] **Step 3: Workflow** matrix over the four images, `make test-docker` with the env vars. Mark it `workflow_dispatch` + nightly `schedule` rather than on every PR, since it takes minutes.

- [ ] **Step 4: Commit**

```bash
git add test .github/workflows/integration.yml PLAN.md
git commit -m "Add Docker integration matrix for Hive 2.3, 3.1, 4.0 and 4.2"
```

This task cannot be verified on a machine without Docker; the executor must say so explicitly rather than claim it passed.

---

## Self-review notes

- SPEC §2.3 Rule 1 → Task 7 `resolveCat` + `supportsCatalogs`; Rules 3–4 → Task 9 `tryReq`; Rule 5 → Tasks 8, 9 chunking.
- SPEC §3.1 ctx binding → Task 3; SASL → Task 4. §3.2 → Task 5. §4 → Task 11. §5 → Tasks 7–9. §6 → Task 10. §7 → Task 2.
- `ServerVersion` (SPEC §5.6) → Task 7 `version.go`.
- `WithPoolSize` → Task 7 pool; `WithRandomEndpointOrder`, `WithMaxRetries` → Task 11.
- Names used across tasks: `conn`, `call`, `resolveCat`, `tryReq`, `useLegacy`, `markLegacy`, `wrapError`, `isUnknownMethod`, `classify`, `hmstest.Start/WithoutRPC/WithFailNext/LastArgs/Calls`, `transport.DialBinary/NewHTTP/ParseEndpoints/NewContextClient/NewSaslPlain`. Keep them exactly.
