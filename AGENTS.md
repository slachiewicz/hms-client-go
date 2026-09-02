# `hms-client-go` Agent Guide

> **Official Guide for AI Coding Agents working on `hms-client-go`.**  
> `CLAUDE.md` is a pointer to this file; there is no second, competing set of instructions.

## What this is

A pure-Go client library for **Apache Hive Metastore (HMS)** supporting **Hive 2.3, 3.x, and 4.x** with dual Binary TCP / Thrift-over-HTTP transports, Multi-Catalog support, High Availability (HA) failover, and zero Cgo dependencies.

Module path: `github.com/slachiewicz/hms-client-go`. Public package: `hms` at the module root.

## Where things are defined

* [`SPEC.md`](SPEC.md) is canonical for the public API, the per-version RPC and transport matrix (§2), the fallback rules (§2.3), and the error mapping (§7). Do not restate or contradict it elsewhere; change it.
* [`PLAN.md`](PLAN.md) holds the layout, component design, and implementation slices.

## Invariants

These hold regardless of which task you are on. Breaking one is a defect even when the tests pass:

1. **Pure Go / Zero Cgo**: No Cgo dependencies. Never introduce JVM, Hadoop XML, or native C Kerberos/GSSAPI requirements. A pure-Go Kerberos implementation is permitted later; it is out of scope for 1.0.
2. **Hive 2/3/4 Interoperability**: Code generated from the Hive 4 IDL MUST keep working against Hive 2.x and 3.x servers. Follow SPEC §2.3: fallbacks are keyed on `TApplicationException(UNKNOWN_METHOD)`, cached per connection, and `catName` is never written on the wire to a server without catalog support. Check SPEC §2.1 before assuming an RPC exists on a given version.
3. **Context Safety**: Every network I/O call MUST respect `context.Context` cancellation and deadlines on the underlying socket or HTTP transport. Binding happens in the `thrift.TClient` wrapper (PLAN §3.2), never by ignoring the context.
4. **Clean Abstractions**: No generated Thrift type appears in an exported identifier of package `hms`. Conversion lives in `convert.go`.
5. **Small Binaries**: Never store the generated `ThriftHiveMetastoreClient` (or a struct holding it) in a field whose type is reachable from an interface. Bind the methods you use into `func` fields. See PLAN §1 goal 6 for the measurement.

## Verification Gate

`make check` runs the whole gate:

```sh
gofmt -l .                                 # must print nothing
go vet ./...
go test -short -race ./...                 # -short skips Docker HMS containers
golangci-lint run ./...                    # gen/ is excluded in .golangci.yml
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

Integration tests build with `-tags integration` and need Docker (`make test-docker`).

## Go Version

* **Floor**: `go 1.26.0` in `go.mod`.
* **CI Build Toolchain**: `1.27.0`.

## Thrift

* Compiler and library are both **0.24.0**. `scripts/gen-thrift.sh` refuses to run with a different compiler version. Bump both together.
* `idl/` and `gen/` are committed. Never hand-edit `gen/`; regenerate with `make gen` and commit the diff.

## Testing

* Tests live in an external `<pkg>_test` package (black-box) and are table-driven.
* Use `t.Parallel()` in both parent tests and subtests.
* `github.com/stretchr/testify` (`assert` + `require`) is the preferred test assertion library.
* Fallback and wire-format behaviour is tested against an in-process fake Thrift server, not by mocking the generated client.

## Persistent Memory (ICM)

Agents working on this repository MUST invoke `icm store`:
1. When resolving a difficult bug or test failure (`-t errors-resolved`).
2. When making an architectural or format design decision (`-t decisions-hms-client-go`).
3. When completing a milestone or significant task (`-t context-hms-client-go`).
