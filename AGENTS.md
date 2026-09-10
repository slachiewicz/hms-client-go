# `hms-client-go` Agent Guide

> **Official Guide for AI Coding Agents working on `hms-client-go`.**  
> `CLAUDE.md` is a pointer to this file; there is no second, competing set of instructions.

## What this is

A pure-Go client library for **Apache Hive Metastore (HMS)** supporting **Hive 2.3, 3.x, and 4.x** with dual Binary TCP / Thrift-over-HTTP transports, Multi-Catalog support, High Availability (HA) failover, and zero Cgo dependencies.

Module path: `github.com/slachiewicz/hms-client-go`. Public packages: `hms` at the module root, and `hmstest` (an in-process fake metastore for downstream tests). `gen/` is importable but not API (SPEC §8); `internal/` is private.

## Where things are defined

* [`SPEC.md`](SPEC.md) is canonical for the public API, the per-version RPC and transport matrix (§2), the fallback rules (§2.3), the retry classification (§4.2), the error mapping (§7), and the stability policy (§8). Do not restate or contradict it elsewhere; change it.
* [`PLAN.md`](PLAN.md) holds the layout, component design, and the implementation slices with their status.
* [`CHANGELOG.md`](CHANGELOG.md) (Keep a Changelog) records every user-visible change under `[Unreleased]` in the same commit that makes it. A doc-only or test-only change needs no entry.
* `docs/superpowers/plans/` holds the historical execution plans the first slices were built from. They are not maintained; PLAN.md is.

A change to behaviour lands in three places at once: the code, SPEC.md, and CHANGELOG.md. PLAN.md gets a checkbox.

## Invariants

These hold regardless of which task you are on. Breaking one is a defect even when the tests pass:

1. **Pure Go / Zero Cgo**: No Cgo dependencies. Never introduce JVM, Hadoop XML, or native C Kerberos/GSSAPI requirements. Kerberos is in scope and shipped through the pure-Go `gokrb5` (SPEC §1.1, §3.1); a C or system-library binding for it is still forbidden.
2. **Hive 2/3/4 Interoperability**: Code generated from the Hive 4 IDL MUST keep working against Hive 2.x and 3.x servers. Follow SPEC §2.3: fallbacks are keyed on `TApplicationException(UNKNOWN_METHOD)`, cached per connection, and `catName` is never written on the wire to a server without catalog support. Check SPEC §2.1 before assuming an RPC exists on a given version.
3. **Context Safety**: Every network I/O call MUST respect `context.Context` cancellation and deadlines on the underlying socket or HTTP transport. Binding happens in the `thrift.TClient` wrapper (PLAN §3.2), never by ignoring the context.
4. **Clean Abstractions**: No generated Thrift type appears in an exported identifier of package `hms`. Conversion lives in `convert.go`.
5. **Small Binaries**: Never store the generated `ThriftHiveMetastoreClient` (or a struct holding it) in a field whose type is reachable from an interface. Bind the methods you use into `func` fields. See PLAN §1 goal 6 for the measurement.
6. **Retry classification**: every RPC goes through `Client.read` (idempotent, retried across endpoints on `ErrUnavailable`) or `Client.call` (not retried once started). Which one is decided by SPEC §4.2 point 3, not by the RPC's name; a new method updates that list.
7. **Wire defaults**: build generated request/response structs through their `NewXxx()` constructors so Thrift "optional with default" fields keep their IDL defaults (SPEC Appendix A). A bare struct literal is a defect even when the fake server accepts it.

## Verification Gate

`make check` runs the whole gate:

```sh
gofmt -l .                                 # must print nothing
go vet ./...
go test -short -race ./...                 # -short skips Docker HMS containers
golangci-lint run ./...                    # gen/ is excluded in .golangci.yml; integration files are linted via build-tags
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

Integration tests build with `-tags integration` and need Docker (`make test-docker`, driven by `HMS_URIS`, `HMS_EXPECT_VERSION`, `HMS_USER`; see `.github/workflows/integration.yml`). The `integration` workflow runs on every push to `main` that touches non-doc files and nightly; report a local `make check` as exactly that, never as "CI green".

## Go Version

* **Floor**: `go 1.26.0` in `go.mod`.
* **CI Build Toolchain**: `1.27.0` in both workflows. Bump the two workflow files together.

## Thrift

* Compiler and library are both **0.24.0**. `scripts/gen-thrift.sh` refuses to run with a different compiler version. Bump both together.
* `idl/` and `gen/` are committed. Never hand-edit `gen/`; regenerate with `make gen` and commit the diff.
* The script applies two IDL patches (drop `SkewedInfo.skewedColValueLocationMaps`; rename three `isSet*` fields). Both generator bugs are fixed on `apache/thrift` master but unreleased; the patches go, together, with the `go.mod` bump to the first release carrying them (SPEC §1.1, Appendix A).

## Testing

* Tests live in an external `<pkg>_test` package (black-box) and are table-driven; `*_internal_test.go` is the exception for unexported helpers.
* Use `t.Parallel()` in both parent tests and subtests.
* `github.com/stretchr/testify` (`assert` + `require`) is the preferred test assertion library.
* Fallback and wire-format behaviour is tested against `hmstest` (`hmstest.Start(t, hmstest.Hive23|Hive31|Hive40)`, with `WithoutRPC`/`WithFailNext` for injection), not by mocking the generated client. Extend the fake's handlers when you add an RPC.
* A behaviour that differs against a real server (Appendix A quirks) also gets an assertion in `test/integration_test.go`, which is version-parameterised on `HMS_EXPECT_VERSION`.

## Persistent Memory (ICM)

Agents working on this repository MUST invoke `icm store`:
1. When resolving a difficult bug or test failure (`-t errors-resolved`).
2. When making an architectural or format design decision (`-t decisions-hms-client-go`).
3. When completing a milestone or significant task (`-t context-hms-client-go`).
