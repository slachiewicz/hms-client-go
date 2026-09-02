# `hms-client-go` Implementation Plan

> **Implementation Blueprint for `github.com/slachiewicz/hms-client-go`**  
> A pure-Go client library for Apache Hive Metastore (HMS) supporting **Hive 2.3, 3.x, and 4.x** with dual Binary TCP / Thrift-over-HTTP transports, Multi-Catalog support, High Availability (HA) failover, and zero Cgo dependencies.

The public API, compatibility matrix and fallback rules are defined once in [`SPEC.md`](SPEC.md). This plan references those sections and does not restate them.

---

## 1. Overview & Objectives

### Key Design Constraints & Goals
1. **Language Floor**: Pure **Go 1.26.0+** (`go 1.26.0` in `go.mod`). Zero Cgo dependencies.
2. **License**: **Apache-2.0**.
3. **Hive Compatibility**: Apache Hive 2.3.x, 3.1.x, 4.0.x – 4.2.x. See SPEC §2 for the per-version RPC and transport matrix.
4. **Dual Transport Support**: Binary TCP Thrift (`thrift://`) on every version; Thrift-over-HTTP/HTTPS (`http://`, `https://`) on Hive 4.0+. See SPEC §3.
5. **High Availability & Fault Tolerance**: sticky active endpoint with failover across multiple HMS endpoints, exponential backoff, connection pooling. See SPEC §4.
6. **Small Binaries**: the generated Thrift client (roughly 280 RPCs) must never be stored in an interface-reachable field, directly or through a struct that is stored in an interface. Bind the handful of methods the wrapper uses into `func` fields instead. In a downstream project the interface-reachable form cost roughly 6.5 MiB of stripped binary; the `func`-field form cost under 1 MiB. Verify with `go tool nm -size` and `-ldflags=-dumpdep` (look for `<UsedInIface>`).

---

## 2. Repository Layout

```
hms-client-go/
├── .github/
│   └── workflows/
│       ├── ci.yml                 # gofmt, vet, unit tests, golangci-lint, govulncheck
│       └── integration.yml        # Docker matrix: Hive 2.3.9, 3.1.3, 4.0.1, 4.2.1
├── client.go                      # package hms: Client, New, Close
├── conn.go                        # One live connection: bound RPC func fields, UNKNOWN_METHOD fallback cache, catalog probe (SPEC §2.3)
├── options.go                     # Functional options (see SPEC §5.1)
├── types.go                       # Clean Go structs (Catalog, Database, Table, Partition, ...)
├── convert.go                     # Thrift <-> hms type mapping (the only file importing gen/ types into hms)
├── table.go                       # Table operations (SPEC §5.4)
├── partition.go                   # Partition operations (SPEC §5.5)
├── errors.go                      # Sentinel errors & exception unwrapping (SPEC §7)
├── version.go                     # Module version (debug.ReadBuildInfo) and the HTTP User-Agent string
├── formats.go                     # Iceberg / Delta / Hudi table builders (SPEC §6)
├── idl/                           # Committed Thrift IDL (Hive 4.2.1 + fb303) for reproducible generation
│   ├── hive_metastore.thrift
│   └── share/fb303/if/fb303.thrift
├── gen/                           # Generated Thrift code, committed; regenerate with `make gen`
│   ├── fb303/
│   └── hive_metastore/
├── internal/
│   ├── transport/
│   │   ├── uri.go                 # Endpoint list parsing (thrift:// and http(s)://)
│   │   ├── ctxclient.go           # thrift.TClient wrapper binding ctx deadlines/cancel to the socket
│   │   ├── binary.go              # TCP dial + optional SASL PLAIN + buffered binary protocol
│   │   ├── sasl.go                # SASL PLAIN client framing
│   │   └── http.go                # THttpClient wrapper: default path, headers, auth
│   └── ha/
│       └── cluster.go             # Endpoint list, cooldown, sticky-active failover
├── test/
│   ├── integration_test.go        # Version-parameterised suite (build tag: integration)
│   └── docker/
│       └── hive-2.3.9/Dockerfile  # Self-built image; apache/hive publishes no 2.x image
├── scripts/
│   └── gen-thrift.sh
├── .gitignore
├── .golangci.yml
├── go.mod
├── go.sum
├── Makefile
├── AGENTS.md / CLAUDE.md
├── SPEC.md / PLAN.md / README.md
└── LICENSE
```

The public package is `hms` at the module root. There is no `api/` package: a client library with one package is the idiomatic Go shape, and it keeps the import path short.

---

## 3. Detailed Component Design

### 3.1. IDL Generation (`scripts/gen-thrift.sh`)
* Base IDL from Apache Hive `rel/release-4.2.1`: `standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift`.
* `hive_metastore.thrift` includes `share/fb303/if/fb303.thrift`, so fb303 is stored at `idl/share/fb303/if/fb303.thrift` (from `apache/thrift` tag `v0.24.0`, `contrib/fb303/if/fb303.thrift`).
* Thrift compiler **0.24.0**, matching `github.com/apache/thrift v0.24.0` in `go.mod`. Generated code and library must be the same minor version; the script refuses to run otherwise.
* Command:
  ```bash
  thrift -r --gen go:package_prefix=github.com/slachiewicz/hms-client-go/gen/ -out gen/ idl/hive_metastore.thrift
  ```
* The script applies two wire-safe patches to the downloaded IDL before generating, and the patched IDL is what gets committed:
  1. `SkewedInfo.skewedColValueLocationMaps` (`map<list<string>, string>`) is removed. Go has no valid type for a list-keyed map and the generator aborts (THRIFT-2063). See SPEC §1.1 for why removal is safe and retyping is not.
  2. `WMNullableResourcePlan.isSetQueryParallelism`, `.isSetDefaultPoolPath` and `WMNullablePool.isSetSchedulingPolicy` are renamed with a `Flag` suffix. The Go generator emits an `IsSetX()` accessor for every field, so a field literally named `isSetX` produces a field and a method with the same name and the package does not compile (THRIFT-6176). Only field IDs are serialised, so the rename does not change the wire format.
* The generated `*-remote` CLI packages (`package main`) are deleted; they are not part of the library.
* Both `idl/` and `gen/` are committed so `go get` works without a Thrift compiler and so the diff of a regeneration is reviewable.

### 3.2. Dual Transport Architecture

#### Binary TCP Socket (`thrift://`)
* `internal/transport/ctxclient.go` wraps `thrift.TClient`. Its single method `Call(ctx, ...)` receives the request context, so it is the natural binding point: before delegating it sets `net.Conn` read/write deadlines from `ctx.Deadline()` (fallback: configured socket timeout) and registers `context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })`, releasing the stop handle on return. Wrapping `TProtocol` would need the same logic repeated across ~40 methods; wrapping `TClient` needs it once.
* SASL PLAIN (`sasl.go`) wraps the socket when `WithPlainAuth` is set: Thrift SASL handshake (START / OK / COMPLETE status bytes, 4-byte big-endian length prefix), then length-prefixed frames for payload.

#### Thrift-over-HTTP/HTTPS (`http://` / `https://`)
* `internal/transport/http.go` wraps Thrift's `THttpClient`, which already implements `TTransport` and honours the context passed to `Flush(ctx)`.
* Default path `/metastore`. Headers per SPEC §3.2, including `x-actor-username` when no bearer token is set.

### 3.3. High Availability (HA) & Failover
* `internal/ha/cluster.go`: endpoint list, per-endpoint cooldown with exponential backoff and full jitter, sticky active index guarded by `sync.RWMutex`.
* Retry wrapper classifies errors (dial, `io.EOF`, `ECONNREFUSED`, `ECONNRESET`, `ETIMEDOUT`, deadline during I/O) and consults the idempotency rule in SPEC §4.2 before re-sending.
* Background `fb303.getStatus` probe re-enables cooled-down endpoints; stopped by `Close`.

### 3.4. Public Client API
Defined in SPEC §5. Implementation notes:
* `convert.go` is the only file in package `hms` allowed to import `gen/hive_metastore` types into function bodies that touch exported types. Nothing generated is exported.
* The `catName` field is written on the wire only when the connection has confirmed catalog support (SPEC §2.3 Rule 1). For Hive 2.x connections it is left unset so the server never sees an unknown field.
* Every RPC goes through one `call(ctx, name, fn)` helper that does context binding, retry/failover, error unwrapping, and fallback caching.

---

## 4. Step-by-Step Implementation Slices

### Slice 1: Scaffold & Thrift Generation
- [x] `go.mod` with Go `1.26.0`, `github.com/apache/thrift v0.24.0`, `github.com/stretchr/testify`.
- [ ] `git init`, first commit of the scaffold.
- [x] Run `scripts/gen-thrift.sh`; `idl/` and `gen/` generated; `go.sum` added.
- [ ] `.github/workflows/ci.yml` running `make check`.
- [x] `make check` green on the generated code (gofmt, vet, golangci-lint with `gen/` excluded from lint, govulncheck).

### Slice 2: Transports & Connection Management
- [ ] `ctxsocket.go` + `ctxprotocol.go` with deadline propagation and `AfterFunc` cancellation.
- [ ] `http.go` over `THttpClient` with default path and headers.
- [ ] `sasl_plain.go`.
- [ ] `internal/pool/` with idle health checks.
- [ ] Unit tests against in-process TCP and HTTP servers: deadline honoured, cancel closes socket, headers present, SASL handshake bytes.

### Slice 3: High-Level Client & Hive 2/3/4 Interop
- [ ] `types.go`, `convert.go`, `errors.go`.
- [ ] `client.go` with Catalog, Database, Table, and Partition operations per SPEC §5.
- [ ] `fallback.go`: `UNKNOWN_METHOD` detection, per-connection fallback cache, Rules 2–4 from SPEC §2.3.
- [ ] `formats.go` builders per SPEC §6.
- [ ] Unit tests with a fake Thrift server that can be told to reject any RPC with `UNKNOWN_METHOD`, proving each fallback path and that `catName` is absent on the wire for Hive 2 connections.
- [ ] **Breaking changes** (Task 3 fix round, aligning the builders with xtable-hive-metastore's conventions, SPEC §6): `hms.DeltaInputFormat` and `hms.DeltaOutputFormat` are removed (Delta's `StorageDescriptor` now carries no input/output format at all, only the `DeltaStorageHandler`); `hms.HudiOutputFormat`'s value changes from the Hudi-specific output format to `org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat`. Allowed on the v0.x line by SPEC §8.

### Slice 4: High Availability (HA) & Clustering
- [ ] `internal/ha/cluster.go`, multi-URI parsing, cooldown, sticky-active selection.
- [ ] Retry wrapper with idempotency classification.
- [ ] Background recovery probe.
- [ ] Unit tests simulating node outages, recovery, and the non-idempotent-after-flush case.

### Slice 5: Multi-Version Docker Integration Tests
- [x] `test/docker/hive-2.3.9/Dockerfile`, published as `slachiewicz/hive-metastore:2.3.9` (amd64 + arm64) by the on-demand `hive-2.3.9-image` workflow; the matrix pulls it and only builds locally as a fallback. archive.apache.org serves ~250 KB/s, so building inside a CI job does not fit the 30-minute limit.
- [x] Version-parameterised suite under `//go:build integration` against: self-built 2.3.9, `apache/hive:3.1.3`, `apache/hive:4.0.1`, `apache/hive:4.2.1` (binary TCP), plus `apache/hive:4.2.1` in HTTP mode.
- [x] Coverage: Iceberg, Delta Lake, and Hudi table creation, schema evolution, partition batch add/alter/drop, catalog operations on 3.x/4.x, `ErrNotSupported` on 2.x.
- [x] `.github/workflows/integration.yml` with the matrix above. First fully green run: 2026-09-02, all five jobs (2.3.9, 3.1.3, 4.0.1, 4.2.1 binary, 4.2.1 HTTP).

### Slice 7: `set_ugi` over binary NOSASL
- [ ] `options.go`: `WithUserGroups`. `client.go`/`conn.go`: on a newly dialed binary connection with no SASL auth configured and `WithUser` set, call `set_ugi(user, groups)` once before the connection is used for anything else. See SPEC §3.1 and §5.1.
- [ ] `internal/hmstest`: a `set_ugi` handler recording the `(user, groups)` it was called with, so a test can assert it fired exactly once per new connection and never over HTTP or under SASL PLAIN/Kerberos.
- [ ] Integration matrix: no new job; covered by the existing binary-TCP legs (2.3.9, 3.1.3, 4.0.1, 4.2.1) asserting `set_ugi` is accepted.

### Slice 8: Partition lookups & `AlterDatabase`
- [x] `partition.go`: `GetPartitionsByNames`, `GetPartitionsByFilter`, `GetPartitionNamesByValues` per SPEC §5.5. `GetPartitionsByNames` is chunked like `AddPartitions` and, like `GetPartitions`/`AlterPartitions`, tries the 4.x-only `get_partitions_by_names_req`/`get_partition_names_ps_req` request variants first (SPEC §2.3 Rules 6/7) before falling back to the legacy RPCs, which -- unlike `get_partitions_req`/`alter_partitions_req` -- exist on every supported version. `client.go`: `AlterDatabase` per SPEC §5.3. `conn.go`: bind `get_partitions_by_names(_req)`, `get_partitions_by_filter`, `get_partition_names_ps(_req)`, `alter_database`.
- [x] Fold in the SPEC §5.0 catalog-resolution fix: `AlterTable` honours `newTable.CatalogName` the same way `CreateTable` does (already in place from an earlier slice).
- [x] `internal/hmstest`: handlers for the six RPCs above (the four legacy plus the two 4.x-only request variants), backed by the existing in-memory store; `GetPartitionsByFilter`'s fixture supports the `key = 'value'` subset of Hive's filter grammar, joined by `and`, rejecting anything else with `MetaException`.
- [x] Unit tests: chunking boundary for `GetPartitionsByNames`, fallback caching for the two request variants, filter string passed through verbatim (plus the fixture's supported/unsupported grammar), prefix matching for `GetPartitionNamesByValues`, `AlterDatabase` round-trip and `ErrNotFound` on a missing database.
- [x] Integration matrix: extended every leg (2.3+, since the legacy RPCs predate catalogs) with a partition-by-names lookup, a partition-filter lookup, a partial-values name lookup, and a database alter.

### Slice 9: Binary and HTTP TLS
- [ ] `options.go`: `WithTLS` per SPEC §5.1. `internal/transport/binary.go`: wrap the dialed socket in `tls.Client` before the SASL/binary protocol layers, for `thrift://` against a `metastore.use.SSL=true` server. `internal/transport/http.go`: apply the configured `*tls.Config` to the `*http.Client`'s `Transport.TLSClientConfig` for `https://`. See SPEC §3.1, §3.2.
- [ ] Unit tests: binary TLS handshake against an in-process TLS listener wrapping `internal/hmstest`; HTTP TLS client config applied and overridable by a caller-supplied `WithHTTPClient`.
- [ ] Integration matrix: no new job; a TLS-enabled 4.2.1 leg is tracked as a follow-up, not required for 1.0 sign-off, since it needs a certificate-bearing Docker image.

### Slice 10: Notifications
- [ ] `client.go` (or a new `notification.go`): `CurrentNotificationID`, `GetNextNotifications`, `NotificationEvent` per SPEC §5.7. `conn.go`: bind `get_current_notificationEventId`, `get_next_notification`. `convert.go`: `NotificationEvent` conversion.
- [ ] `internal/hmstest`: an append-only event log driven by the store's own mutations (create/drop/alter), plus handlers for the two RPCs.
- [ ] Unit tests: event ordering, `eventTypes` filtering, `lastEventID` pagination.
- [ ] Integration matrix: extend every leg (2.3+) with a notification-id check after a table create.

### Slice 11: Column statistics (read-only)
- [ ] A new `stats.go`: `GetTableColumnStatistics` and the `ColumnStatistics`/`*ColumnStats`/`Decimal` types per SPEC §5.8. `conn.go`: bind `get_table_statistics_req`. `convert.go`: the `ColumnStatisticsData` union-arm conversion.
- [ ] `internal/hmstest`: a `get_table_statistics_req` handler serving fixture stats for at least one column of each of the seven exposed types (bool/long/double/string/binary/decimal/date).
- [ ] Unit tests: each union arm converts to the right `ColumnStatistics` field with the rest nil; an unset optional low/high value stays nil, not a zero-valued pointer.
- [ ] Integration matrix: extend the 3.x/4.x legs with a column-statistics read after `ANALYZE TABLE` (or an equivalent stats-populating step) on a fixture table.

### Slice 12: ACID locks and transactions
- [ ] A new `txn.go`: `OpenTransaction`, `CommitTransaction`, `AbortTransaction`, `Heartbeat`, `Lock`, `CheckLock`, `Unlock`, and the `Lock*`/`LockRequest`/`LockResponse` types per SPEC §5.9. `conn.go`: bind `open_txns`, `commit_txn`, `abort_txn`, `heartbeat`, `lock`, `check_lock`, `unlock`.
- [ ] `internal/hmstest`: a minimal txn/lock table (open txn ids, lock ids and their state) backing the seven handlers above.
- [ ] Unit tests: open/commit/abort round-trip, a lock request that returns `LockStateWaiting` then `LockStateAcquired` on `CheckLock`, heartbeat on a txn-only and lock-only request.
- [ ] Integration matrix: extend the 2.3+ legs with an open/lock/heartbeat/commit cycle against a fixture table.

### Slice 13: `SkewedInfo` exposure (gated)
- [ ] Gate: track the upstream Thrift Go fix for THRIFT-2063 (PR 3778). Not started until a `github.com/apache/thrift` release containing it is available to pin in `go.mod`.
- [ ] Once ungated: `scripts/gen-thrift.sh` stops removing `skewedColValueLocationMaps` from the IDL; regenerate `gen/`. `types.go`: `SkewedInfo`, `SkewedLocation` per SPEC §5.4. `convert.go`: `StorageDescriptor.SkewedInfo` conversion, including the list-keyed map.
- [ ] `internal/hmstest`: a fixture table with skewed columns, values, and at least one location-map entry.
- [ ] Unit tests: round-trip of `ColumnValueLocationMaps` through `CreateTable`/`GetTable`.
- [ ] Integration matrix: extend the 3.x/4.x legs (skew is unsupported on 2.3) with a skewed-table create/read.

### Slice 14: Kerberos (pure Go)
- [x] `go.mod`: add `github.com/jcmturner/gokrb5/v8` (pure Go, zero Cgo, per AGENTS.md invariant #1). `options.go`: `WithKerberos` per SPEC §5.1. `internal/transport/gssapi.go`: SASL GSSAPI (QOP `auth`) negotiation over the binary socket, wired in alongside the existing SASL PLAIN path in `DialBinary`; `sasl.go` drives both through a shared `saslMech` interface. See SPEC §3.1.
- [x] Unit tests (`internal/transport/gssapi_internal_test.go`): the whole handshake against a fake acceptor that validates the AP_REQ with gokrb5's `service` package, over a service ticket minted in-process from a generated keytab (`messages.NewTicket`), so no KDC is involved. Covered: AP_REQ framing and the mutual-required option, AP_REP verification keyed on the echoed authenticator timestamp, an acceptor subkey taking over the security layer's wrap tokens, RFC 4121 wrap-token rotation, the QOP `auth` selection, and an fb303 `getStatus` round trip over the resulting transport. Failure legs: a `BAD` frame, an AP_REP encrypted with the wrong key, a skewed AP_REP timestamp, an offer without the `auth` bit, and an offer whose checksum fails.
- [ ] Not covered by unit tests, and only a KDC-backed leg can cover it: the KDC exchanges themselves (AS and TGS), credential loading from a real keytab or `ccache` beyond its error paths, and interoperability with a JDK `SaslServer` acceptor -- notably whether Hive's acceptor sends an AP_REP subkey and whether it rotates its wrap tokens.
- [ ] Integration matrix: a Kerberized 4.2.1 leg is tracked as a follow-up (needs a KDC sidecar in the Docker Compose fixture), not required for 1.0 sign-off. `test/integration_test.go`'s `TestKerberos` is the placeholder: it skips unless `HMS_KRB5_URIS` is set, so the leg is a matter of adding the fixture and the environment, not new test code.

### Slice 15: Observability & API stability
- [ ] `options.go`: `WithLogger`, `WithRPCObserver` per SPEC §5.10. `client.go`: emit `RPCInfo` from the `call`/`read` retry loop (`do`); log connection dial/close, `MarkFailed`/`MarkHealthy` transitions, and recovery-probe sweeps through the configured logger.
- [ ] `go.mod`/tag policy: confirm module tags stay `v0.x` until the `polytable` adoption note below ships, per SPEC §8.
- [ ] Unit tests: an observer sees one `RPCInfo` per attempt (including retried attempts) with the right `Attempt`/`Err`; a logger sees a dial/close pair and a failover transition under a simulated endpoint outage.
- [ ] Integration matrix: no new job; observability is exercised incidentally by the other legs via `WithLogger` attached to the test harness's own logger.

### Downstream: adoption in `polytable`
Tracked in the `polytable` repository, not here: replace `github.com/beltran/gohive` with this module in `pkg/catalog/hms.go` and confirm its unit and Docker suites pass. This module reaches 1.0.0 only after that adoption has shipped.
