# hms-client-go

A pure-Go client for **Apache Hive Metastore** that speaks to **Hive 2.3, 3.x and 4.x** servers over binary Thrift and, on Hive 4, Thrift-over-HTTP. No Cgo, no JVM, no Hadoop configuration files.

[![ci](https://github.com/slachiewicz/hms-client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/slachiewicz/hms-client-go/actions/workflows/ci.yml)
[![integration](https://github.com/slachiewicz/hms-client-go/actions/workflows/integration.yml/badge.svg)](https://github.com/slachiewicz/hms-client-go/actions/workflows/integration.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/slachiewicz/hms-client-go.svg)](https://pkg.go.dev/github.com/slachiewicz/hms-client-go)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8.svg)](go.mod)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> **Status:** `v0.1.0` is the first tagged release. The API follows the stability policy in [SPEC.md §8](SPEC.md): `v0.x` may still change before `v1.0.0`; every release is verified nightly against real Hive 2.3.9, 3.1.3, 4.0.1 and 4.2.1 metastores.
> on `main`, pending a `v0.1.0` tag. The API is usable today and is verified nightly against
> real metastores, but it may still change before `v1.0.0`. See [SPEC.md §8](SPEC.md) for the
> stability policy.

## What it does

* **Catalog, database, table and partition operations** with clean Go types; generated Thrift types never appear in the API.
* **One client for three Hive generations.** Generated from the Hive 4.2.1 IDL; the Hive 4-only RPCs fall back to their legacy forms on 2.3 and 3.x, and catalog names are never sent to servers that predate catalogs.
* **Two transports.** `thrift://` binary TCP on every version, with optional SASL PLAIN for LDAP-authenticated metastores; `http://` and `https://` on Hive 4, with JWT bearer tokens and custom headers for gateways such as Knox.
* **Failover.** Comma-separated endpoints, a sticky active endpoint, jittered backoff, per-endpoint connection pools and a recovery probe.
* **Context-safe I/O.** Every deadline and cancellation reaches the socket.
* **Iceberg, Delta Lake and Hudi** table builders with the exact storage-handler, SerDe and parameter conventions those engines expect.
* **Identity and auth.** `set_ugi` caller identity over binary NOSASL, SASL PLAIN for
  LDAP/CUSTOM, pure-Go Kerberos (`gokrb5`, zero Cgo), and TLS for both transports.
* **Notifications, column statistics, and ACID.** Metastore event polling, read-only column
  statistics, and minimal lock/transaction RPCs; observability via `WithLogger` and
  `WithRPCObserver`.

## Verified against

Every push to `main` and a nightly schedule run the [integration matrix](.github/workflows/integration.yml) against real metastores in Docker:

| Metastore | Transport | Image |
| :--- | :--- | :--- |
| Hive 2.3.9 | binary | `slachiewicz/hive-metastore:2.3.9` (built from [`test/docker`](test/docker/hive-2.3.9)) |
| Hive 3.1.3 | binary | `apache/hive:3.1.3` |
| Hive 4.0.1 | binary | `apache/hive:4.0.1` |
| Hive 4.2.1 | binary and HTTP | `apache/hive:4.2.1` |

The unit suite runs against an in-process fake metastore that emulates each version's RPC set, so version fallbacks are tested on every commit without Docker.

The Kerberos and TLS code paths are unit-tested (a fake GSSAPI acceptor and an in-process TLS
listener) but have no integration matrix job yet: a Kerberized leg needs a KDC sidecar and a
TLS leg needs a certificate-bearing image, both tracked as follow-ups (see PLAN.md Slices 9
and 14).

## Install

```sh
go get github.com/slachiewicz/hms-client-go
```

## Usage

```go
ctx := context.Background()
c, err := hms.New(ctx, "thrift://hms1:9083,thrift://hms2:9083",
	hms.WithTimeout(30*time.Second))
if err != nil {
	log.Fatal(err)
}
defer c.Close()

tbl, err := c.GetTable(ctx, "default", "events", hms.InCatalog("spark_catalog"))
switch {
case errors.Is(err, hms.ErrNotFound):
	log.Println("table does not exist")
case errors.Is(err, hms.ErrNotSupported):
	log.Println("this metastore has no catalogs")
case err != nil:
	log.Fatal(err)
default:
	log.Println(tbl.TableName, tbl.Owner)
}

// Register an Iceberg table.
ice := hms.NewIcebergTable("default", "orders", "s3://bucket/orders",
	"s3://bucket/orders/metadata/v1.metadata.json",
	[]*hms.FieldSchema{{Name: "id", Type: "bigint"}})
if err := c.CreateTable(ctx, ice); err != nil {
	log.Fatal(err)
}
```

Errors map to six sentinels (`ErrNotFound`, `ErrAlreadyExists`, `ErrInvalidOperation`, `ErrMeta`, `ErrUnavailable`, `ErrNotSupported`); the original Thrift exception stays reachable through `errors.As`.

## Building and testing

```sh
make check        # gofmt, go vet, go test -race, golangci-lint, govulncheck
make test-docker  # integration suite; needs HMS_URIS and HMS_EXPECT_VERSION, see the workflow
make gen          # regenerate gen/ from idl/ (Thrift 0.24.0 compiler required)
```

The Thrift Go generator cannot compile the Hive IDL as published; `scripts/gen-thrift.sh` applies two wire-safe patches and explains why ([THRIFT-2063](https://issues.apache.org/jira/browse/THRIFT-2063), [THRIFT-6176](https://issues.apache.org/jira/browse/THRIFT-6176)).

## Documentation

* [SPEC.md](SPEC.md): the canonical API, compatibility matrix, fallback rules, server quirks and stability policy.
* [PLAN.md](PLAN.md): layout, design notes and the implementation roadmap to 1.0.
* [AGENTS.md](AGENTS.md): invariants and the verification gate for contributors and coding agents.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
