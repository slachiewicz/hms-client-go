# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project has not yet tagged
a release, so everything below is unreleased.

## [Unreleased]

### Added

- `GetPartitionsSeq` and `GetTablesSeq`, `iter.Seq2` streaming forms of `GetPartitions`/
  `GetTables` that actually bound memory: each calls its names-only RPC once
  (`get_partition_names`/`get_all_tables`) and then fetches by name in chunks of
  `WithChunkSize`, yielding each result as its chunk arrives instead of accumulating the whole
  table's or database's result in memory first (SPEC §5.4, §5.5, G11). Breaking out of the range
  loop early issues no further RPCs. `GetPartitions`, `GetPartitionsByNames`,
  `GetPartitionsByFilter`, and `GetPartitionsSeq` now intern identical `Storage.Columns` lists
  (by `Name`/`Type`/`Comment`) within one call, so partitions sharing a column list share one
  `[]*FieldSchema` instead of each getting its own copy; `GetTables` and `GetTablesSeq` do the
  same for `Table.Storage.Columns`. See `Partition.Storage`'s and `Table.Storage`'s doc comments
  for the resulting aliasing contract.
- `DropPartitionsByNames` and `DropPartitions` (by partition values), both wrapping the batched
  `drop_partitions_req` RPC, which carries no legacy-RPC fallback: it is declared in the Hive
  2.3.9 and 3.1.3 IDL as well as 4.2.1's (SPEC §2.1, §2.3 Rule 2, §5.5).
- `PartitionName`, building a Hive-style partition name from a table's partition keys and one
  partition's values, following Hive's own `Warehouse.makePartName`/`FileUtils.escapePathName`
  escaping and lowercasing rules exactly (SPEC §5.5).

### Changed

- `AlterPartitions` now batches at `WithPartitionBatchSize` (the same knob `AddPartitions` and
  the new `DropPartitionsByNames` use), sending one `alter_partitions_req`/`alter_partitions` per
  batch instead of the whole `partitions` slice in one request (SPEC §2.3 Rule 5, §5.5).

## [0.1.0] - 2026-09-02

### Added

- `set_ugi` caller identity on binary NOSASL connections, and `WithUserGroups` to set the
  groups it advertises (SPEC §3.1, §5.1).
- `WithTLS` for both the binary and HTTP transports, and `WithConnectTimeout` to bound
  dial/TLS/SASL handshake setup independently of the per-call timeout (SPEC §3.1, §3.2, §5.1).
- Catalog resolution precedence across `InCatalog`, a struct's own `CatalogName`, `WithCatalog`,
  and the `"hive"` default (SPEC §5.0); `AlterTable` now honours `newTable.CatalogName` the
  same way `CreateTable` does.
- `Table.OwnerType`, `Database.CreateTime`, and `StorageDescriptor.Skewed` (`SkewedInfo`'s
  column names and values; `skewedColValueLocationMaps` stays gated, SPEC §1.1, §5.4).
- Round-trip fidelity: a `Table`, `Partition`, or `Database` read from the server keeps an
  internal snapshot of its unmodelled generated fields, so `AlterTable`/`AlterPartitions`/
  `AlterDatabase` no longer silently reset them (SPEC §5.4).
- `Message(err)` to extract a clean exception message, and every error this package returns
  now uses it (SPEC §7).
- `WithChunkSize` to control the per-request chunk size for `GetTables` and `GetPartitionsByNames`
  (SPEC §5.1, §5.4, §5.5).
- `WithPartitionBatchSize` to control `AddPartitions`' batch size independently of
  `WithChunkSize` (SPEC §5.1, §5.5, §2.3 Rule 5).
- `WithoutUGI` to disable the binary NOSASL `set_ugi` call entirely, for a server that rejects
  it or for deliberate anonymous use (SPEC §3.1, §5.1).
- Partition lookups by name, filter, and partial values: `GetPartitionsByNames`,
  `GetPartitionsByFilter`, `GetPartitionNamesByValues` (SPEC §5.5).
- `AlterDatabase` (SPEC §5.3).
- Metastore notifications: `CurrentNotificationID`, `GetNextNotifications`, and the
  `NotificationEvent` type (SPEC §5.7).
- Read-only column statistics: `GetTableColumnStatistics` and the `ColumnStatistics`/
  `*ColumnStats`/`Decimal` types, covering all eight `ColumnStatisticsData` union arms
  including `Timestamp` (SPEC §5.8).
- Minimal ACID locks and transactions: `OpenTransaction`, `CommitTransaction`,
  `AbortTransaction`, `Heartbeat`, `Lock`, `CheckLock`, `Unlock`, and the
  `Lock*`/`LockRequest`/`LockResponse` types (SPEC §5.9).
- Kerberos authentication over binary TCP via a pure-Go SASL GSSAPI implementation
  (`gokrb5`, zero Cgo): `WithKerberos`, `WithKerberosServicePrincipal`, `WithKrb5Config`
  (SPEC §3.1, §5.1).
- Observability: `WithLogger` for connection lifecycle, failover, and recovery-probe events,
  and `WithRPCObserver` for a per-attempt `RPCInfo` hook (SPEC §5.10).
- Lakehouse helper constants: `DeltaStorageHandler`, `ParamIcebergCatalog`,
  `ParamSerializationFormat`, and `ParamPath` (SPEC §6).

### Changed

- Iceberg/Delta/Hudi table builders realigned to `xtable-hive-metastore`'s conventions so
  registered tables are readable by the same downstream engines (SPEC §6); see Breaking below.
- Error text is now consistently `"<op>: <server message>"`, via `Message(err)` (SPEC §7).
- TLS/x509 handshake failures and `TApplicationException`s in the frame-desync class now
  classify as `hms.ErrUnavailable` instead of an unclassified error.
- `ConfigValSecurityException` from `get_config_value` now maps to `hms.ErrInvalidOperation`
  instead of an unclassified error (SPEC §7).
- `AddPartitions` and `AlterPartitions` now take the database and table from the call's own
  `dbName`/`tableName` arguments; a `Partition.DatabaseName`/`TableName` no longer overrides them.
- `WithChunkSize` no longer governs `AddPartitions`' batch size; use the new
  `WithPartitionBatchSize` for that. A caller that set `WithChunkSize` only to control
  `AddPartitions` batching (e.g. to bound request size against a server-side limit) must add
  `WithPartitionBatchSize` explicitly -- `AddPartitions` now always batches at the 1000-item
  default unless it is set.
- `CreateDatabase` now caches the warehouse root it resolves for an empty `LocationURI`, per
  `Client` per catalog name, instead of re-resolving it (`get_catalog`/`get_config_value`) on
  every call (SPEC §5.3). A warehouse directory changed on the server after that is not picked
  up by a running `Client` -- construct a new one.
- Binary NOSASL connections now send `set_ugi` by default, with the current OS user, matching
  the Java `HiveMetaStoreClient` and this package's own HTTP `x-actor-username` default; `WithUser`
  still overrides the identity sent, and `WithoutUGI` (new, above) disables the call entirely.
  Previously `set_ugi` was sent only when `WithUser` was configured -- a client relying on that to
  skip `set_ugi` (e.g. against a server that rejects it) must now call `WithoutUGI` explicitly.
  `New` now performs an RPC on every newly dialed binary connection rather than just the dial
  itself, so a bare listener that accepts a connection but never speaks Thrift no longer models
  a live server in a test -- `WithoutUGI` restores the pre-0.1.0 dial-only behavior for such a
  test double. This `set_ugi` call is bounded by `WithConnectTimeout`, not the per-call
  `WithTimeout`, since it is part of connection establishment (SPEC §3.1).

### Known issues

- `govulncheck` reports GO-2026-5932 (`golang.org/x/crypto/openpgp` is unmaintained) in the
  module requirements. `golang.org/x/crypto` is required only because `gokrb5` imports its
  `md4` package; nothing here imports `openpgp`, so the symbol scan is clean, and the
  advisory has no fixed version to move to.

### Removed / Breaking

Allowed on the `v0.x` line per SPEC §8.

- `hms.DeltaInputFormat` and `hms.DeltaOutputFormat` are removed: Delta Lake tables are
  registered by storage handler only, with `Storage.InputFormat`/`OutputFormat` left empty.
- `hms.HudiOutputFormat`'s value changed from the Hudi-specific output format to
  `org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat`.
- The `SetChunkSizeForTest` test-only hook is removed; use `WithChunkSize` instead.

### Fixed

- `ServerVersion` no longer misreports Hive 2.3/3.x as unknown: it disambiguates the shared
  `"3.0"` schema-line answer by probing catalog support on the same connection (SPEC Appendix A).
- `DropDatabase` on Hive 3.1 now maps the server's bare `MetaException(NullPointerException)`
  on a missing database to `hms.ErrNotFound`, matching every other supported version
  (SPEC §7, Appendix A).
- `CreateDatabase` fills in a default `LocationURI` client-side against real servers, working
  around Hive 3.1's `get_config_value` not resolving `hive.metastore.warehouse.dir` to its
  `metastore.warehouse.dir` alias (SPEC Appendix A).
