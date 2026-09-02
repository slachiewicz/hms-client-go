# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project has not yet tagged
a release, so everything below is unreleased.

## [Unreleased]

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
- `WithChunkSize` to control the per-request chunk size for `GetTables`, `AddPartitions`, and
  `GetPartitionsByNames` (SPEC §5.1, §5.4, §5.5).
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

### Changed

- Iceberg/Delta/Hudi table builders realigned to `xtable-hive-metastore`'s conventions so
  registered tables are readable by the same downstream engines (SPEC §6); see Breaking below.
- Error text is now consistently `"<op>: <server message>"`, via `Message(err)` (SPEC §7).
- TLS/x509 handshake failures and `TApplicationException`s in the frame-desync class now
  classify as `hms.ErrUnavailable` instead of an unclassified error.
- `ConfigValSecurityException` from `get_config_value` now maps to `hms.ErrInvalidOperation`
  instead of an unclassified error (SPEC §7).

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
