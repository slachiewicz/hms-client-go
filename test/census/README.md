# Metastore RPC census

Which Hive Metastore RPCs Spark, Trino and Flink issue for ordinary DDL and
DML, recorded by `internal/cmd/hmsrecord`, a proxy in front of a real
metastore. The list is the requirement set for a lightweight Go metastore
that can stand in for the Java images.

Run it with the `census` workflow (`.github/workflows/census.yml`, manual
dispatch). Each job uploads its JSON-lines call log and a per-method count.
The statements each engine runs are in this directory: `spark.sql`,
`trino.sql`, `flink.sql`.

## Results

Recorded 2026-10-11, run 38143741025, against `apache/hive:3.1.3` and
`apache/hive:4.2.1` (column suffix `/ 3` and `/ 4`):

- Spark 3.5.9 and 4.2.0. Against Hive 3.1.3, Spark's built-in Hive 2.3 client.
  Against Hive 4.2.1 that client fails on its first table read
  (`Invalid method name: 'get_table'`), so those legs set
  `spark.sql.hive.metastore.version` to the newest client each line supports:
  3.1.3 for Spark 3.5, 4.0.1 for Spark 4.2.
- Trino 483, Hive connector. Against Hive 4.2.1 it needs
  `hive.non-managed-table-writes-enabled=true`: Hive 4 turns a
  non-transactional managed table into an external one, and Trino refuses to
  write to it otherwise.
- Flink 1.20.5 with `flink-sql-connector-hive-3.1.3` 1.20.5 (Hive 3.1.3
  client) on both. Flink 2.x has no released Hive connector.

Counts are calls per run. The last column says whether `hmstest` already
implements the call.

| RPC | spark-3.5 / 3 | spark-3.5 / 4 | spark-4.2 / 3 | spark-4.2 / 4 | trino / 3 | trino / 4 | flink / 3 | flink / 4 | hmstest |
|---|---|---|---|---|---|---|---|---|---|
| `add_partition_with_environment_context` |  |  |  |  |  |  | 2 | 2 | no |
| `add_partitions` |  |  |  |  | 1 | 1 |  |  | no |
| `add_partitions_req` | 3 | 3 | 3 | 3 |  |  |  |  | yes |
| `alter_table_req` |  |  |  | 6 |  |  |  |  | no |
| `alter_table_with_environment_context` | 6 | 6 | 6 |  | 7 | 7 | 3 | 3 | no |
| `create_database` | 1 | 1 | 1 |  | 1 | 1 | 1 | 1 | yes |
| `create_database_req` |  |  |  | 1 |  |  |  |  | no |
| `create_function` | 1 | 1 | 1 | 1 |  |  | 1 | 1 | no |
| `create_table` |  |  |  |  | 3 | 3 |  |  | yes |
| `create_table_req` |  |  |  | 4 |  |  |  |  | no |
| `create_table_with_environment_context` | 4 | 4 | 4 |  |  |  | 3 | 3 | no |
| `drop_database` | 1 | 1 | 1 |  | 1 | 1 | 1 | 1 | yes |
| `drop_database_req` |  |  |  | 1 |  |  |  |  | no |
| `drop_function` | 1 | 1 | 1 | 1 |  |  | 1 | 1 | no |
| `drop_partition` |  |  |  |  | 1 | 1 |  |  | yes |
| `drop_partition_req` |  |  |  | 1 |  |  |  |  | no |
| `drop_partition_with_environment_context` | 1 | 1 | 1 |  |  |  |  |  | no |
| `drop_table` |  |  |  |  | 3 | 3 |  |  | yes |
| `drop_table_req` |  |  |  | 4 |  |  |  |  | no |
| `drop_table_with_environment_context` | 4 | 4 | 4 |  |  |  | 3 | 3 | no |
| `fetch_partition_names_req` |  |  |  | 1 |  |  |  |  | no |
| `get_all_databases` |  |  |  |  | 1 | 1 |  |  | yes |
| `get_all_tables` | 1 | 1 | 1 | 1 |  |  | 4 | 4 | yes |
| `get_database` | 121 | 119 | 81 |  | 5 | 5 | 42 | 42 | yes |
| `get_database_req` |  |  |  | 79 |  |  |  |  | no |
| `get_databases` | 1 | 1 | 1 | 1 |  |  | 4 | 4 | yes |
| `get_function` | 7 | 7 | 4 | 4 |  |  | 3 | 3 | no |
| `get_functions` | 1 | 1 | 1 | 1 |  |  | 1 | 1 | no |
| `get_not_null_constraints` |  |  |  |  |  |  | 18 | 18 | no |
| `get_part_specs_by_filter` |  |  |  |  |  |  | 1 | 1 | no |
| `get_partition` |  |  |  |  | 4 | 4 | 6 | 6 | no |
| `get_partition_names` | 1 | 1 | 1 |  | 2 | 2 | 1 | 1 | yes |
| `get_partition_names_ps` |  |  |  |  | 2 | 2 |  |  | yes |
| `get_partition_names_ps_req` |  |  |  | 2 |  |  |  |  | yes |
| `get_partitions_by_filter` | 1 | 1 | 1 |  |  |  |  |  | yes |
| `get_partitions_by_filter_req` |  |  |  | 1 |  |  |  |  | no |
| `get_partitions_by_names` |  |  |  |  | 1 | 1 | 2 | 2 | yes |
| `get_partitions_ps_with_auth` | 2 | 2 | 2 |  |  |  |  |  | no |
| `get_partitions_ps_with_auth_req` |  |  |  | 1 |  |  |  |  | no |
| `get_partitions_statistics_req` |  |  |  |  |  |  | 1 | 1 | no |
| `get_primary_keys` |  |  |  |  |  |  | 18 | 18 | no |
| `get_table` | 167 |  | 108 |  |  |  |  |  | not in 4.2.1 IDL |
| `get_table_meta` |  |  |  |  | 2 | 2 |  |  | no |
| `get_table_objects_by_name_req` |  |  |  | 3 |  |  |  |  | yes |
| `get_table_req` |  | 167 |  | 107 | 33 | 33 | 66 | 66 | yes |
| `get_table_statistics_req` |  |  |  |  | 6 | 6 | 9 | 9 | yes |
| `get_tables` | 2 | 2 | 2 |  |  |  |  |  | no |
| `get_tables_by_type` |  | 1 |  | 1 |  |  | 1 | 1 | no |
| `set_ugi` | 3 | 3 | 3 | 3 |  |  | 10 | 10 | yes |
| `shutdown` | 1 | 1 | 1 | 2 |  |  | 10 | 10 | not in 4.2.1 IDL |
| `update_partition_column_statistics` |  |  |  |  | 2 | 2 |  |  | no |
| `update_table_column_statistics` |  |  |  |  | 2 | 2 |  |  | no |

52 distinct RPCs; `hmstest` implements 19 of them.

Spark 4.2 on the Hive 4 client moves to the request-object API: every
database, table and partition call becomes its `*_req` form
(`create_table_req`, `get_database_req`, `get_partitions_by_filter_req`,
`fetch_partition_names_req`, ...). Spark 3.5 on the Hive 3.1 client only
swaps `get_table` for `get_table_req`. Trino and Flink issue the same calls
against both metastores.

## What the results mean for a Go metastore

- **Legacy `get_table`.** Spark's built-in Hive 2.3 client reads every table
  through `get_table`, which the Hive 4.2.1 IDL this repository generates
  from no longer declares and a real Hive 4 metastore no longer serves. A
  server that serves it, through a hand-written processor next to the
  generated ones, lets out-of-the-box Spark work where a Hive 4 metastore
  does not.
- **Three generations of the same call.** Depending on the client, the same
  operation arrives as the plain form, the `*_with_environment_context`
  form, or the Hive 4 `*_req` form. A server that serves all three
  generations serves every client in the census.
- **`shutdown`** is the fb303 one-way call a Hive client sends when it
  closes. Accept it and do nothing.
- **Engine-specific groups:** functions (`get_function`, `get_functions`,
  `create_function`, `drop_function`) for Spark and Flink; constraints
  (`get_primary_keys`, `get_not_null_constraints`) for Flink; column
  statistics updates for Trino.

## Not covered yet

- A Hive 2.3 metastore.
- ACID and transactional tables, and the lock and transaction RPCs.
- Iceberg tables registered in the metastore, from Spark and Trino.
- SASL (PLAIN, Kerberos) and the HTTP transport. `hmsrecord` proxies such
  connections without recording their calls.

