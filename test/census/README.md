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

Recorded 2026-10-11, run 38137289850, against `apache/hive:3.1.3`:

- Spark 3.5.9 and 4.1.3, through Spark's built-in Hive 2.3 client.
- Trino 483, Hive connector.
- Flink 1.20.4, `flink-sql-connector-hive-3.1.3` 1.20.4.

Counts are calls per run. The last column says whether `hmstest` already
implements the call.

| RPC | flink | spark-3.5 | spark-4.1 | trino | hmstest |
|---|---|---|---|---|---|
| `add_partition_with_environment_context` | 2 |  |  |  | no |
| `add_partitions` |  |  |  | 1 | no |
| `add_partitions_req` |  | 3 | 3 |  | yes |
| `alter_table_with_environment_context` | 3 | 6 | 6 | 7 | no |
| `create_database` | 1 | 1 | 1 | 1 | yes |
| `create_function` | 1 | 1 | 1 |  | no |
| `create_table` |  |  |  | 3 | yes |
| `create_table_with_environment_context` | 3 | 4 | 4 |  | no |
| `drop_database` | 1 | 1 | 1 | 1 | yes |
| `drop_function` | 1 | 1 | 1 |  | no |
| `drop_partition` |  |  |  | 1 | yes |
| `drop_partition_with_environment_context` |  | 1 | 1 |  | no |
| `drop_table` |  |  |  | 3 | yes |
| `drop_table_with_environment_context` | 3 | 4 | 4 |  | no |
| `get_all_databases` |  |  |  | 1 | yes |
| `get_all_tables` | 4 | 1 | 1 |  | yes |
| `get_database` | 42 | 121 | 145 | 5 | yes |
| `get_databases` | 4 | 1 | 1 |  | yes |
| `get_function` | 3 | 7 | 7 |  | no |
| `get_functions` | 1 | 1 | 1 |  | no |
| `get_not_null_constraints` | 18 |  |  |  | no |
| `get_part_specs_by_filter` | 1 |  |  |  | no |
| `get_partition` | 6 |  |  | 4 | no |
| `get_partition_names` | 1 | 1 | 1 | 2 | yes |
| `get_partition_names_ps` |  |  |  | 2 | yes |
| `get_partitions_by_filter` |  | 1 | 1 |  | yes |
| `get_partitions_by_names` | 2 |  |  | 1 | yes |
| `get_partitions_ps_with_auth` |  | 2 | 2 |  | no |
| `get_partitions_statistics_req` | 1 |  |  |  | no |
| `get_primary_keys` | 18 |  |  |  | no |
| `get_table` |  | 167 | 164 |  | not in 4.2.1 IDL |
| `get_table_meta` |  |  |  | 2 | no |
| `get_table_req` | 66 |  |  | 33 | yes |
| `get_table_statistics_req` | 9 |  |  | 6 | yes |
| `get_tables` |  | 2 | 2 |  | no |
| `get_tables_by_type` | 1 |  |  |  | no |
| `set_ugi` | 10 | 3 | 3 |  | yes |
| `shutdown` | 10 | 1 | 1 |  | not in 4.2.1 IDL |
| `update_partition_column_statistics` |  |  |  | 2 | no |
| `update_table_column_statistics` |  |  |  | 2 | no |

40 distinct RPCs; `hmstest` implements 17 of them.

## What the results mean for a Go metastore

- **Legacy `get_table`.** Spark's built-in Hive 2.3 client reads every table
  through `get_table`, which the Hive 4.2.1 IDL this repository generates
  from no longer declares. A server must register a hand-written processor
  for it next to the generated ones, or Spark cannot read a single table.
- **`shutdown`** is the fb303 one-way call a Hive client sends when it
  closes. Accept it and do nothing.
- **Writes go through `*_with_environment_context`** on Spark and Flink,
  and through the plain forms on Trino; a server needs both.
- **Engine-specific groups:** functions (`get_function`, `get_functions`,
  `create_function`, `drop_function`) for Spark and Flink; constraints
  (`get_primary_keys`, `get_not_null_constraints`) for Flink; column
  statistics updates for Trino.

## Not covered yet

- A Hive 4 metastore, and a Hive 2.3 one.
- ACID and transactional tables, and the lock and transaction RPCs.
- Iceberg tables registered in the metastore, from Spark and Trino.
- SASL (PLAIN, Kerberos) and the HTTP transport. `hmsrecord` proxies such
  connections without recording their calls.

