-- Exercised by .github/workflows/census.yml against a recording proxy, to
-- learn which metastore RPCs Flink's HiveCatalog issues on a Hive 4 client
-- (apache/flink-connector-hive PR #37).
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.dml-sync' = 'true';

CREATE CATALOG hive WITH ('type' = 'hive', 'hive-conf-dir' = '/census/flink-hive-conf', 'hive-version' = '4.0.0');
USE CATALOG hive;
CREATE DATABASE census;
USE census;
SHOW DATABASES;

CREATE TABLE plain (id INT, name STRING) WITH ('connector' = 'hive');
INSERT INTO plain VALUES (1, 'a'), (2, 'b');
SELECT * FROM plain;
ALTER TABLE plain SET ('census' = 'yes');

CREATE TABLE parted (id INT, dt STRING) PARTITIONED BY (dt) WITH ('connector' = 'hive');
INSERT INTO parted VALUES (1, '2026-01-01'), (2, '2026-01-02');
SELECT * FROM parted WHERE dt = '2026-01-01';
SHOW PARTITIONS parted;

CREATE VIEW v AS SELECT id FROM plain;
SELECT * FROM v;
SHOW TABLES;
ALTER TABLE plain RENAME TO plain2;

CREATE FUNCTION census_upper AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';
SHOW USER FUNCTIONS;
DROP FUNCTION census_upper;

DROP VIEW v;
DROP TABLE plain2;
DROP TABLE parted;
USE `default`;
DROP DATABASE census CASCADE;
