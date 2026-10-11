-- Exercised by .github/workflows/census.yml against a recording proxy, to
-- learn which metastore RPCs Trino's Hive connector issues.
CREATE SCHEMA hive.census;
SHOW SCHEMAS FROM hive;

CREATE TABLE hive.census.plain (id integer, name varchar) WITH (format = 'PARQUET');
INSERT INTO hive.census.plain VALUES (1, 'a'), (2, 'b');
SELECT * FROM hive.census.plain;
ANALYZE hive.census.plain;
SHOW STATS FOR hive.census.plain;
ALTER TABLE hive.census.plain ADD COLUMN extra varchar;
COMMENT ON TABLE hive.census.plain IS 'census';
SHOW CREATE TABLE hive.census.plain;

CREATE TABLE hive.census.parted (id integer, dt varchar) WITH (format = 'ORC', partitioned_by = ARRAY['dt']);
INSERT INTO hive.census.parted VALUES (1, '2026-01-01'), (2, '2026-01-02');
SELECT * FROM hive.census.parted WHERE dt = '2026-01-01';
SELECT * FROM hive.census."parted$partitions";
DELETE FROM hive.census.parted WHERE dt = '2026-01-01';
CALL hive.system.sync_partition_metadata('census', 'parted', 'FULL');

CREATE VIEW hive.census.v AS SELECT id FROM hive.census.plain;
SELECT * FROM hive.census.v;
SHOW TABLES FROM hive.census;
ALTER TABLE hive.census.plain RENAME TO hive.census.plain2;

DROP VIEW hive.census.v;
DROP TABLE hive.census.plain2;
DROP TABLE hive.census.parted;
DROP SCHEMA hive.census;
