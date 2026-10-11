-- Exercised by .github/workflows/census.yml against a recording proxy, to
-- learn which metastore RPCs Spark issues for ordinary DDL and DML.
CREATE DATABASE IF NOT EXISTS census;
USE census;
SHOW DATABASES;

CREATE TABLE plain (id INT, name STRING) USING parquet;
INSERT INTO plain VALUES (1, 'a'), (2, 'b');
SELECT * FROM plain;
ANALYZE TABLE plain COMPUTE STATISTICS;
ANALYZE TABLE plain COMPUTE STATISTICS FOR COLUMNS id;
ALTER TABLE plain SET TBLPROPERTIES ('census' = 'yes');
ALTER TABLE plain ADD COLUMNS (extra STRING);
DESCRIBE TABLE EXTENDED plain;

CREATE TABLE parted (id INT, dt STRING) USING parquet PARTITIONED BY (dt);
INSERT INTO parted VALUES (1, '2026-01-01'), (2, '2026-01-02');
SELECT * FROM parted WHERE dt = '2026-01-01';
SHOW PARTITIONS parted;
ALTER TABLE parted ADD PARTITION (dt = '2026-01-03');
ALTER TABLE parted DROP PARTITION (dt = '2026-01-01');
MSCK REPAIR TABLE parted;

CREATE TABLE serde (id INT) STORED AS ORC;
INSERT INTO serde VALUES (1);
SELECT count(*) FROM serde;

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
DROP TABLE serde;
DROP DATABASE census CASCADE;
