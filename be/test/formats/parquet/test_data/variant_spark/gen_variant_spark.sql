-- Generates Iceberg v3 VARIANT data files with Spark, shredded and unshredded, from the same rows.
-- Run with gen_variant_spark.sh (Spark 4.1 + iceberg-spark-runtime-4.1_2.13:1.12.0, Hadoop catalog `iceberg_fs`).
-- Every INSERT writes exactly one data file (REPARTITION(1)); each file covers a disjoint id range,
-- which gen_variant_spark.sh uses to name the files:
--   01_typed      ids   1-6    objects with typed scalars, nested objects, arrays, arrays of objects
--   02_conflict   ids   7-10   the same paths with other types than in 01_typed, mixed within the file
--   03_nulls      ids  11-16   variant null, SQL NULL, empty object, missing fields, nulls inside objects/arrays
--   04_scalars    ids  17-21   top-level values that are not objects
--   05_inference  ids 101-250  rows after the inference buffer (100 rows) change the type of `i` and add `x`
--   06_top_int    ids 301-405  top-level ints shredded at the root; strings and a null after the buffer stay residual

SET spark.sql.session.timeZone=UTC;

CREATE NAMESPACE IF NOT EXISTS iceberg_fs.variant_spark_gen;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.shred PURGE;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.noshred PURGE;

CREATE TABLE iceberg_fs.variant_spark_gen.shred (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true');
CREATE TABLE iceberg_fs.variant_spark_gen.noshred (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='false');

-- 01_typed: typed scalars keep their variant type (int8/int64, decimal, double, bool, timestamp, timestamp_ntz, date).
CREATE OR REPLACE TEMPORARY VIEW f01 AS
SELECT 1 AS id, to_variant_object(named_struct(
    'i', CAST(1 AS BIGINT), 'i8', CAST(5 AS TINYINT), 'dec', CAST(12.345 AS DECIMAL(10,3)), 'dbl', CAST(1.25 AS DOUBLE),
    's', 'alpha', 'b', true, 't', TIMESTAMP'2024-01-02 03:04:05.123456', 'tn', TIMESTAMP_NTZ'2024-01-02 03:04:05',
    'd', DATE'2024-01-02', 'o', named_struct('x', 1, 'y', 'p', 'z', named_struct('w', 10)),
    'arr', array(1, 2, 3), 'ao', array(named_struct('k', 1, 's', 'a'), named_struct('k', 2, 's', 'b')))) AS v
UNION ALL
SELECT 2, to_variant_object(named_struct(
    'i', CAST(2 AS BIGINT), 'i8', CAST(-5 AS TINYINT), 'dec', CAST(-0.001 AS DECIMAL(10,3)), 'dbl', CAST(-0.5 AS DOUBLE),
    's', 'beta', 'b', false, 't', TIMESTAMP'1969-12-31 23:59:59.999999', 'tn', TIMESTAMP_NTZ'1970-01-01 00:00:00',
    'd', DATE'1970-01-01', 'o', named_struct('x', 2, 'y', 'q', 'z', named_struct('w', 20)),
    'arr', array(4), 'ao', array(named_struct('k', 3, 's', 'c'))))
UNION ALL
SELECT 3, to_variant_object(named_struct(
    'i', CAST(9223372036854775807 AS BIGINT), 'i8', CAST(127 AS TINYINT), 'dec', CAST(9999999.999 AS DECIMAL(10,3)),
    'dbl', CAST(1e300 AS DOUBLE), 's', '中文 字符串', 'b', true, 't', TIMESTAMP'2262-04-11 23:47:16.854775',
    'tn', TIMESTAMP_NTZ'9999-12-31 23:59:59.999999', 'd', DATE'9999-12-31',
    'o', named_struct('x', -3, 'y', '', 'z', named_struct('w', 0)),
    'arr', CAST(array() AS ARRAY<INT>), 'ao', CAST(array() AS ARRAY<STRUCT<k: INT, s: STRING>>)))
UNION ALL
SELECT 4, to_variant_object(named_struct(
    'i', CAST(-9223372036854775808 AS BIGINT), 'i8', CAST(-128 AS TINYINT), 'dec', CAST(0 AS DECIMAL(10,3)),
    'dbl', CAST(0 AS DOUBLE), 's', repeat('long', 64), 'b', false, 't', TIMESTAMP'2000-02-29 12:00:00',
    'tn', TIMESTAMP_NTZ'2000-02-29 12:00:00', 'd', DATE'2000-02-29',
    'o', named_struct('x', 4, 'y', 'r', 'z', named_struct('w', 40)),
    'arr', array(-1, 0, 1), 'ao', array(named_struct('k', 4, 's', 'd'), named_struct('k', 5, 's', 'e'))))
UNION ALL
SELECT 5, parse_json('{"i":5,"i8":6,"dec":3.14,"dbl":2.5e0,"s":"gamma","b":true,"o":{"x":5,"y":"s","z":{"w":50}},"arr":[7,8],"ao":[{"k":6,"s":"f"}]}')
UNION ALL
SELECT 6, parse_json('{"o":{"z":{"w":60},"x":6,"y":"t"},"i":6,"s":"delta"}');

-- 02_conflict: the same paths with other types; `i` mixes string and int inside the file.
CREATE OR REPLACE TEMPORARY VIEW f02 AS
SELECT 7 AS id, parse_json('{"i":"seven","s":7,"dec":"1.5","b":1,"o":[1,2],"arr":{"k":1},"ao":"none"}') AS v
UNION ALL SELECT 8, parse_json('{"i":"eight","s":8.5,"dec":1.5e0,"b":"true","o":{"x":"eight","z":{"w":"w8"}},"arr":[1,"two",{"three":3}]}')
UNION ALL SELECT 9, parse_json('{"i":9,"s":null,"dec":9,"o":{"x":9.5,"z":[9]},"arr":[[1,2],[3]]}')
UNION ALL SELECT 10, parse_json('{"i":10,"s":"ten","o":{"x":10,"z":{"w":100,"extra":true}},"arr":[10]}');

-- 03_nulls: variant null, SQL NULL, empty object, missing fields, nulls inside objects and arrays.
CREATE OR REPLACE TEMPORARY VIEW f03 AS
SELECT 11 AS id, parse_json('null') AS v
UNION ALL SELECT 12, CAST(NULL AS VARIANT)
UNION ALL SELECT 13, parse_json('{}')
UNION ALL SELECT 14, parse_json('{"i":14}')
UNION ALL SELECT 15, parse_json('{"i":null,"s":null,"o":{"x":null,"z":null}}')
UNION ALL SELECT 16, parse_json('{"i":16,"arr":[1,null,3],"ao":[{"k":1},null,{"s":"z"}],"o":{}}');

-- 04_scalars: top-level values that are not objects.
CREATE OR REPLACE TEMPORARY VIEW f04 AS
SELECT 17 AS id, parse_json('17') AS v
UNION ALL SELECT 18, parse_json('"eighteen"')
UNION ALL SELECT 19, parse_json('[1,"a",{"x":1}]')
UNION ALL SELECT 20, parse_json('3.5')
UNION ALL SELECT 21, CAST(TIMESTAMP'2024-05-06 07:08:09' AS VARIANT);

-- 06_top_int: the root itself is shredded as an int (inferred from the first 100 rows);
-- the strings and the variant null after the buffer stay in the residual value.
CREATE OR REPLACE TEMPORARY VIEW f06 AS
SELECT CAST(id AS INT) AS id,
       CASE WHEN id <= 400 THEN parse_json(CAST(id * (CASE WHEN id % 2 = 0 THEN 1 ELSE -1 END) AS STRING))
            WHEN id <= 404 THEN parse_json(format_string('"s%d"', id))
            ELSE parse_json('null') END AS v
FROM range(301, 406);

-- 05_inference: shredding is inferred from the first 100 buffered rows (i: int, s: string);
-- the last 50 rows store `i` as a string and add `x`, which then live only in the residual `value`.
CREATE OR REPLACE TEMPORARY VIEW f05 AS
SELECT CAST(id AS INT) AS id,
       CASE WHEN id <= 200 THEN parse_json(format_string('{"i":%d,"s":"r%d"}', id, id))
            ELSE parse_json(format_string('{"i":"str%d","x":%d}', id, id)) END AS v
FROM range(101, 251);

INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f01;
INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f02;
INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f03;
INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f04;
INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f05 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.shred SELECT /*+ REPARTITION(1) */ id, v FROM f06 ORDER BY id;

INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f01;
INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f02;
INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f03;
INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f04;
INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f05 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.noshred SELECT /*+ REPARTITION(1) */ id, v FROM f06 ORDER BY id;
