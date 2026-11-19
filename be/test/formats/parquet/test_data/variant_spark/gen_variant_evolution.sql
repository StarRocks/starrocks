-- Generates Iceberg v3 VARIANT data files with Spark whose shredded layout changes from file to file,
-- and one file with several small row groups for pruning checks.
-- Run with gen_variant_evolution.sh (Spark 4.1 + iceberg-spark-runtime-4.1_2.13:1.12.0, Hadoop catalog `iceberg_fs`).
--
-- evo / evo_noshred: the same rows, shredded and unshredded; every INSERT writes one file over a disjoint id range.
-- Iceberg infers each file's layout from its own rows, so `$.a` ends up with a different type in each file:
--   e1_int      ids 1000-1099  a: small int (int16)         b: string
--   e2_bigint   ids 2000-2099  a: int64 above 2^53          c: double            (b removed, c added)
--   e3_double   ids 3000-3099  a: double                    b: int               (b changes type)
--   e4_array    ids 4000-4099  a: array<int>                b: string
--   e5_string   ids 5000-5099  a: string
--   e6_object   ids 6000-6099  a: object {x: int}
--   e7_mixed    ids 7000-7099  a: int / string / double mixed in the file, not shredded
--
-- prune: ids 0-2999 sorted by id, written in row groups of about 800 rows and pages of 100 rows.
--   v = {"n": id, "s": "s<id, 6 digits>", "m": id when id % 10 != 0, else missing, "g": id / 1000}
--   `$.n` is shredded as int8 (inferred from the first rows), so values above 127 stay in the fallback `value`.
-- prune_int16: the same row groups with v = {"w": 1000 + id}; every value fits int16, so `$.w` is shredded as int16
--   and its fallback `value` is null everywhere.

SET spark.sql.session.timeZone=UTC;

CREATE NAMESPACE IF NOT EXISTS iceberg_fs.variant_spark_gen;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.evo PURGE;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.evo_noshred PURGE;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.prune PURGE;
DROP TABLE IF EXISTS iceberg_fs.variant_spark_gen.prune_int16 PURGE;

CREATE TABLE iceberg_fs.variant_spark_gen.evo (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true');
CREATE TABLE iceberg_fs.variant_spark_gen.evo_noshred (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='false');
CREATE TABLE iceberg_fs.variant_spark_gen.prune (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true',
               'write.parquet.row-group-size-bytes'='8192', 'write.parquet.page-row-limit'='100');
CREATE TABLE iceberg_fs.variant_spark_gen.prune_int16 (id INT, v VARIANT) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true',
               'write.parquet.row-group-size-bytes'='8192', 'write.parquet.page-row-limit'='100');

CREATE OR REPLACE TEMPORARY VIEW e1 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":%d,"b":"b%d"}', id - 1000, id)) AS v FROM range(1000, 1100);
CREATE OR REPLACE TEMPORARY VIEW e2 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":%d,"c":%d.5}', 9007199254740993 + id, id)) AS v FROM range(2000, 2100);
CREATE OR REPLACE TEMPORARY VIEW e3 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":%d.25e0,"b":%d}', id, id)) AS v FROM range(3000, 3100);
CREATE OR REPLACE TEMPORARY VIEW e4 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":[%d,%d],"b":"b%d"}', id, id + 1, id)) AS v FROM range(4000, 4100);
CREATE OR REPLACE TEMPORARY VIEW e5 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":"a%d"}', id)) AS v FROM range(5000, 5100);
CREATE OR REPLACE TEMPORARY VIEW e6 AS
SELECT CAST(id AS INT) AS id, parse_json(format_string('{"a":{"x":%d}}', id)) AS v FROM range(6000, 6100);
CREATE OR REPLACE TEMPORARY VIEW e7 AS
SELECT CAST(id AS INT) AS id,
       CASE id % 3 WHEN 0 THEN parse_json(format_string('{"a":%d}', id))
                   WHEN 1 THEN parse_json(format_string('{"a":"a%d"}', id))
                   ELSE parse_json(format_string('{"a":%d.5e0}', id)) END AS v
FROM range(7000, 7100);

INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e1 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e2 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e3 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e4 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e5 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e6 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo SELECT /*+ REPARTITION(1) */ id, v FROM e7 ORDER BY id;

INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e1 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e2 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e3 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e4 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e5 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e6 ORDER BY id;
INSERT INTO iceberg_fs.variant_spark_gen.evo_noshred SELECT /*+ REPARTITION(1) */ id, v FROM e7 ORDER BY id;

INSERT INTO iceberg_fs.variant_spark_gen.prune
SELECT /*+ REPARTITION(1) */ CAST(id AS INT),
       parse_json(CASE WHEN id % 10 = 0 THEN format_string('{"n":%d,"s":"s%06d","g":%d}', id, id, id DIV 1000)
                       ELSE format_string('{"n":%d,"s":"s%06d","m":%d,"g":%d}', id, id, id, id DIV 1000) END)
FROM range(0, 3000) ORDER BY id;

INSERT INTO iceberg_fs.variant_spark_gen.prune_int16
SELECT /*+ REPARTITION(1) */ CAST(id AS INT), parse_json(format_string('{"w":%d}', 1000 + id))
FROM range(0, 3000) ORDER BY id;
