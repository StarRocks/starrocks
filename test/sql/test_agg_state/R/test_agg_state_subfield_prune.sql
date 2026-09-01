-- name: test_agg_state_subfield_prune
SET cbo_prune_subfield = true;
-- result:
-- !result
CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 2000)) g1, TABLE(generate_series(1, 40)) g2;
-- result:
-- !result
CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO st SELECT k, array_agg_distinct_state(s) FROM src;
-- result:
-- !result
INSERT INTO st SELECT k, array_agg_distinct_state(s) FROM src;
-- result:
-- !result
SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM st;
-- result:
2000	80000
-- !result
SELECT count(*) AS n_full_arrays FROM st WHERE array_length(v) = 40;
-- result:
2000
-- !result
SELECT array_length(v) FROM st WHERE k = 1;
-- result:
40
-- !result
SELECT sum(cardinality(v)) AS total_elems FROM st;
-- result:
80000
-- !result
SELECT array_length(v), v[1] IS NOT NULL FROM st WHERE k = 1;
-- result:
40	1
-- !result
CREATE TABLE plain (k INT, v ARRAY<VARCHAR(100)>)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO plain SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;
-- result:
-- !result
SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM plain;
-- result:
2000	80000
-- !result
CREATE TABLE repl (k INT, v ARRAY<VARCHAR(100)> REPLACE)
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO repl SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;
-- result:
-- !result
INSERT INTO repl SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;
-- result:
-- !result
SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM repl;
-- result:
2000	80000
-- !result