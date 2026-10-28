-- name: test_agg_state_scalar_merge_mempool
CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 10000)) g1, TABLE(generate_series(1, 40)) g2;
-- result:
-- !result
CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO st SELECT k, array_agg_distinct_combine(s) FROM src GROUP BY k;
-- result:
-- !result
SET pipeline_dop = 8;
-- result:
-- !result
INSERT INTO blackhole() SELECT array_agg_distinct_state_merge(v) FROM st;
-- result:
-- !result
SELECT count(*) AS n, sum(array_length(array_agg_distinct_state_merge(v))) AS total_elems
FROM st;
-- result:
10000	400000
-- !result
