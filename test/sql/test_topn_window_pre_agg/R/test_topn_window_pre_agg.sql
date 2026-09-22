-- name: test_topn_window_pre_agg
DROP TABLE IF EXISTS t_topn_pre_agg;
-- result:
-- !result
CREATE TABLE t_topn_pre_agg (
  g INT NOT NULL,
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(g, k)
DISTRIBUTED BY HASH(g) BUCKETS 3
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_topn_pre_agg (g, k, v) VALUES
  (1, 1, 10), (1, 2, 20), (1, 3, 30),
  (2, 1, 100), (2, 2, 200),
  (3, 1, 5), (3, 2, 6), (3, 3, 7), (3, 4, 8);
-- result:
-- !result
function: assert_explain_contains('SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k', 'PARTITION-TOP-N')
-- result:
None
-- !result
function: assert_explain_contains('SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k', 'pre agg functions')
-- result:
None
-- !result
SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k;
-- result:
1	1	10	1	60
1	2	20	2	60
2	1	100	1	300
2	2	200	2	300
3	1	5	1	26
3	2	6	2	26
-- !result
set enable_push_down_pre_agg_with_rank = false;
-- result:
-- !result
SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k;
-- result:
1	1	10	1	60
1	2	20	2	60
2	1	100	1	300
2	2	200	2	300
3	1	5	1	26
3	2	6	2	26
-- !result
set enable_push_down_pre_agg_with_rank = true;
-- result:
-- !result
SELECT g, count(*) AS n, max(s) AS total FROM (SELECT g, row_number() OVER (PARTITION BY g ORDER BY v) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 GROUP BY g ORDER BY g;
-- result:
1	2	60
2	2	300
3	2	26
-- !result
DROP TABLE t_topn_pre_agg;
-- result:
-- !result
