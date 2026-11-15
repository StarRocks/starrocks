-- name: test_agg_state_const_arg
CREATE TABLE t_agg_state_const (
  k1 VARCHAR(10),
  v_avg avg(bigint),
  v_sum sum(bigint),
  v_min min(bigint),
  v_max max(bigint),
  v_count count(bigint)
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_agg_state_const VALUES
  ('a', avg_state(cast(38 as bigint)), sum_state(cast(38 as bigint)), min_state(cast(38 as bigint)), max_state(cast(38 as bigint)), count_state(cast(38 as bigint))),
  ('b', avg_state(cast(50 as bigint)), sum_state(cast(50 as bigint)), min_state(cast(50 as bigint)), max_state(cast(50 as bigint)), count_state(cast(50 as bigint)));
-- result:
-- !result
SELECT k1, avg_merge(v_avg), sum_merge(v_sum), min_merge(v_min), max_merge(v_max), count_merge(v_count)
FROM t_agg_state_const GROUP BY k1 ORDER BY k1;
-- result:
a	38.0	38	38	38	1
b	50.0	50	50	50	1
-- !result
CREATE TABLE t_src (k1 VARCHAR(10), pv BIGINT)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_src VALUES ('a', 38), ('b', 50);
-- result:
-- !result
CREATE TABLE t_agg_state_col (
  k1 VARCHAR(10),
  v_avg avg(bigint),
  v_sum sum(bigint),
  v_min min(bigint),
  v_max max(bigint),
  v_count count(bigint)
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_agg_state_col
SELECT k1, avg_state(pv), sum_state(pv), min_state(pv), max_state(pv), count_state(pv) FROM t_src;
-- result:
-- !result
SELECT k1, avg_merge(v_avg), sum_merge(v_sum), min_merge(v_min), max_merge(v_max), count_merge(v_count)
FROM t_agg_state_col GROUP BY k1 ORDER BY k1;
-- result:
a	38.0	38	38	38	1
b	50.0	50	50	50	1
-- !result
SELECT avg_merge(avg_state(cast(38 as bigint))), sum_merge(sum_state(cast(38 as bigint))), min_merge(min_state(cast(38 as bigint))), max_merge(max_state(cast(38 as bigint))), count_merge(count_state(cast(38 as bigint)));
-- result:
38.0	38	38	38	1
-- !result
SELECT avg_merge(avg_state(cast(NULL as bigint)));
-- result:
None
-- !result
SELECT avg_merge(avg_state(cast(38 as bigint))), count_merge(count_state(cast(38 as bigint))) FROM t_src;
-- result:
38.0	2
-- !result
SELECT percentile_approx_merge(percentile_approx_state(cast(38 as double), 0.5)), percentile_approx_merge(percentile_approx_state(pv, 0.5)) FROM t_src;
-- result:
38.0	44.0
-- !result
CREATE TABLE t_agg_state_percentile (
  k1 VARCHAR(10),
  v_col percentile_approx(double, double),
  v_const percentile_approx(double, double),
  v_weighted percentile_approx_weighted(double, bigint, double)
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_agg_state_percentile
SELECT k1,
       percentile_approx_state(cast(pv as double), 0.5),
       percentile_approx_state(cast(38 as double), 0.5),
       percentile_approx_weighted_state(cast(pv as double), 1, 0.5)
FROM t_src;
-- result:
-- !result
INSERT INTO t_agg_state_percentile VALUES
  ('c', percentile_approx_state(cast(38 as double), 0.5), percentile_approx_state(cast(50 as double), 0.5),
        percentile_approx_weighted_state(cast(38 as double), 1, 0.5));
-- result:
-- !result
SELECT k1, percentile_approx_merge(v_col), percentile_approx_merge(v_const), percentile_approx_weighted_merge(v_weighted)
FROM t_agg_state_percentile GROUP BY k1 ORDER BY k1;
-- result:
a	38.0	38.0	38.0
b	50.0	38.0	50.0
c	38.0	50.0	38.0
-- !result