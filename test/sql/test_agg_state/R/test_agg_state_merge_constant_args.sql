-- name: test_agg_state_merge_constant_args
CREATE TABLE t_merge_const (k INT, g INT, v INT, x DOUBLE, w BIGINT)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_merge_const
SELECT generate_series, generate_series % 4, (generate_series * 7919) % 1009, (generate_series * 7919) % 10007,
       generate_series % 3 + 1
FROM TABLE(generate_series(1, 4000));
-- result:
-- !result
SELECT g, percentile_approx(x, 0.9, 2048), percentile_approx_weighted(x, w, 0.9, 2048),
       percentile_approx(x, [0.5, 0.9], 2048)
FROM t_merge_const GROUP BY g ORDER BY g;
-- result:
0	9043.5	9003.3330078125	[5001.5,9043.5]
1	8995.5	8984.25	[5029.5,8995.5]
2	8984.5	9022.69921875	[5019.5,8984.5]
3	8995.0	9051.83984375	[4992,8995]
-- !result
SELECT g, percentile_approx_state_merge(percentile_approx_combine(x, 0.9, 2048)),
       percentile_approx_weighted_state_merge(percentile_approx_weighted_combine(x, w, 0.9, 2048)),
       percentile_approx_state_merge(percentile_approx_combine(x, [0.5, 0.9], 2048))
FROM t_merge_const GROUP BY g ORDER BY g;
-- result:
0	9043.5	9003.3330078125	[5001.5,9043.5]
1	8995.5	8984.25	[5029.5,8995.5]
2	8984.5	9022.69921875	[5019.5,8984.5]
3	8995.0	9051.83984375	[4992,8995]
-- !result
WITH p AS (
    SELECT g, k % 3 = 0 AS part,
           percentile_approx_combine(x, 0.9, 2048) AS pa, percentile_approx_weighted_combine(x, w, 0.9, 2048) AS pw,
           percentile_approx_combine(x, [0.5, 0.9], 2048) AS pq
    FROM t_merge_const GROUP BY g, k % 3 = 0
)
SELECT a.g, percentile_approx_state_merge(percentile_approx_state_union(a.pa, b.pa)),
       percentile_approx_weighted_state_merge(percentile_approx_weighted_state_union(a.pw, b.pw)),
       percentile_approx_state_merge(percentile_approx_state_union(a.pq, b.pq))
FROM p a JOIN p b ON a.g = b.g AND a.part AND NOT b.part ORDER BY a.g;
-- result:
0	9043.5	9003.3330078125	[5001.5,9043.5]
1	8995.5	8984.25	[5029.5,8995.5]
2	8984.5	9022.69921875	[5019.5,8984.5]
3	8995.0	9051.83984375	[4992,8995]
-- !result
WITH p AS (
    SELECT g, k % 3 = 0 AS part,
           percentile_approx_combine(x, 0.9, 2048) AS pa, percentile_approx_weighted_combine(x, w, 0.9, 2048) AS pw,
           percentile_approx_combine(x, [0.5, 0.9], 2048) AS pq
    FROM t_merge_const GROUP BY g, k % 3 = 0
)
SELECT g, percentile_approx_state_merge(percentile_approx_union(pa)), percentile_approx_weighted_merge(pw),
       percentile_approx_merge(pq)
FROM p GROUP BY g ORDER BY g;
-- result:
0	9043.5	9003.3330078125	[5001.5,9043.5]
1	8995.5	8984.25	[5029.5,8995.5]
2	8984.5	9022.69921875	[5019.5,8984.5]
3	8995.0	9051.83984375	[4992,8995]
-- !result
CREATE TABLE t_merge_const_states (
    g INT,
    pa percentile_approx(double, double, double),
    pw percentile_approx_weighted(double, bigint, double, double)
)
AGGREGATE KEY(g)
DISTRIBUTED BY HASH(g) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_merge_const_states
SELECT g, percentile_approx_combine(x, 0.9, 2048), percentile_approx_weighted_combine(x, w, 0.9, 2048)
FROM t_merge_const WHERE k % 3 = 0 GROUP BY g;
-- result:
-- !result
INSERT INTO t_merge_const_states
SELECT g, percentile_approx_combine(x, 0.9, 2048), percentile_approx_weighted_combine(x, w, 0.9, 2048)
FROM t_merge_const WHERE k % 3 != 0 GROUP BY g;
-- result:
-- !result
SELECT g, percentile_approx_merge(pa), percentile_approx_weighted_merge(pw)
FROM t_merge_const_states GROUP BY g ORDER BY g;
-- result:
0	9043.5	9003.3330078125
1	8995.5	8984.25
2	8984.5	9022.69921875
3	8995.0	9051.83984375
-- !result
SELECT g, percentile_approx_state_merge(pa), percentile_approx_weighted_state_merge(pw)
FROM t_merge_const_states ORDER BY g;
-- result:
0	9043.5	9003.3330078125
1	8995.5	8984.25
2	8984.5	9022.69921875
3	8995.0	9051.83984375
-- !result
