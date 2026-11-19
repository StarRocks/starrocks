-- name: test_agg_state_merge_constant_args
-- <fn>_state_merge / <fn>_state_union, the <fn>_union / <fn>_merge aggregates and the storage merge of agg_state
-- columns merge states without the call's constant arguments: the compression of percentile_approx(_weighted)
-- must come from the states, and the results must equal the plain aggregate's.
CREATE TABLE t_merge_const (k INT, g INT, v INT, x DOUBLE, w BIGINT)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO t_merge_const
SELECT generate_series, generate_series % 4, (generate_series * 7919) % 1009, (generate_series * 7919) % 10007,
       generate_series % 3 + 1
FROM TABLE(generate_series(1, 4000));

SELECT g, percentile_approx(x, 0.9, 2048), percentile_approx_weighted(x, w, 0.9, 2048),
       percentile_approx(x, [0.5, 0.9], 2048)
FROM t_merge_const GROUP BY g ORDER BY g;

SELECT g, percentile_approx_state_merge(percentile_approx_combine(x, 0.9, 2048)),
       percentile_approx_weighted_state_merge(percentile_approx_weighted_combine(x, w, 0.9, 2048)),
       percentile_approx_state_merge(percentile_approx_combine(x, [0.5, 0.9], 2048))
FROM t_merge_const GROUP BY g ORDER BY g;

-- Two partial states per group.
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

WITH p AS (
    SELECT g, k % 3 = 0 AS part,
           percentile_approx_combine(x, 0.9, 2048) AS pa, percentile_approx_weighted_combine(x, w, 0.9, 2048) AS pw,
           percentile_approx_combine(x, [0.5, 0.9], 2048) AS pq
    FROM t_merge_const GROUP BY g, k % 3 = 0
)
SELECT g, percentile_approx_state_merge(percentile_approx_union(pa)), percentile_approx_weighted_merge(pw),
       percentile_approx_merge(pq)
FROM p GROUP BY g ORDER BY g;

-- Two loads of the same keys, merged by the storage engine.
CREATE TABLE t_merge_const_states (
    g INT,
    pa percentile_approx(double, double, double),
    pw percentile_approx_weighted(double, bigint, double, double)
)
AGGREGATE KEY(g)
DISTRIBUTED BY HASH(g) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO t_merge_const_states
SELECT g, percentile_approx_combine(x, 0.9, 2048), percentile_approx_weighted_combine(x, w, 0.9, 2048)
FROM t_merge_const WHERE k % 3 = 0 GROUP BY g;

INSERT INTO t_merge_const_states
SELECT g, percentile_approx_combine(x, 0.9, 2048), percentile_approx_weighted_combine(x, w, 0.9, 2048)
FROM t_merge_const WHERE k % 3 != 0 GROUP BY g;

SELECT g, percentile_approx_merge(pa), percentile_approx_weighted_merge(pw)
FROM t_merge_const_states GROUP BY g ORDER BY g;

SELECT g, percentile_approx_state_merge(pa), percentile_approx_weighted_state_merge(pw)
FROM t_merge_const_states ORDER BY g;
