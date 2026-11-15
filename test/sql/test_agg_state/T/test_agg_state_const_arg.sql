-- name: test_agg_state_const_arg
-- Regression for *_state(<literal>) producing a corrupted aggregate state.
-- The scalar StateFunction passed its input columns to the aggregate's
-- convert_to_serialize_format() as-is, so a literal argument arrived as a ConstColumn while
-- every aggregate implementation down_casts it to a concrete data column. The states were
-- silently wrong (avg_merge read 38 back as -24) or the BE crashed with SIGSEGV in
-- StateFunction::execute. The aggregate-driven *_state(<column>) form was unaffected, because
-- Aggregator::evaluate_agg_input_column already expands a constant data argument.
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

-- The *_state arguments are literals, so the BE evaluates them over ConstColumns.
INSERT INTO t_agg_state_const VALUES
  ('a', avg_state(cast(38 as bigint)), sum_state(cast(38 as bigint)), min_state(cast(38 as bigint)), max_state(cast(38 as bigint)), count_state(cast(38 as bigint))),
  ('b', avg_state(cast(50 as bigint)), sum_state(cast(50 as bigint)), min_state(cast(50 as bigint)), max_state(cast(50 as bigint)), count_state(cast(50 as bigint)));

SELECT k1, avg_merge(v_avg), sum_merge(v_sum), min_merge(v_min), max_merge(v_max), count_merge(v_count)
FROM t_agg_state_const GROUP BY k1 ORDER BY k1;

-- The same states built from a column must merge back to exactly the same results.
CREATE TABLE t_src (k1 VARCHAR(10), pv BIGINT)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO t_src VALUES ('a', 38), ('b', 50);

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

INSERT INTO t_agg_state_col
SELECT k1, avg_state(pv), sum_state(pv), min_state(pv), max_state(pv), count_state(pv) FROM t_src;

SELECT k1, avg_merge(v_avg), sum_merge(v_sum), min_merge(v_min), max_merge(v_max), count_merge(v_count)
FROM t_agg_state_col GROUP BY k1 ORDER BY k1;

-- Pure query form, no storage involved.
SELECT avg_merge(avg_state(cast(38 as bigint))), sum_merge(sum_state(cast(38 as bigint))), min_merge(min_state(cast(38 as bigint))), max_merge(max_state(cast(38 as bigint))), count_merge(count_state(cast(38 as bigint)));

-- A constant NULL argument must be expanded into a nullable column as well.
SELECT avg_merge(avg_state(cast(NULL as bigint)));

-- Constant argument evaluated over a multi-row chunk.
SELECT avg_merge(avg_state(cast(38 as bigint))), count_merge(count_state(cast(38 as bigint))) FROM t_src;

-- Guard: the quantile of percentile_approx must stay a constant column, so only the data
-- argument is expanded. Both the constant and the column form have to keep working.
SELECT percentile_approx_merge(percentile_approx_state(cast(38 as double), 0.5)), percentile_approx_merge(percentile_approx_state(pv, 0.5)) FROM t_src;

-- The same guard on the load path, where the states are serialized into storage rather than
-- consumed by the query itself. percentile_approx keeps its quantile as a constant column and
-- percentile_approx_weighted keeps both its weight and its quantile, so expanding every
-- argument instead of only the data argument would corrupt these states on the way in.
-- v_col is the form that already worked before the fix and must keep its exact values.
CREATE TABLE t_agg_state_percentile (
  k1 VARCHAR(10),
  v_col percentile_approx(double, double),
  v_const percentile_approx(double, double),
  v_weighted percentile_approx_weighted(double, bigint, double)
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO t_agg_state_percentile
SELECT k1,
       percentile_approx_state(cast(pv as double), 0.5),
       percentile_approx_state(cast(38 as double), 0.5),
       percentile_approx_weighted_state(cast(pv as double), 1, 0.5)
FROM t_src;

INSERT INTO t_agg_state_percentile VALUES
  ('c', percentile_approx_state(cast(38 as double), 0.5), percentile_approx_state(cast(50 as double), 0.5),
        percentile_approx_weighted_state(cast(38 as double), 1, 0.5));

SELECT k1, percentile_approx_merge(v_col), percentile_approx_merge(v_const), percentile_approx_weighted_merge(v_weighted)
FROM t_agg_state_percentile GROUP BY k1 ORDER BY k1;
