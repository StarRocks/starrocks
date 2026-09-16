-- name: test_ds_hll_count_distinct_estimate_stable

-- Regression test for the order-dependent (unstable) HLL cardinality estimate.
--
-- Both tables hold the SAME 16000 distinct values, but split into a different number
-- of per-key (coupon-mode) partial sketches: 16 partials vs 1. The total cardinality
-- crosses the SET->HLL promotion threshold (lg_k=17, threshold = 3/4 * 2^(17-3) = 12288),
-- so the union gadget is promoted to HLL via coupon replay WITHOUT setting the
-- out-of-order flag. The old estimate_cardinality() then used the HIP estimator, whose
-- value depends on the merge/replay order, so the two tables produced different (and
-- run-to-run unstable) results. The composite estimator depends only on the final
-- register state, which is identical regardless of how the values were partitioned, so
-- the two estimates must agree (allowing 1 for double->int64 truncation) and stay close
-- to the true cardinality.

create database db_${uuid0};
use db_${uuid0};

CREATE TABLE t_hll_16 (
  k    int NULL,
  cnt  ds_hll_count_distinct(varchar, int, varchar) NULL
) ENGINE=OLAP
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 16
PROPERTIES ("replication_num" = "1");

CREATE TABLE t_hll_1 (
  k    int NULL,
  cnt  ds_hll_count_distinct(varchar, int, varchar) NULL
) ENGINE=OLAP
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 16
PROPERTIES ("replication_num" = "1");

-- 16 keys * 1000 distinct values each = 16000 distinct in total.
insert into t_hll_16
select generate_series % 16, ds_hll_count_distinct_state(cast(generate_series as varchar), 17, 'HLL_6')
from table(generate_series(1, 16000));

-- The same 16000 distinct values accumulated under a single key.
insert into t_hll_1
select 0, ds_hll_count_distinct_state(cast(generate_series as varchar), 17, 'HLL_6')
from table(generate_series(1, 16000));

-- Partition-independent: the two estimates must match (modulo double->int64 truncation).
select abs(
  (select ds_hll_count_distinct_merge(s) from (select ds_hll_count_distinct_union(cnt) s from t_hll_16) a)
  -
  (select ds_hll_count_distinct_merge(s) from (select ds_hll_count_distinct_union(cnt) s from t_hll_1) b)
) <= 1;
-- result:
1
-- !result

-- Accuracy: the estimate stays within ~3% of the true cardinality (16000).
select ds_hll_count_distinct_merge(s) between 15500 and 16500
from (select ds_hll_count_distinct_union(cnt) s from t_hll_16) a;
-- result:
1
-- !result

drop database db_${uuid0};