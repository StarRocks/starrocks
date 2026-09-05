-- name: test_agg_state_scalar_merge_mempool
-- Regression for the BE agg-state combinator MemPool crash (a hotfix on top of #76840, which
-- retired the THREAD_LOCAL FunctionStateScope and started passing a null mem_pool to scalar
-- function contexts). The scalar array_agg_distinct_state_merge(agg_state_col) drives
-- array_agg_distinct::merge, which allocates its distinct-key storage from the FunctionContext's
-- mem_pool -- with the null pool that path null-dereferenced and SIGSEGV'd the BE. The combinator
-- is also a single object shared by every pipeline driver, so its nested context/pool were touched
-- concurrently (a data race). The aggregate form array_agg_distinct_merge (per-driver pool) does
-- NOT exercise this; only the scalar _state_merge over a stored agg-state column does, which is why
-- the existing all-functions coverage did not catch it.
--
-- This case uses a large table and evaluates the scalar merge across many parallel pipeline drivers
-- (BUCKETS 8 + pipeline_dop 8), discarding the output through the blackhole sink so nothing
-- serializes back to the client and the merge runs at full concurrency. On the unfixed BE this
-- crashes; with the fix each worker gets its own nested context + private MemPool.
CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 10000 groups x 40 distinct strings each = 400000 rows.
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 10000)) g1, TABLE(generate_series(1, 40)) g2;

CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

INSERT INTO st SELECT k, array_agg_distinct_combine(s) FROM src GROUP BY k;

SET pipeline_dop = 8;

-- Concurrency stress: run the scalar state-merge across many parallel drivers, output discarded.
INSERT INTO blackhole() SELECT array_agg_distinct_state_merge(v) FROM st;

-- Deterministic, order-independent correctness check: every one of the 10000 groups merges back to
-- its 40 distinct strings (10000 * 40 = 400000).
SELECT count(*) AS n, sum(array_length(array_agg_distinct_state_merge(v))) AS total_elems
FROM st;
