-- name: test_topn_window_pre_agg
-- A ranking window filtered by `rn <= k`, sitting on a second window with the SAME PARTITION BY, no
-- ORDER BY and a single-argument plain aggregate, is turned into a PARTITION-TOP-N whose pre-agg
-- calls are pushed into it (enable_push_down_pre_agg_with_rank, on by default).
--
-- The FE gives that SORT node TWO tuples -- the sort tuple and the pre-agg tuple. The BE asserted it
-- had exactly one and aborted in TopNNode::init, before executing a single row, so every plan of this
-- shape killed a debug/ASan backend. There was no test over this path, which is how the assertion
-- shipped.
DROP TABLE IF EXISTS t_topn_pre_agg;

CREATE TABLE t_topn_pre_agg (
  g INT NOT NULL,
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(g, k)
DISTRIBUTED BY HASH(g) BUCKETS 3
PROPERTIES("replication_num" = "1");

-- k is unique within each g, so row_number() is deterministic and the expected output is stable.
INSERT INTO t_topn_pre_agg (g, k, v) VALUES
  (1, 1, 10), (1, 2, 20), (1, 3, 30),
  (2, 1, 100), (2, 2, 200),
  (3, 1, 5), (3, 2, 6), (3, 3, 7), (3, 4, 8);

-- Assert the plan is the one that used to crash. Without this, a planner change could stop producing
-- the pre-agg PARTITION-TOP-N and the test would keep passing while covering nothing at all.
function: assert_explain_contains('SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k', 'PARTITION-TOP-N')
function: assert_explain_contains('SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k', 'pre agg functions')

-- Aborts a debug/ASan BE before the fix.
SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k;

-- The optimization must not change the answer: same rows with the pre-agg push-down turned off.
set enable_push_down_pre_agg_with_rank = false;

SELECT * FROM (SELECT g, k, v, row_number() OVER (PARTITION BY g ORDER BY k) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 ORDER BY g, k;

set enable_push_down_pre_agg_with_rank = true;

-- A partition whose rows all tie on the ranking key still has a deterministic row COUNT, and the
-- pre-agg result is order-independent, so this stays stable while exercising the same node with a
-- non-unique sort key.
SELECT g, count(*) AS n, max(s) AS total FROM (SELECT g, row_number() OVER (PARTITION BY g ORDER BY v) AS rn, sum(v) OVER (PARTITION BY g) AS s FROM t_topn_pre_agg) x WHERE rn <= 2 GROUP BY g ORDER BY g;

DROP TABLE t_topn_pre_agg;
