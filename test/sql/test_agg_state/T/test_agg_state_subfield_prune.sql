-- name: test_agg_state_subfield_prune
-- Regression for silently wrong results, and a BE crash, when subfield pruning is applied to an
-- AGG_STATE_UNION column.
--
-- An agg-state column does not store the value a query reads. It stores a serialized aggregate
-- state, and the storage layer rebuilds the value by running the aggregate function over that state
-- while it merges rowsets. PruneSubfieldRule handed those columns a subfield access path anyway:
-- array_length(v) needs only the length, so the scan was told to fetch /v/OFFSET and skip the
-- elements, and the merge then ran against an array that kept its offsets and lost its elements.
-- array_length() answered 0 or 1 instead of the real length, so the same row gave different answers
-- depending on whether the query also projected an element. On wider data the merge indexed past the
-- element column and killed the BE in NullableColumn::null_count(), and since compaction re-runs the
-- same merge, the BE crash-looped after every restart. A global dictionary is unusable on these
-- columns for the same reason, see #77096.
--
-- Every assertion below is order-independent: array_agg_distinct is an unordered aggregate, so only
-- element counts are stable, never element positions. The states are built with the scalar
-- array_agg_distinct_state() rather than the aggregate _combine(), because _state() is the spelling
-- that exists on every branch this fix is backported to.
SET cbo_prune_subfield = true;

CREATE TABLE src (k INT, s VARCHAR(100))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 2000 groups x 40 distinct strings each = 80000 rows.
INSERT INTO src
SELECT g1.generate_series AS k,
       concat('str_', cast((g2.generate_series % 40) AS string)) AS s
FROM TABLE(generate_series(1, 2000)) g1, TABLE(generate_series(1, 40)) g2;

CREATE TABLE st (k INT, v array_agg_distinct(varchar(100)))
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- One state per source row, loaded twice, so a key's value only exists after the storage layer
-- unions many states across two rowsets. Each key still ends up with its 40 distinct strings.
INSERT INTO st SELECT k, array_agg_distinct_state(s) FROM src;
INSERT INTO st SELECT k, array_agg_distinct_state(s) FROM src;

-- Length-only projections: these are the ones that used to be handed /v/OFFSET.
SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM st;
SELECT count(*) AS n_full_arrays FROM st WHERE array_length(v) = 40;
SELECT array_length(v) FROM st WHERE k = 1;
SELECT sum(cardinality(v)) AS total_elems FROM st;

-- Projecting an element as well was always correct, because that plan reads the whole column. The
-- two projections of the same row have to agree.
SELECT array_length(v), v[1] IS NOT NULL FROM st WHERE k = 1;

-- Control: a plain ARRAY column is the value it stores, so it keeps its subfield pruning and its
-- results must be unaffected by the fix.
CREATE TABLE plain (k INT, v ARRAY<VARCHAR(100)>)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

INSERT INTO plain SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;

SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM plain;

-- Control: an ARRAY column with REPLACE aggregation lives in an aggregate table and is merged on
-- read too, but it stores the value itself, so it was never affected either.
CREATE TABLE repl (k INT, v ARRAY<VARCHAR(100)> REPLACE)
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 8
PROPERTIES ("replication_num" = "1");

INSERT INTO repl SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;
INSERT INTO repl SELECT k, array_agg(DISTINCT s) FROM src GROUP BY k;

SELECT count(*) AS n, sum(array_length(v)) AS total_elems FROM repl;
