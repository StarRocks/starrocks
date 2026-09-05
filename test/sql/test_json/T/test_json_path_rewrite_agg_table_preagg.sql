-- name: test_json_path_rewrite_agg_table_preagg
-- Regression: aggregating a JSON subfield of an AGGREGATE KEY table used to fail in the FE.
--
-- The JSON path pushdown rewrites get_json_xxx(col, '<constant path>') into a synthetic subfield column
-- and attaches it to the scan. Only AGGREGATE KEY tables run PreAggregateTurnOnRule, which compares the
-- query's aggregate against each scanned column's aggregation type -- and a synthetic subfield has none,
-- so the rule dereferenced null and the raw NullPointerException reached the client as ERROR 1064. A
-- derived subfield carries no aggregation semantics of its own, so pre-aggregation is simply turned off.
--
-- Nothing here can be worked around with a session variable: cbo_prune_json_subfield gates
-- PruneSubfieldRule, not this rewrite. The same queries against a DUPLICATE table were always fine,
-- which is why this needs an aggregate table to reproduce.
drop database if exists test_json_path_rewrite_agg_table_preagg;
CREATE DATABASE test_json_path_rewrite_agg_table_preagg;
USE test_json_path_rewrite_agg_table_preagg;

CREATE TABLE agg_json (
  `k` int NULL,
  `v` json REPLACE
) ENGINE=OLAP
AGGREGATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- The same values twice, so two rowsets have to be merged on read while the REPLACE result stays
-- deterministic whichever row wins.
INSERT INTO agg_json VALUES (1, parse_json('{"a":1,"b":"x"}')), (2, parse_json('{"a":2,"b":"y"}'));
INSERT INTO agg_json VALUES (1, parse_json('{"a":1,"b":"x"}')), (2, parse_json('{"a":2,"b":"y"}'));

-- Each of these used to return ERROR 1064 with a raw NullPointerException message.
SELECT sum(get_json_int(v, '$.a')) FROM agg_json;
SELECT max(get_json_int(v, '$.a')) FROM agg_json;
SELECT count(get_json_int(v, '$.a')) FROM agg_json;
SELECT min(get_json_string(v, '$.b')) FROM agg_json;
SELECT sum(get_json_int(v, '$.a')) FROM agg_json WHERE k = 2;

-- Projecting the same subfield always worked; the two forms have to agree.
SELECT k, get_json_int(v, '$.a'), get_json_string(v, '$.b') FROM agg_json ORDER BY k;

-- Controls: an aggregate that the rewrite does not touch, and a key-column aggregate.
SELECT sum(json_length(v)) FROM agg_json;
SELECT max(k) FROM agg_json;

-- The loads above carry identical values, which pins that no row is counted twice but not that a
-- superseded value stays out of the answer. Load different values for the same keys so the three
-- possible outcomes are all distinguishable: 402 when the merge is honoured, 406 if the rows are
-- aggregated unmerged, and 6 if the superseded values win.
CREATE TABLE agg_json_superseded (
  `k` int NULL,
  `v` json REPLACE
) ENGINE=OLAP
AGGREGATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 4
PROPERTIES("replication_num" = "1");

INSERT INTO agg_json_superseded VALUES
  (1, parse_json('{"a":1}')), (2, parse_json('{"a":2}')), (3, parse_json('{"a":3}'));
INSERT INTO agg_json_superseded VALUES
  (1, parse_json('{"a":100}')), (3, parse_json('{"a":300}'));

SELECT sum(get_json_int(v, '$.a')) FROM agg_json_superseded;
SELECT max(get_json_int(v, '$.a')) FROM agg_json_superseded;
SELECT count(*) FROM agg_json_superseded;
SELECT k, get_json_int(v, '$.a') FROM agg_json_superseded ORDER BY k;
