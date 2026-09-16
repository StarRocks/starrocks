-- name: test_json_path_rewrite_agg_table_preagg
drop database if exists test_json_path_rewrite_agg_table_preagg;
-- result:
-- !result
CREATE DATABASE test_json_path_rewrite_agg_table_preagg;
-- result:
-- !result
USE test_json_path_rewrite_agg_table_preagg;
-- result:
-- !result
CREATE TABLE agg_json (
  `k` int NULL,
  `v` json REPLACE
) ENGINE=OLAP
AGGREGATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO agg_json VALUES (1, parse_json('{"a":1,"b":"x"}')), (2, parse_json('{"a":2,"b":"y"}'));
-- result:
-- !result
INSERT INTO agg_json VALUES (1, parse_json('{"a":1,"b":"x"}')), (2, parse_json('{"a":2,"b":"y"}'));
-- result:
-- !result
SELECT sum(get_json_int(v, '$.a')) FROM agg_json;
-- result:
3
-- !result
SELECT max(get_json_int(v, '$.a')) FROM agg_json;
-- result:
2
-- !result
SELECT count(get_json_int(v, '$.a')) FROM agg_json;
-- result:
2
-- !result
SELECT min(get_json_string(v, '$.b')) FROM agg_json;
-- result:
x
-- !result
SELECT sum(get_json_int(v, '$.a')) FROM agg_json WHERE k = 2;
-- result:
2
-- !result
SELECT k, get_json_int(v, '$.a'), get_json_string(v, '$.b') FROM agg_json ORDER BY k;
-- result:
1	1	x
2	2	y
-- !result
SELECT sum(json_length(v)) FROM agg_json;
-- result:
4
-- !result
SELECT max(k) FROM agg_json;
-- result:
2
-- !result
CREATE TABLE agg_json_superseded (
  `k` int NULL,
  `v` json REPLACE
) ENGINE=OLAP
AGGREGATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 4
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO agg_json_superseded VALUES
  (1, parse_json('{"a":1}')), (2, parse_json('{"a":2}')), (3, parse_json('{"a":3}'));
-- result:
-- !result
INSERT INTO agg_json_superseded VALUES
  (1, parse_json('{"a":100}')), (3, parse_json('{"a":300}'));
-- result:
-- !result
SELECT sum(get_json_int(v, '$.a')) FROM agg_json_superseded;
-- result:
402
-- !result
SELECT max(get_json_int(v, '$.a')) FROM agg_json_superseded;
-- result:
300
-- !result
SELECT count(*) FROM agg_json_superseded;
-- result:
3
-- !result
SELECT k, get_json_int(v, '$.a') FROM agg_json_superseded ORDER BY k;
-- result:
1	100
2	2
3	300
-- !result