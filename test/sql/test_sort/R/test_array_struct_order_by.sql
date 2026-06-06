-- name: test_array_struct_order_by
drop database if exists test_array_struct_order_by;
-- result:
-- !result
create database test_array_struct_order_by;
-- result:
-- !result
use test_array_struct_order_by;
-- result:
-- !result
CREATE TABLE IF NOT EXISTS test_array_struct (
    id INT,
    tags ARRAY<STRUCT<tag_key STRING, tag_value INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_array_struct VALUES
    (1, [row('a', 1), row('b', 2)]),
    (2, [row('a', 1), row('b', 1)]),
    (3, [row('a', 2), row('b', 1)]),
    (4, [row('a', 1)]),
    (5, []);
-- result:
-- !result
SELECT * FROM test_array_struct ORDER BY tags;
-- result:
5	[]
4	[{"tag_key":"a","tag_value":1}]
2	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":1}]
1	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":2}]
3	[{"tag_key":"a","tag_value":2},{"tag_key":"b","tag_value":1}]
-- !result
SELECT * FROM test_array_struct ORDER BY tags DESC;
-- result:
3	[{"tag_key":"a","tag_value":2},{"tag_key":"b","tag_value":1}]
1	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":2}]
2	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":1}]
4	[{"tag_key":"a","tag_value":1}]
5	[]
-- !result
INSERT INTO test_array_struct VALUES
    (6, NULL),
    (7, [row(NULL, 1)]),
    (8, [row('c', NULL)]);
-- result:
-- !result
SELECT * FROM test_array_struct ORDER BY tags NULLS FIRST;
-- result:
6	None
5	[]
7	[{"tag_key":null,"tag_value":1}]
4	[{"tag_key":"a","tag_value":1}]
2	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":1}]
1	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":2}]
3	[{"tag_key":"a","tag_value":2},{"tag_key":"b","tag_value":1}]
8	[{"tag_key":"c","tag_value":null}]
-- !result
SELECT * FROM test_array_struct ORDER BY tags NULLS LAST;
-- result:
5	[]
4	[{"tag_key":"a","tag_value":1}]
2	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":1}]
1	[{"tag_key":"a","tag_value":1},{"tag_key":"b","tag_value":2}]
3	[{"tag_key":"a","tag_value":2},{"tag_key":"b","tag_value":1}]
8	[{"tag_key":"c","tag_value":null}]
7	[{"tag_key":null,"tag_value":1}]
6	None
-- !result
CREATE TABLE IF NOT EXISTS test_array_struct_lengths (
    id INT,
    items ARRAY<STRUCT<name STRING, score INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_array_struct_lengths VALUES
    (1, [row('Alice', 90)]),
    (2, [row('Alice', 90), row('Bob', 80)]),
    (3, [row('Alice', 90), row('Bob', 80), row('Charlie', 85)]),
    (4, []),
    (5, [row('Alice', 85)]);
-- result:
-- !result
SELECT * FROM test_array_struct_lengths ORDER BY items;
-- result:
4	[]
5	[{"name":"Alice","score":85}]
1	[{"name":"Alice","score":90}]
2	[{"name":"Alice","score":90},{"name":"Bob","score":80}]
3	[{"name":"Alice","score":90},{"name":"Bob","score":80},{"name":"Charlie","score":85}]
-- !result
CREATE TABLE IF NOT EXISTS test_array_struct_complex (
    id INT,
    events ARRAY<STRUCT<event_name STRING, timestamp BIGINT, metadata STRUCT<source STRING, priority INT>>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_array_struct_complex VALUES
    (1, [row('login', 1000, row('web', 1))]),
    (2, [row('login', 1000, row('mobile', 2))]),
    (3, [row('login', 999, row('web', 1))]),
    (4, [row('logout', 1000, row('web', 1))]);
-- result:
-- !result
SELECT * FROM test_array_struct_complex ORDER BY events;
-- result:
3	[{"event_name":"login","timestamp":999,"metadata":{"source":"web","priority":1}}]
2	[{"event_name":"login","timestamp":1000,"metadata":{"source":"mobile","priority":2}}]
1	[{"event_name":"login","timestamp":1000,"metadata":{"source":"web","priority":1}}]
4	[{"event_name":"logout","timestamp":1000,"metadata":{"source":"web","priority":1}}]
-- !result
SELECT * FROM (
    VALUES 
        (1, [row('x', 10)]),
        (2, [row('y', 20)]),
        (3, [row('x', 15)])
) t(id, data) 
ORDER BY data;
-- result:
1	[{"col1":"x","col2":10}]
3	[{"col1":"x","col2":15}]
2	[{"col1":"y","col2":20}]
-- !result
SELECT * FROM test_array_struct_lengths WHERE id <= 3 ORDER BY items;
-- result:
1	[{"name":"Alice","score":90}]
2	[{"name":"Alice","score":90},{"name":"Bob","score":80}]
3	[{"name":"Alice","score":90},{"name":"Bob","score":80},{"name":"Charlie","score":85}]
-- !result
CREATE TABLE IF NOT EXISTS test_array_struct_join (
    id INT,
    categories ARRAY<STRUCT<name STRING, level INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_array_struct_join VALUES
    (1, [row('tech', 1)]),
    (2, [row('sports', 2)]),
    (3, [row('tech', 2)]);
-- result:
-- !result
SELECT t1.id, t1.items, t2.categories
FROM test_array_struct_lengths t1
JOIN test_array_struct_join t2 ON t1.id = t2.id
ORDER BY t1.items, t2.categories;
-- result:
1	[{"name":"Alice","score":90}]	[{"name":"tech","level":1}]
2	[{"name":"Alice","score":90},{"name":"Bob","score":80}]	[{"name":"sports","level":2}]
3	[{"name":"Alice","score":90},{"name":"Bob","score":80},{"name":"Charlie","score":85}]	[{"name":"tech","level":2}]
-- !result
SELECT id, items, COUNT(*) as cnt
FROM (
    SELECT id, items FROM test_array_struct_lengths
    UNION ALL
    SELECT id, items FROM test_array_struct_lengths WHERE id <= 2
) t
GROUP BY id, items
ORDER BY items;
-- result:
4	[]	1
5	[{"name":"Alice","score":85}]	1
1	[{"name":"Alice","score":90}]	2
2	[{"name":"Alice","score":90},{"name":"Bob","score":80}]	2
3	[{"name":"Alice","score":90},{"name":"Bob","score":80},{"name":"Charlie","score":85}]	1
-- !result
SELECT * FROM test_array_struct_lengths ORDER BY items LIMIT 3;
-- result:
4	[]
5	[{"name":"Alice","score":85}]
1	[{"name":"Alice","score":90}]
-- !result
INSERT INTO test_array_struct_lengths VALUES (6, NULL);
-- result:
-- !result
SELECT * FROM test_array_struct_lengths WHERE id >= 4 ORDER BY items NULLS FIRST;
-- result:
6	None
4	[]
5	[{"name":"Alice","score":85}]
-- !result
CREATE TABLE IF NOT EXISTS test_array_struct_compare (
    id INT,
    pairs ARRAY<STRUCT<first INT, second INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_array_struct_compare VALUES
    (1, [row(1, 2), row(3, 4)]),
    (2, [row(1, 2), row(3, 3)]),
    (3, [row(1, 2), row(2, 4)]),
    (4, [row(1, 2)]);
-- result:
-- !result
SELECT * FROM test_array_struct_compare ORDER BY pairs;
-- result:
4	[{"first":1,"second":2}]
3	[{"first":1,"second":2},{"first":2,"second":4}]
2	[{"first":1,"second":2},{"first":3,"second":3}]
1	[{"first":1,"second":2},{"first":3,"second":4}]
-- !result
SELECT id, pairs FROM (
    SELECT * FROM test_array_struct_compare WHERE id <= 3
) t 
ORDER BY pairs DESC;
-- result:
1	[{"first":1,"second":2},{"first":3,"second":4}]
2	[{"first":1,"second":2},{"first":3,"second":3}]
3	[{"first":1,"second":2},{"first":2,"second":4}]
-- !result
SELECT * FROM test_array_struct_compare ORDER BY pairs, id DESC;
-- result:
4	[{"first":1,"second":2}]
3	[{"first":1,"second":2},{"first":2,"second":4}]
2	[{"first":1,"second":2},{"first":3,"second":3}]
1	[{"first":1,"second":2},{"first":3,"second":4}]
-- !result
SELECT pairs FROM test_array_struct_compare WHERE id <= 2
UNION ALL
SELECT [row(1, 3), row(3, 3)]
ORDER BY pairs;
-- result:
[{"first":1,"second":2},{"first":3,"second":3}]
[{"first":1,"second":2},{"first":3,"second":4}]
[{"first":1,"second":3},{"first":3,"second":3}]
-- !result
DROP TABLE test_array_struct FORCE;
-- result:
-- !result
DROP TABLE test_array_struct_lengths FORCE;
-- result:
-- !result
DROP TABLE test_array_struct_complex FORCE;
-- result:
-- !result
DROP TABLE test_array_struct_join FORCE;
-- result:
-- !result
DROP TABLE test_array_struct_compare FORCE;
-- result:
-- !result