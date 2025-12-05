-- name: test_array_struct_order_by

drop database if exists test_array_struct_order_by;
create database test_array_struct_order_by;
use test_array_struct_order_by;

-- Create table with ARRAY<STRUCT> column
CREATE TABLE IF NOT EXISTS test_array_struct (
    id INT,
    tags ARRAY<STRUCT<tag_key STRING, tag_value INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

-- Insert test data
INSERT INTO test_array_struct VALUES
    (1, [row('a', 1), row('b', 2)]),
    (2, [row('a', 1), row('b', 1)]),
    (3, [row('a', 2), row('b', 1)]),
    (4, [row('a', 1)]),
    (5, []);

-- Test 1: Basic ORDER BY on ARRAY<STRUCT>
SELECT * FROM test_array_struct ORDER BY tags;

-- Test 2: ORDER BY DESC
SELECT * FROM test_array_struct ORDER BY tags DESC;

-- Test 3: ARRAY<STRUCT> with NULL values
INSERT INTO test_array_struct VALUES
    (6, NULL),
    (7, [row(NULL, 1)]),
    (8, [row('c', NULL)]);

SELECT * FROM test_array_struct ORDER BY tags NULLS FIRST;
SELECT * FROM test_array_struct ORDER BY tags NULLS LAST;

-- Test 4: Different array lengths
CREATE TABLE IF NOT EXISTS test_array_struct_lengths (
    id INT,
    items ARRAY<STRUCT<name STRING, score INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO test_array_struct_lengths VALUES
    (1, [row('Alice', 90)]),
    (2, [row('Alice', 90), row('Bob', 80)]),
    (3, [row('Alice', 90), row('Bob', 80), row('Charlie', 85)]),
    (4, []),
    (5, [row('Alice', 85)]);

SELECT * FROM test_array_struct_lengths ORDER BY items;

-- Test 5: Nested ARRAY<STRUCT> with multiple fields
CREATE TABLE IF NOT EXISTS test_array_struct_complex (
    id INT,
    events ARRAY<STRUCT<event_name STRING, timestamp BIGINT, metadata STRUCT<source STRING, priority INT>>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO test_array_struct_complex VALUES
    (1, [row('login', 1000, row('web', 1))]),
    (2, [row('login', 1000, row('mobile', 2))]),
    (3, [row('login', 999, row('web', 1))]),
    (4, [row('logout', 1000, row('web', 1))]);

SELECT * FROM test_array_struct_complex ORDER BY events;

-- Test 6: ARRAY<STRUCT> in VALUES clause
SELECT * FROM (
    VALUES 
        (1, [row('x', 10)]),
        (2, [row('y', 20)]),
        (3, [row('x', 15)])
) t(id, data) 
ORDER BY data;

-- Test 7: ORDER BY with WHERE clause
SELECT * FROM test_array_struct_lengths WHERE id <= 3 ORDER BY items;

-- Test 8: ORDER BY with JOIN
CREATE TABLE IF NOT EXISTS test_array_struct_join (
    id INT,
    categories ARRAY<STRUCT<name STRING, level INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO test_array_struct_join VALUES
    (1, [row('tech', 1)]),
    (2, [row('sports', 2)]),
    (3, [row('tech', 2)]);

SELECT t1.id, t1.items, t2.categories
FROM test_array_struct_lengths t1
JOIN test_array_struct_join t2 ON t1.id = t2.id
ORDER BY t1.items, t2.categories;

-- Test 9: ORDER BY with GROUP BY on other columns
SELECT id, items, COUNT(*) as cnt
FROM (
    SELECT id, items FROM test_array_struct_lengths
    UNION ALL
    SELECT id, items FROM test_array_struct_lengths WHERE id <= 2
) t
GROUP BY id, items
ORDER BY items;

-- Test 10: ARRAY<STRUCT> with LIMIT
SELECT * FROM test_array_struct_lengths ORDER BY items LIMIT 3;

-- Test 11: Empty array vs NULL
INSERT INTO test_array_struct_lengths VALUES (6, NULL);
SELECT * FROM test_array_struct_lengths WHERE id >= 4 ORDER BY items NULLS FIRST;

-- Test 12: ARRAY<STRUCT> with same first element, different second
CREATE TABLE IF NOT EXISTS test_array_struct_compare (
    id INT,
    pairs ARRAY<STRUCT<first INT, second INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO test_array_struct_compare VALUES
    (1, [row(1, 2), row(3, 4)]),
    (2, [row(1, 2), row(3, 3)]),
    (3, [row(1, 2), row(2, 4)]),
    (4, [row(1, 2)]);

SELECT * FROM test_array_struct_compare ORDER BY pairs;

-- Test 13: ARRAY<STRUCT> in subquery
SELECT id, pairs FROM (
    SELECT * FROM test_array_struct_compare WHERE id <= 3
) t 
ORDER BY pairs DESC;

-- Test 14: Multiple ORDER BY columns including ARRAY<STRUCT>
SELECT * FROM test_array_struct_compare ORDER BY pairs, id DESC;

-- Test 15: ARRAY<STRUCT> with UNION
SELECT pairs FROM test_array_struct_compare WHERE id <= 2
UNION ALL
SELECT [row(1, 3), row(3, 3)]
ORDER BY pairs;

-- Clean up
DROP TABLE test_array_struct FORCE;
DROP TABLE test_array_struct_lengths FORCE;
DROP TABLE test_array_struct_complex FORCE;
DROP TABLE test_array_struct_join FORCE;
DROP TABLE test_array_struct_compare FORCE;

