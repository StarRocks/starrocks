-- name: test_struct_order_by_edge_cases

drop database if exists test_struct_order_by_edge_cases;
create database test_struct_order_by_edge_cases;
use test_struct_order_by_edge_cases;


-- Single field struct (should work like ordering by that field)
SELECT * FROM (
    VALUES 
        (1, named_struct('value', 100)),
        (2, named_struct('value', 50)),
        (3, named_struct('value', 75))
) t(id, info) 
ORDER BY info;

-- Struct with all NULL fields
SELECT * FROM (
    VALUES 
        (1, named_struct('name', NULL, 'age', NULL)),
        (2, named_struct('name', 'Alice', 'age', 25)),
        (3, named_struct('name', NULL, 'age', NULL))
) t(id, info) 
ORDER BY info;

-- Struct with boolean fields
SELECT * FROM (
    VALUES 
        (1, named_struct('flag1', true, 'flag2', false)),
        (2, named_struct('flag1', false, 'flag2', true)),
        (3, named_struct('flag1', true, 'flag2', true)),
        (4, named_struct('flag1', false, 'flag2', false))
) t(id, info) 
ORDER BY info;

-- Struct with date/datetime fields
SELECT * FROM (
    VALUES 
        (1, named_struct('start_date', DATE '2024-01-01', 'end_date', DATE '2024-12-31')),
        (2, named_struct('start_date', DATE '2024-01-01', 'end_date', DATE '2024-06-30')),
        (3, named_struct('start_date', DATE '2023-01-01', 'end_date', DATE '2023-12-31'))
) t(id, info) 
ORDER BY info;

-- Deeply nested structs (3 levels)
SELECT * FROM (
    VALUES 
        (1, named_struct('level1', named_struct('level2', named_struct('level3', 'A')))),
        (2, named_struct('level1', named_struct('level2', named_struct('level3', 'C')))),
        (3, named_struct('level1', named_struct('level2', named_struct('level3', 'B'))))
) t(id, info) 
ORDER BY info;

-- Struct with mixed NULL and non-NULL rows
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, NULL),
        (3, named_struct('name', 'Bob', 'age', 30)),
        (4, NULL),
        (5, named_struct('name', 'Charlie', 'age', 20))
) t(id, info) 
ORDER BY info NULLS FIRST;

-- Struct with mixed NULL and non-NULL rows, NULLS LAST
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, NULL),
        (3, named_struct('name', 'Bob', 'age', 30)),
        (4, NULL)
) t(id, info) 
ORDER BY info NULLS LAST;

-- Struct with decimal fields
SELECT * FROM (
    VALUES 
        (1, named_struct('price', CAST(100.50 AS DECIMAL(10,2)), 'quantity', 10)),
        (2, named_struct('price', CAST(100.50 AS DECIMAL(10,2)), 'quantity', 5)),
        (3, named_struct('price', CAST(99.99 AS DECIMAL(10,2)), 'quantity', 20))
) t(id, info) 
ORDER BY info;

-- Struct with very long string fields
SELECT * FROM (
    VALUES 
        (1, named_struct('description', REPEAT('A', 100), 'id', 1)),
        (2, named_struct('description', REPEAT('B', 100), 'id', 2)),
        (3, named_struct('description', REPEAT('A', 100), 'id', 2))
) t(id, info) 
ORDER BY info;

-- ORDER BY struct column position
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Bob', 'age', 30)),
        (2, named_struct('name', 'Alice', 'age', 25))
) t(id, info) 
ORDER BY 2;

-- ORDER BY struct with alias
SELECT id, info as person_info FROM (
    VALUES 
        (1, named_struct('name', 'Bob', 'age', 30)),
        (2, named_struct('name', 'Alice', 'age', 25))
) t(id, info) 
ORDER BY person_info;

-- Struct in HAVING clause with ORDER BY
CREATE TABLE IF NOT EXISTS test_struct_edge (
    id INT,
    category STRING,
    info STRUCT<name STRING, score INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO test_struct_edge VALUES
    (1, 'A', named_struct('name', 'Alice', 'score', 90)),
    (2, 'A', named_struct('name', 'Bob', 'score', 85)),
    (3, 'B', named_struct('name', 'Charlie', 'score', 92)),
    (4, 'B', named_struct('name', 'David', 'score', 88));

SELECT category, MIN(info) as min_info, MAX(info) as max_info
FROM test_struct_edge
GROUP BY category
ORDER BY min_info;

-- ORDER BY with CASE expression returning struct
SELECT id, 
       CASE WHEN id % 2 = 0 
            THEN named_struct('type', 'even', 'id', id)
            ELSE named_struct('type', 'odd', 'id', id)
       END as result
FROM (VALUES (1), (2), (3), (4)) t(id)
ORDER BY result;

-- Struct comparison in WHERE clause
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, named_struct('name', 'Bob', 'age', 30)),
        (3, named_struct('name', 'Charlie', 'age', 20))
) t(id, info)
WHERE info > named_struct('name', 'Alice', 'age', 20)
ORDER BY info;

-- Struct with DISTINCT
SELECT DISTINCT info FROM (
    VALUES 
        (named_struct('name', 'Alice', 'age', 25)),
        (named_struct('name', 'Bob', 'age', 30)),
        (named_struct('name', 'Alice', 'age', 25)),
        (named_struct('name', 'Charlie', 'age', 20))
) t(info)
ORDER BY info;

-- Clean up
DROP TABLE test_struct_edge FORCE;
