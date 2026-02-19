-- name: test_struct_order_by_edge_cases
drop database if exists test_struct_order_by_edge_cases;
-- result:
-- !result
create database test_struct_order_by_edge_cases;
-- result:
-- !result
use test_struct_order_by_edge_cases;
-- result:
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('value', 100)),
        (2, named_struct('value', 50)),
        (3, named_struct('value', 75))
) t(id, info) 
ORDER BY info;
-- result:
2	{"value":50}
3	{"value":75}
1	{"value":100}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', NULL, 'age', NULL)),
        (2, named_struct('name', 'Alice', 'age', 25)),
        (3, named_struct('name', NULL, 'age', NULL))
) t(id, info) 
ORDER BY info;
-- result:
1	{"name":null,"age":null}
3	{"name":null,"age":null}
2	{"name":"Alice","age":25}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('flag1', true, 'flag2', false)),
        (2, named_struct('flag1', false, 'flag2', true)),
        (3, named_struct('flag1', true, 'flag2', true)),
        (4, named_struct('flag1', false, 'flag2', false))
) t(id, info) 
ORDER BY info;
-- result:
4	{"flag1":0,"flag2":0}
2	{"flag1":0,"flag2":1}
1	{"flag1":1,"flag2":0}
3	{"flag1":1,"flag2":1}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('start_date', DATE '2024-01-01', 'end_date', DATE '2024-12-31')),
        (2, named_struct('start_date', DATE '2024-01-01', 'end_date', DATE '2024-06-30')),
        (3, named_struct('start_date', DATE '2023-01-01', 'end_date', DATE '2023-12-31'))
) t(id, info) 
ORDER BY info;
-- result:
3	{"start_date":"2023-01-01","end_date":"2023-12-31"}
2	{"start_date":"2024-01-01","end_date":"2024-06-30"}
1	{"start_date":"2024-01-01","end_date":"2024-12-31"}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('level1', named_struct('level2', named_struct('level3', 'A')))),
        (2, named_struct('level1', named_struct('level2', named_struct('level3', 'C')))),
        (3, named_struct('level1', named_struct('level2', named_struct('level3', 'B'))))
) t(id, info) 
ORDER BY info;
-- result:
1	{"level1":{"level2":{"level3":"A"}}}
3	{"level1":{"level2":{"level3":"B"}}}
2	{"level1":{"level2":{"level3":"C"}}}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, NULL),
        (3, named_struct('name', 'Bob', 'age', 30)),
        (4, NULL),
        (5, named_struct('name', 'Charlie', 'age', 20))
) t(id, info) 
ORDER BY info NULLS FIRST;
-- result:
4	None
2	None
1	{"name":"Alice","age":25}
3	{"name":"Bob","age":30}
5	{"name":"Charlie","age":20}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, NULL),
        (3, named_struct('name', 'Bob', 'age', 30)),
        (4, NULL)
) t(id, info) 
ORDER BY info NULLS LAST;
-- result:
1	{"name":"Alice","age":25}
3	{"name":"Bob","age":30}
2	None
4	None
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('price', CAST(100.50 AS DECIMAL(10,2)), 'quantity', 10)),
        (2, named_struct('price', CAST(100.50 AS DECIMAL(10,2)), 'quantity', 5)),
        (3, named_struct('price', CAST(99.99 AS DECIMAL(10,2)), 'quantity', 20))
) t(id, info) 
ORDER BY info;
-- result:
3	{"price":99.99,"quantity":20}
2	{"price":100.50,"quantity":5}
1	{"price":100.50,"quantity":10}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('description', REPEAT('A', 100), 'id', 1)),
        (2, named_struct('description', REPEAT('B', 100), 'id', 2)),
        (3, named_struct('description', REPEAT('A', 100), 'id', 2))
) t(id, info) 
ORDER BY info;
-- result:
1	{"description":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","id":1}
3	{"description":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","id":2}
2	{"description":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB","id":2}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Bob', 'age', 30)),
        (2, named_struct('name', 'Alice', 'age', 25))
) t(id, info) 
ORDER BY 2;
-- result:
2	{"name":"Alice","age":25}
1	{"name":"Bob","age":30}
-- !result
SELECT id, info as person_info FROM (
    VALUES 
        (1, named_struct('name', 'Bob', 'age', 30)),
        (2, named_struct('name', 'Alice', 'age', 25))
) t(id, info) 
ORDER BY person_info;
-- result:
2	{"name":"Alice","age":25}
1	{"name":"Bob","age":30}
-- !result
CREATE TABLE IF NOT EXISTS test_struct_edge (
    id INT,
    category STRING,
    info STRUCT<name STRING, score INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_struct_edge VALUES
    (1, 'A', named_struct('name', 'Alice', 'score', 90)),
    (2, 'A', named_struct('name', 'Bob', 'score', 85)),
    (3, 'B', named_struct('name', 'Charlie', 'score', 92)),
    (4, 'B', named_struct('name', 'David', 'score', 88));
-- result:
-- !result
SELECT category, MIN(info) as min_info, MAX(info) as max_info
FROM test_struct_edge
GROUP BY category
ORDER BY min_info;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 17 to line 1, column 25. Detail message: No matching function with signature: min(struct<name varchar(65533), score int(11)>).')
-- !result
SELECT id, 
       CASE WHEN id % 2 = 0 
            THEN named_struct('type', 'even', 'id', id)
            ELSE named_struct('type', 'odd', 'id', id)
       END as result
FROM (VALUES (1), (2), (3), (4)) t(id)
ORDER BY result;
-- result:
E: (1064, "Getting analyzing error. Detail message: Column 'result' cannot be resolved.")
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, named_struct('name', 'Bob', 'age', 30)),
        (3, named_struct('name', 'Charlie', 'age', 20))
) t(id, info)
WHERE info > named_struct('name', 'Alice', 'age', 20)
ORDER BY info;
-- result:
E: (1064, 'Getting analyzing error from line 7, column 6 to line 7, column 52. Detail message: Column type struct<name varchar, age tinyint(4)> does not support binary predicate operation with type struct<name varchar, age tinyint(4)>.')
-- !result
SELECT DISTINCT info FROM (
    VALUES 
        (named_struct('name', 'Alice', 'age', 25)),
        (named_struct('name', 'Bob', 'age', 30)),
        (named_struct('name', 'Alice', 'age', 25)),
        (named_struct('name', 'Charlie', 'age', 20))
) t(info)
ORDER BY info;
-- result:
{"name":"Alice","age":25}
{"name":"Bob","age":30}
{"name":"Charlie","age":20}
-- !result
DROP TABLE test_struct_edge FORCE;
-- result:
-- !result