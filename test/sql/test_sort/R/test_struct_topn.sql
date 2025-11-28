-- name: test_struct_topn
drop database if exists test_struct_topn;
-- result:
-- !result
create database test_struct_topn;
-- result:
-- !result
use test_struct_topn;
-- result:
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'score', 95)),
        (2, named_struct('name', 'Bob', 'score', 88)),
        (3, named_struct('name', 'Charlie', 'score', 92)),
        (4, named_struct('name', 'David', 'score', 85)),
        (5, named_struct('name', 'Eve', 'score', 90))
) t(id, info) 
ORDER BY info 
LIMIT 3;
-- result:
1	{"name":"Alice","score":95}
2	{"name":"Bob","score":88}
3	{"name":"Charlie","score":92}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'score', 95)),
        (2, named_struct('name', 'Bob', 'score', 88)),
        (3, named_struct('name', 'Charlie', 'score', 92)),
        (4, named_struct('name', 'David', 'score', 85)),
        (5, named_struct('name', 'Eve', 'score', 90))
) t(id, info) 
ORDER BY info DESC 
LIMIT 3;
-- result:
5	{"name":"Eve","score":90}
4	{"name":"David","score":85}
3	{"name":"Charlie","score":92}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25, 'scores', [90, 85, 88])),
        (2, named_struct('name', 'Bob', 'age', 30, 'scores', [88, 92, 85])),
        (3, named_struct('name', 'Charlie', 'age', 22, 'scores', [95, 90, 93]))
) t(id, info) 
ORDER BY info 
LIMIT 2;
-- result:
1	{"name":"Alice","age":25,"scores":[90,85,88]}
2	{"name":"Bob","age":30,"scores":[88,92,85]}
-- !result
SELECT * FROM (
    VALUES 
        (1, named_struct('name', 'Alice', 'age', 25), named_struct('city', 'Beijing', 'country', 'China')),
        (2, named_struct('name', 'Bob', 'age', 30), named_struct('city', 'Tokyo', 'country', 'Japan')),
        (3, named_struct('name', 'Alice', 'age', 30), named_struct('city', 'Beijing', 'country', 'China'))
) t(id, person, location) 
ORDER BY person, location 
LIMIT 2;
-- result:
1	{"name":"Alice","age":25}	{"city":"Beijing","country":"China"}
3	{"name":"Alice","age":30}	{"city":"Beijing","country":"China"}
-- !result
WITH data AS (
    SELECT * FROM (
        VALUES 
            (1, named_struct('name', 'Alice', 'dept', 'IT')),
            (2, named_struct('name', 'Bob', 'dept', 'HR')),
            (3, named_struct('name', 'Charlie', 'dept', 'IT')),
            (4, named_struct('name', 'David', 'dept', 'Sales'))
    ) t(id, employee)
)
SELECT * FROM data ORDER BY employee LIMIT 3;
-- result:
1	{"name":"Alice","dept":"IT"}
2	{"name":"Bob","dept":"HR"}
3	{"name":"Charlie","dept":"IT"}
-- !result
CREATE TABLE IF NOT EXISTS test_struct_topn (
    id INT,
    info STRUCT<category STRING, value INT, timestamp DATETIME>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_struct_topn VALUES
    (1, named_struct('category', 'A', 'value', 100, 'timestamp', '2024-01-01 10:00:00')),
    (2, named_struct('category', 'B', 'value', 200, 'timestamp', '2024-01-02 10:00:00')),
    (3, named_struct('category', 'A', 'value', 150, 'timestamp', '2024-01-03 10:00:00')),
    (4, named_struct('category', 'C', 'value', 180, 'timestamp', '2024-01-04 10:00:00')),
    (5, named_struct('category', 'B', 'value', 120, 'timestamp', '2024-01-05 10:00:00')),
    (6, named_struct('category', 'A', 'value', 100, 'timestamp', '2024-01-06 10:00:00')),
    (7, named_struct('category', 'C', 'value', 220, 'timestamp', '2024-01-07 10:00:00')),
    (8, named_struct('category', 'B', 'value', 190, 'timestamp', '2024-01-08 10:00:00')),
    (9, named_struct('category', 'A', 'value', 110, 'timestamp', '2024-01-09 10:00:00')),
    (10, named_struct('category', 'C', 'value', 160, 'timestamp', '2024-01-10 10:00:00'));
-- result:
-- !result
SELECT * FROM test_struct_topn ORDER BY info LIMIT 5;
-- result:
1	{"category":"A","value":100,"timestamp":"2024-01-01 10:00:00"}
6	{"category":"A","value":100,"timestamp":"2024-01-06 10:00:00"}
9	{"category":"A","value":110,"timestamp":"2024-01-09 10:00:00"}
3	{"category":"A","value":150,"timestamp":"2024-01-03 10:00:00"}
5	{"category":"B","value":120,"timestamp":"2024-01-05 10:00:00"}
-- !result
SELECT * FROM test_struct_topn ORDER BY info DESC LIMIT 5;
-- result:
7	{"category":"C","value":220,"timestamp":"2024-01-07 10:00:00"}
4	{"category":"C","value":180,"timestamp":"2024-01-04 10:00:00"}
10	{"category":"C","value":160,"timestamp":"2024-01-10 10:00:00"}
2	{"category":"B","value":200,"timestamp":"2024-01-02 10:00:00"}
8	{"category":"B","value":190,"timestamp":"2024-01-08 10:00:00"}
-- !result
SELECT * FROM test_struct_topn ORDER BY info LIMIT 3 OFFSET 2;
-- result:
9	{"category":"A","value":110,"timestamp":"2024-01-09 10:00:00"}
3	{"category":"A","value":150,"timestamp":"2024-01-03 10:00:00"}
5	{"category":"B","value":120,"timestamp":"2024-01-05 10:00:00"}
-- !result
SELECT * FROM test_struct_topn WHERE id > 3 ORDER BY info LIMIT 4;
-- result:
6	{"category":"A","value":100,"timestamp":"2024-01-06 10:00:00"}
9	{"category":"A","value":110,"timestamp":"2024-01-09 10:00:00"}
5	{"category":"B","value":120,"timestamp":"2024-01-05 10:00:00"}
8	{"category":"B","value":190,"timestamp":"2024-01-08 10:00:00"}
-- !result
SELECT id, info, 
       ROW_NUMBER() OVER (ORDER BY info) as rn
FROM test_struct_topn 
ORDER BY info 
LIMIT 5;
-- result:
1	{"category":"A","value":100,"timestamp":"2024-01-01 10:00:00"}	1
6	{"category":"A","value":100,"timestamp":"2024-01-06 10:00:00"}	2
9	{"category":"A","value":110,"timestamp":"2024-01-09 10:00:00"}	3
3	{"category":"A","value":150,"timestamp":"2024-01-03 10:00:00"}	4
5	{"category":"B","value":120,"timestamp":"2024-01-05 10:00:00"}	5
-- !result
DROP TABLE test_struct_topn FORCE;
-- result:
-- !result