-- name: test_join_with_using
DROP DATABASE IF EXISTS test_join_with_using;
-- result:
-- !result
CREATE DATABASE test_join_with_using;
-- result:
-- !result
use test_join_with_using;
-- result:
-- !result
CREATE TABLE left_table (
    name VARCHAR(20),
    id1 INT,
    age INT,
    city VARCHAR(20),
    id2 INT,
    salary DECIMAL(10,2),
    status VARCHAR(10)
) DUPLICATE KEY(name) DISTRIBUTED BY HASH(name) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE right_table (
    dept VARCHAR(20),
    id1 INT,
    bonus DECIMAL(8,2),
    id2 INT
) DUPLICATE KEY(dept) DISTRIBUTED BY HASH(dept) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO left_table VALUES
    ('Alice', 1, 25, 'New York', 100, 5000.00, 'Active'),
    ('Bob', 2, 30, 'Boston', 200, 6000.00, 'Active'),
    ('Charlie', 3, 35, 'Chicago', 300, 7000.00, 'Inactive'),
    ('David', 4, 28, 'Denver', NULL, 5500.00, 'Active'),
    ('Eve', 5, 32, 'Seattle', 500, 6500.00, 'Active'),
    ('Frank', 6, 40, NULL, 600, 8000.00, 'Inactive');
-- result:
-- !result
INSERT INTO right_table VALUES 
    ('Engineering', 1, 1000.00, 100),
    ('Marketing', 2, 800.00, 200),
    ('Sales', 7, 1200.00, 700),
    ('HR', 8, 900.00, NULL),
    ('Finance', 9, NULL, 900);
-- result:
-- !result
SELECT * FROM left_table JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100	Alice	25	New York	5000.00	Active	Engineering	1000.00
2	200	Bob	30	Boston	6000.00	Active	Marketing	800.00
-- !result
SELECT id1, id2 FROM left_table JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100
2	200
-- !result
SELECT * FROM left_table LEFT JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100	Alice	25	New York	5000.00	Active	Engineering	1000.00
2	200	Bob	30	Boston	6000.00	Active	Marketing	800.00
3	300	Charlie	35	Chicago	7000.00	Inactive	None	None
4	None	David	28	Denver	5500.00	Active	None	None
5	500	Eve	32	Seattle	6500.00	Active	None	None
6	600	Frank	40	None	8000.00	Inactive	None	None
-- !result
SELECT id1, id2 FROM left_table LEFT JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100
2	200
3	300
4	None
5	500
6	600
-- !result
SELECT * FROM left_table RIGHT JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100	Engineering	1000.00	Alice	25	New York	5000.00	Active
2	200	Marketing	800.00	Bob	30	Boston	6000.00	Active
7	700	Sales	1200.00	None	None	None	None	None
8	None	HR	900.00	None	None	None	None	None
9	900	Finance	None	None	None	None	None	None
-- !result
SELECT id1, id2 FROM left_table RIGHT JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100
2	200
7	700
8	None
9	900
-- !result
SELECT * FROM left_table FULL OUTER JOIN right_table USING(id1, id2) ORDER BY right_table.dept, right_table.bonus;
-- result:
Eve	5	32	Seattle	500	6500.00	Active	None	None	None	None
Charlie	3	35	Chicago	300	7000.00	Inactive	None	None	None	None
David	4	28	Denver	None	5500.00	Active	None	None	None	None
Frank	6	40	None	600	8000.00	Inactive	None	None	None	None
Alice	1	25	New York	100	5000.00	Active	Engineering	1	1000.00	100
None	None	None	None	None	None	None	Finance	9	None	900
None	None	None	None	None	None	None	HR	8	900.00	None
Bob	2	30	Boston	200	6000.00	Active	Marketing	2	800.00	200
None	None	None	None	None	None	None	Sales	7	1200.00	700
-- !result
SELECT L.id1, R.id1, id1 FROM left_table L JOIN right_table R USING(id1, id2) ORDER BY L.id1;
-- result:
1	1	1
2	2	2
-- !result
SELECT * FROM left_table JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Alice	25	New York	100	5000.00	Active	Engineering	1000.00	100
2	Bob	30	Boston	200	6000.00	Active	Marketing	800.00	200
-- !result
SELECT * FROM left_table LEFT JOIN right_table USING(id1, id2) WHERE id1 IS NULL OR id2 IS NULL;
-- result:
4	None	David	28	Denver	5500.00	Active	None	None
-- !result
SELECT * FROM left_table RIGHT SEMI JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100	Engineering	1000.00
2	200	Marketing	800.00
-- !result
SELECT * FROM left_table RIGHT ANTI JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
7	700	Sales	1200.00
8	None	HR	900.00
9	900	Finance	None
-- !result
SELECT * FROM left_table CROSS JOIN right_table ORDER BY left_table.id1, right_table.id1 LIMIT 5;
-- result:
Alice	1	25	New York	100	5000.00	Active	Engineering	1	1000.00	100
Alice	1	25	New York	100	5000.00	Active	Marketing	2	800.00	200
Alice	1	25	New York	100	5000.00	Active	Sales	7	1200.00	700
Alice	1	25	New York	100	5000.00	Active	HR	8	900.00	None
Alice	1	25	New York	100	5000.00	Active	Finance	9	None	900
-- !result
SELECT * FROM left_table LEFT SEMI JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
1	100	Alice	25	New York	5000.00	Active
2	200	Bob	30	Boston	6000.00	Active
-- !result
SELECT * FROM left_table LEFT ANTI JOIN right_table USING(id1, id2) ORDER BY id1;
-- result:
3	300	Charlie	35	Chicago	7000.00	Inactive
4	None	David	28	Denver	5500.00	Active
5	500	Eve	32	Seattle	6500.00	Active
6	600	Frank	40	None	8000.00	Inactive
-- !result
SELECT * FROM left_table JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Alice	25	New York	100	5000.00	Active	Engineering	1000.00	100
2	Bob	30	Boston	200	6000.00	Active	Marketing	800.00	200
-- !result
SELECT * FROM left_table LEFT JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Alice	25	New York	100	5000.00	Active	Engineering	1000.00	100
2	Bob	30	Boston	200	6000.00	Active	Marketing	800.00	200
3	Charlie	35	Chicago	300	7000.00	Inactive	None	None	None
4	David	28	Denver	None	5500.00	Active	None	None	None
5	Eve	32	Seattle	500	6500.00	Active	None	None	None
6	Frank	40	None	600	8000.00	Inactive	None	None	None
-- !result
SELECT * FROM left_table RIGHT JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Engineering	1000.00	100	Alice	25	New York	100	5000.00	Active
2	Marketing	800.00	200	Bob	30	Boston	200	6000.00	Active
7	Sales	1200.00	700	None	None	None	None	None	None
8	HR	900.00	None	None	None	None	None	None	None
9	Finance	None	900	None	None	None	None	None	None
-- !result
SELECT * FROM left_table FULL OUTER JOIN right_table USING(id1) ORDER BY right_table.dept, right_table.bonus;
-- result:
Eve	5	32	Seattle	500	6500.00	Active	None	None	None	None
David	4	28	Denver	None	5500.00	Active	None	None	None	None
Charlie	3	35	Chicago	300	7000.00	Inactive	None	None	None	None
Frank	6	40	None	600	8000.00	Inactive	None	None	None	None
Alice	1	25	New York	100	5000.00	Active	Engineering	1	1000.00	100
None	None	None	None	None	None	None	Finance	9	None	900
None	None	None	None	None	None	None	HR	8	900.00	None
Bob	2	30	Boston	200	6000.00	Active	Marketing	2	800.00	200
None	None	None	None	None	None	None	Sales	7	1200.00	700
-- !result
SELECT * FROM left_table LEFT SEMI JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Alice	25	New York	100	5000.00	Active
2	Bob	30	Boston	200	6000.00	Active
-- !result
SELECT * FROM left_table LEFT ANTI JOIN right_table USING(id1) ORDER BY id1;
-- result:
3	Charlie	35	Chicago	300	7000.00	Inactive
4	David	28	Denver	None	5500.00	Active
5	Eve	32	Seattle	500	6500.00	Active
6	Frank	40	None	600	8000.00	Inactive
-- !result
SELECT * FROM left_table RIGHT SEMI JOIN right_table USING(id1) ORDER BY id1;
-- result:
1	Engineering	1000.00	100
2	Marketing	800.00	200
-- !result
SELECT * FROM left_table RIGHT ANTI JOIN right_table USING(id1) ORDER BY id1;
-- result:
7	Sales	1200.00	700
8	HR	900.00	None
9	Finance	None	900
-- !result
SELECT id1 AS join_key FROM left_table JOIN right_table USING(id1) ORDER BY join_key;
-- result:
1
2
-- !result
SELECT l.id1 l_id1 FROM left_table l JOIN right_table r USING(id1) ORDER BY l_id1;
-- result:
1
2
-- !result
SELECT l.id2 l_id1 FROM left_table l right JOIN right_table r USING(id1) ORDER BY l_id1;
-- result:
None
None
None
100
200
-- !result
SELECT r.id2 l_id1 FROM left_table l right JOIN right_table r USING(id1) ORDER BY l_id1;
-- result:
None
100
200
700
900
-- !result
SELECT id1 AS key1, id2 AS key2 FROM left_table JOIN right_table USING(id1, id2) ORDER BY key1;
-- result:
1	100
2	200
-- !result
SELECT L.id1 AS left_key, R.id1 AS right_key, id1 AS coalesced_key 
FROM left_table L JOIN right_table R USING(id1) ORDER BY left_key;
-- result:
1	1	1
2	2	2
-- !result
SELECT id1 AS right_preferred_key FROM left_table RIGHT JOIN right_table USING(id1) ORDER BY right_preferred_key;
-- result:
1
2
7
8
9
-- !result
SELECT 
    id1 AS main_id,
    name AS emp_name,
    dept AS department,
    salary AS emp_salary,
    bonus AS emp_bonus
FROM left_table LEFT JOIN right_table USING(id1, id2) 
ORDER BY main_id;
-- result:
1	Alice	Engineering	5000.00	1000.00
2	Bob	Marketing	6000.00	800.00
3	Charlie	None	7000.00	None
4	David	None	5500.00	None
5	Eve	None	6500.00	None
6	Frank	None	8000.00	None
-- !result
SELECT id1 AS pk, name, dept 
FROM left_table LEFT JOIN right_table USING(id1) 
WHERE id1 > 2 
ORDER BY pk;
-- result:
3	Charlie	None
4	David	None
5	Eve	None
6	Frank	None
-- !result
SELECT * FROM left_table WHERE id1 NOT IN (SELECT id1 FROM right_table WHERE id1 IS NOT NULL) ORDER BY id1;
-- result:
Charlie	3	35	Chicago	300	7000.00	Inactive
David	4	28	Denver	None	5500.00	Active
Eve	5	32	Seattle	500	6500.00	Active
Frank	6	40	None	600	8000.00	Inactive
-- !result
SELECT dept, id1, name FROM left_table LEFT JOIN right_table USING(id1) ORDER BY id1;
-- result:
Engineering	1	Alice
Marketing	2	Bob
None	3	Charlie
None	4	David
None	5	Eve
None	6	Frank
-- !result
SELECT * FROM (
    SELECT id1, name, age FROM left_table WHERE id1 <= 5
) L JOIN (
    SELECT id1, dept FROM right_table WHERE id1 <= 5  
) R USING(id1) ORDER BY id1;
-- result:
1	Alice	25	Engineering
2	Bob	30	Marketing
-- !result
SELECT outer_query.id1, outer_query.total_count
FROM (
    SELECT id1, COUNT(*) as total_count
    FROM left_table L JOIN right_table R USING(id1)
    GROUP BY id1
) outer_query
WHERE outer_query.total_count > 0
ORDER BY outer_query.id1;
-- result:
1	1
2	1
-- !result
SELECT id1, name, R.dept
FROM left_table L JOIN right_table R USING(id1)
WHERE L.id1 IN (
    SELECT id1 FROM left_table WHERE salary > 6000
)
ORDER BY L.id1;
-- result:
-- !result
SELECT sub.id1, sub.id2, sub.emp_info, sub.dept_info
FROM (
    SELECT
        id1,
        id2,
        CONCAT(name, '-', CAST(age AS VARCHAR)) as emp_info,
        COALESCE(dept, 'Unknown') as dept_info
    FROM left_table L LEFT JOIN right_table R USING(id1, id2)
    WHERE id1 IS NOT NULL
) sub
WHERE sub.emp_info LIKE '%Alice%' OR sub.dept_info = 'Engineering'
ORDER BY sub.id1;
-- result:
1	100	Alice-25	Engineering
-- !result
SELECT
    L_AGG.id1,
    L_AGG.avg_salary,
    R_AGG.total_bonus
FROM (
    SELECT id1, AVG(salary) as avg_salary
    FROM left_table
    WHERE salary IS NOT NULL
    GROUP BY id1
) L_AGG
JOIN (
    SELECT id1, SUM(bonus) as total_bonus
    FROM right_table
    WHERE bonus IS NOT NULL
    GROUP BY id1
) R_AGG USING(id1)
ORDER BY L_AGG.id1;
-- result:
1	5000.00000000	1000.00
2	6000.00000000	800.00
-- !result