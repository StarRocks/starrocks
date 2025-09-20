-- name: test_join_with_using
DROP DATABASE IF EXISTS test_join_with_using;
CREATE DATABASE test_join_with_using;
use test_join_with_using;

CREATE TABLE left_table (
    name VARCHAR(20),
    id1 INT,
    age INT,
    city VARCHAR(20),
    id2 INT,
    salary DECIMAL(10,2),
    status VARCHAR(10)
) DUPLICATE KEY(name) DISTRIBUTED BY HASH(name) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE right_table (
    dept VARCHAR(20),
    id1 INT,
    bonus DECIMAL(8,2),
    id2 INT
) DUPLICATE KEY(dept) DISTRIBUTED BY HASH(dept) BUCKETS 1 PROPERTIES ("replication_num" = "1");

INSERT INTO left_table VALUES
    ('Alice', 1, 25, 'New York', 100, 5000.00, 'Active'),
    ('Bob', 2, 30, 'Boston', 200, 6000.00, 'Active'),
    ('Charlie', 3, 35, 'Chicago', 300, 7000.00, 'Inactive'),
    ('David', 4, 28, 'Denver', NULL, 5500.00, 'Active'),
    ('Eve', 5, 32, 'Seattle', 500, 6500.00, 'Active'),
    ('Frank', 6, 40, NULL, 600, 8000.00, 'Inactive');

INSERT INTO right_table VALUES 
    ('Engineering', 1, 1000.00, 100),
    ('Marketing', 2, 800.00, 200),
    ('Sales', 7, 1200.00, 700),
    ('HR', 8, 900.00, NULL),
    ('Finance', 9, NULL, 900);

-- Test INNER JOIN with two USING columns
SELECT * FROM left_table JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test INNER JOIN selecting only USING columns
SELECT id1, id2 FROM left_table JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test LEFT OUTER JOIN
SELECT * FROM left_table LEFT JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test LEFT OUTER JOIN selecting only USING columns
SELECT id1, id2 FROM left_table LEFT JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test RIGHT OUTER JOIN (critical test for column ordering)
SELECT * FROM left_table RIGHT JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test RIGHT OUTER JOIN selecting only USING columns (should prefer right table values)
SELECT id1, id2 FROM left_table RIGHT JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test FULL OUTER JOIN
SELECT * FROM left_table FULL OUTER JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test FULL OUTER JOIN selecting only USING columns
SELECT id1, id2 FROM left_table FULL OUTER JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test with table aliases and qualified references
SELECT L.id1, R.id1, id1 FROM left_table L JOIN right_table R USING(id1, id2) ORDER BY L.id1;

-- Test single USING column
SELECT * FROM left_table JOIN right_table USING(id1) ORDER BY id1;

-- Test NULL handling in USING columns
SELECT * FROM left_table LEFT JOIN right_table USING(id1, id2) WHERE id1 IS NULL OR id2 IS NULL;

-- Test RIGHT SEMI JOIN
SELECT * FROM left_table RIGHT SEMI JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test RIGHT ANTI JOIN
SELECT * FROM left_table RIGHT ANTI JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test CROSS JOIN
SELECT * FROM left_table CROSS JOIN right_table ORDER BY left_table.id1, right_table.id1 LIMIT 5;

-- Test LEFT SEMI JOIN
SELECT * FROM left_table LEFT SEMI JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test LEFT ANTI JOIN
SELECT * FROM left_table LEFT ANTI JOIN right_table USING(id1, id2) ORDER BY id1;

-- Test single column USING cases
SELECT * FROM left_table JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table LEFT JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table RIGHT JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table FULL OUTER JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table LEFT SEMI JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table LEFT ANTI JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table RIGHT SEMI JOIN right_table USING(id1) ORDER BY id1;

SELECT * FROM left_table RIGHT ANTI JOIN right_table USING(id1) ORDER BY id1;

-- Test single column USING with aliases
SELECT id1 AS join_key FROM left_table JOIN right_table USING(id1) ORDER BY join_key;

SELECT l.id1 l_id1 FROM left_table l JOIN right_table r USING(id1) ORDER BY l_id1;

SELECT l.id2 l_id1 FROM left_table l right JOIN right_table r USING(id1) ORDER BY l_id1;

SELECT r.id2 l_id1 FROM left_table l right JOIN right_table r USING(id1) ORDER BY l_id1;

SELECT id1 AS key1, id2 AS key2 FROM left_table JOIN right_table USING(id1, id2) ORDER BY key1;

-- Test USING column with table alias and column alias
SELECT L.id1 AS left_key, R.id1 AS right_key, id1 AS coalesced_key 
FROM left_table L JOIN right_table R USING(id1) ORDER BY left_key;

-- Test RIGHT JOIN with single column and alias (should prefer right table value)
SELECT id1 AS right_preferred_key FROM left_table RIGHT JOIN right_table USING(id1) ORDER BY right_preferred_key;

-- Test complex alias scenarios
SELECT 
    id1 AS main_id,
    name AS emp_name,
    dept AS department,
    salary AS emp_salary,
    bonus AS emp_bonus
FROM left_table LEFT JOIN right_table USING(id1, id2) 
ORDER BY main_id;

-- Test USING with WHERE clause on aliased columns
SELECT id1 AS pk, name, dept 
FROM left_table LEFT JOIN right_table USING(id1) 
WHERE id1 > 2 
ORDER BY pk;

-- Test NULL AWARE LEFT ANTI JOIN (if supported)
SELECT * FROM left_table WHERE id1 NOT IN (SELECT id1 FROM right_table WHERE id1 IS NOT NULL) ORDER BY id1;

-- Test edge cases with mixed USING columns and aliases
SELECT 
    id1 AS shared_key,
    COALESCE(name, 'N/A') AS employee,
    COALESCE(dept, 'No Dept') AS department
FROM left_table FULL OUTER JOIN right_table USING(id1)
ORDER BY shared_key;

-- Test USING columns in different SELECT positions
SELECT dept, id1, name FROM left_table LEFT JOIN right_table USING(id1) ORDER BY id1;

-- Test multiple single-column USING joins in sequence (subquery style)
SELECT * FROM (
    SELECT id1, name, age FROM left_table WHERE id1 <= 5
) L JOIN (
    SELECT id1, dept FROM right_table WHERE id1 <= 5  
) R USING(id1) ORDER BY id1;

-- Test subquery with JOIN USING as inner query
SELECT outer_query.id1, outer_query.total_count
FROM (
    SELECT id1, COUNT(*) as total_count
    FROM left_table L JOIN right_table R USING(id1)
    GROUP BY id1
) outer_query
WHERE outer_query.total_count > 0
ORDER BY outer_query.id1;

-- Test JOIN USING with subquery in WHERE clause
SELECT id1, name, R.dept
FROM left_table L JOIN right_table R USING(id1)
WHERE L.id1 IN (
    SELECT id1 FROM left_table WHERE salary > 6000
)
ORDER BY L.id1;

-- Test complex subquery with multiple USING columns
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

-- Test JOIN USING between subqueries with aggregation
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

