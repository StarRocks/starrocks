# Recursive CTE (Recursive Common Table Expression)

## Overview

A Recursive Common Table Expression (Recursive CTE) is a special type of CTE (Common Table Expression) that can reference itself, enabling recursive queries. Recursive CTEs are particularly useful for processing hierarchical data structures such as organizational charts, file systems, graph traversals, and more.

StarRocks supports Recursive CTE functionality starting from version 4.1, using an iterative execution approach to efficiently handle various hierarchical and tree-structured data.

## Basic Concepts

A Recursive CTE consists of the following components:

1. **Anchor Member**: A non-recursive initial query that provides the starting dataset for recursion
2. **Recursive Member**: A recursive query that references the CTE itself
3. **Termination Condition**: A condition to prevent infinite recursion, typically implemented through a WHERE clause

Execution process of a Recursive CTE:

1. Execute the anchor member to obtain the initial result set (level 0)
2. Use the initial results as input, execute the recursive member to get level 1 results
3. Use level 1 results as input, execute the recursive member again to get level 2 results
4. Repeat this process until the recursive member returns no rows or reaches the maximum recursion depth
5. Merge all levels of results using UNION ALL (or UNION)

## Syntax

```sql
WITH RECURSIVE cte_name [(column_list)] AS (
    -- Anchor member (non-recursive part)
    anchor_query
    UNION [ALL | DISTINCT]
    -- Recursive member (recursive part)
    recursive_query
)
SELECT ... FROM cte_name ...;
```

**Parameter Description:**

- `cte_name`: Name of the CTE
- `column_list` (optional): List of column names for the CTE result set
- `anchor_query`: Non-recursive initial query that cannot reference the CTE itself
- `UNION [ALL | DISTINCT]`: Union operator
  - `UNION ALL`: Retains all rows (including duplicates), recommended for better performance
  - `UNION`: Removes duplicate rows
- `recursive_query`: Recursive query that references the CTE itself

## Limitations

Recursive CTEs in StarRocks have the following limitations:

1. **Feature Flag Required**: Must set `enable_recursive_cte = true` to use Recursive CTE

2. **Structural Requirements**:
   - Recursive CTEs must use UNION or UNION ALL to connect anchor and recursive members
   - Anchor member cannot reference the CTE itself
   - If the recursive member does not reference the CTE itself, it executes as a regular CTE

3. **Recursion Depth Limit**:
   - Default maximum recursion depth is 5 levels
   - Can be adjusted via `recursive_cte_max_depth` parameter to prevent infinite recursion

4. **Execution Constraints**:
   - Current version does not support multi-level nested recursive CTEs
   - Complex recursive CTEs may have weaker performance
   - Constants in `anchor_query` should have types consistent with `recursive_query` output types

## Session Variables

The following session variables are required for using Recursive CTE:

| Variable Name | Type | Default | Description |
|---------------|------|---------|-------------|
| `enable_recursive_cte` | BOOLEAN | false | Whether to enable Recursive CTE functionality |
| `recursive_cte_max_depth` | INT | 5 | Maximum recursion depth to prevent infinite recursion |


## Examples

### Example 1: Querying Organizational Hierarchy
This is one of the most common use cases for Recursive CTE, querying employee organizational hierarchy relationships.

**Prepare Data:**

```sql
CREATE TABLE employees (
  employee_id INT,
  name VARCHAR(100),
  manager_id INT,
  title VARCHAR(50)
) DUPLICATE KEY(employee_id)
DISTRIBUTED BY RANDOM;

INSERT INTO employees VALUES
(1, 'Alicia', NULL, 'CEO'),
(2, 'Bob', 1, 'CTO'),
(3, 'Carol', 1, 'CFO'),
(4, 'David', 2, 'VP of Engineering'),
(5, 'Eve', 2, 'VP of Research'),
(6, 'Frank', 3, 'VP of Finance'),
(7, 'Grace', 4, 'Engineering Manager'),
(8, 'Heidi', 4, 'Tech Lead'),
(9, 'Ivan', 5, 'Research Manager'),
(10, 'Judy', 7, 'Senior Engineer');
```

**Query Organizational Hierarchy:**

```sql
WITH RECURSIVE org_hierarchy AS (
    -- Anchor member: Start from CEO (employee with no manager)
    SELECT 
        employee_id, 
        name, 
        manager_id, 
        title, 
        CAST(1 AS BIGINT) AS level,
        name AS path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive member: Find subordinates at next level
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        e.title,
        oh.level + 1,
        CONCAT(oh.path, ' -> ', e.name) AS path
    FROM employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true) */
    employee_id,
    name,
    title,
    level,
    path
FROM org_hierarchy
ORDER BY employee_id;
```

**Result:**

```
+-------------+---------+----------------------+-------+----------------------------------+
| employee_id | name    | title                | level | path                             |
+-------------+---------+----------------------+-------+----------------------------------+
|           1 | Alicia  | CEO                  |     1 | Alicia                           |
|           2 | Bob     | CTO                  |     2 | Alicia -> Bob                    |
|           3 | Carol   | CFO                  |     2 | Alicia -> Carol                  |
|           4 | David   | VP of Engineering    |     3 | Alicia -> Bob -> David           |
|           5 | Eve     | VP of Research       |     3 | Alicia -> Bob -> Eve             |
|           6 | Frank   | VP of Finance        |     3 | Alicia -> Carol -> Frank         |
|           7 | Grace   | Engineering Manager  |     4 | Alicia -> Bob -> David -> Grace  |
|           8 | Heidi   | Tech Lead            |     4 | Alicia -> Bob -> David -> Heidi  |
|           9 | Ivan    | Research Manager     |     4 | Alicia -> Bob -> Eve -> Ivan     |
|          10 | Judy    | Senior Engineer      |     5 | Alicia -> Bob -> David -> Grace -> Judy |
+-------------+---------+----------------------+-------+----------------------------------+
```

### Example 2: Multiple Recursive CTEs

You can define multiple recursive CTEs in a single query:

```sql
WITH RECURSIVE
cte1 AS (
    SELECT CAST(1 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte1 WHERE n < 5
),
cte2 AS (
    SELECT CAST(10 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte2 WHERE n < 15
)
SELECT /*+ SET_VAR(enable_recursive_cte=true) */
    'cte1' AS source,
    n
FROM cte1
UNION ALL
SELECT 
    'cte2' AS source,
    n
FROM cte2
ORDER BY source, n;
```
