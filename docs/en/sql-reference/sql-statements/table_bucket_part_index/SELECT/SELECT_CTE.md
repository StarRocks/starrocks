---
displayed_sidebar: docs
sidebar_label: "CTE"
---

# Common Table Expression

You can use a common table expression (CTE) to define a temporary result set that you can reference possibly multiple times within the scope of a SQL statement.

## WITH

A clause that can be added before a SELECT statement to define an alias for a complex expression that is referenced multiple times inside SELECT.

Similar to CRATE VIEW, but the table and column names defined in the clause do not persist after the query ends and do not conflict with names in the actual table or VIEW.

The benefits of using a WITH clause are:

Convenient and easy to maintain, reducing duplication within queries.

It is easier to read and understand SQL code by abstracting the most complex parts of a query into separate blocks.

Examples:

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the initial stage of the UNION ALL query.

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

## RECURSIVE

From v4.1 onwards, StarRocks supports Recursive Common Table Expression (CTE), using an iterative execution approach to efficiently handle various hierarchical and tree-structured data.

A Recursive CTE is a special type of CTE that can reference itself, enabling recursive queries. Recursive CTEs are particularly useful for processing hierarchical data structures such as organizational charts, file systems, graph traversals, and more.

A Recursive CTE consists of the following components:

- **Anchor Member**: A non-recursive initial query that provides the starting dataset for recursion.
- **Recursive Member**: A recursive query that references the CTE itself.
- **Termination Condition**: A condition to prevent infinite recursion, typically implemented through a WHERE clause.

The execution process of a Recursive CTE is as follows:

1. Execute the anchor member to obtain the initial result set (level 0).
2. Use level 0 results as input, execute the recursive member to get level 1 results.
3. Use level 1 results as input, execute the recursive member again to get level 2 results.
4. Repeat this process until the recursive member returns no rows or reaches the maximum recursion depth.
5. Merge all levels of results using UNION ALL (or UNION).

:::tip
You must enable this feature by setting the system variable `enable_recursive_cte` to `true` before using it.
:::

### Syntax

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

### Parameters

- `cte_name`: Name of the CTE.
- `column_list` (optional): List of column names for the CTE result set.
- `anchor_query`: Initial query that must be non-recursive and cannot reference the CTE itself.
- `UNION`: Union operator.
  - `UNION ALL`: Retains all rows (including duplicates), recommended for better performance.
  - `UNION` or `UNION DISTINCT`: Removes duplicate rows.
- `recursive_query`: Recursive query that references the CTE itself.

### Limitations

Recursive CTEs in StarRocks have the following limitations:

- **Feature flag required**

  You must manually enable Recursive CTE by setting the system variable `enable_recursive_cte` to `true`.

- **Structural requirements**
  - UNION or UNION ALL must be used to connect anchor and recursive members.
  - The anchor member cannot reference the CTE itself.
  - If the recursive member does not reference the CTE itself, it is executed as a regular CTE.

- **Recursion depth limit**
  - By default, the maximum recursion depth is 5 (levels).
  - The maximum depth can be adjusted via the system variable `recursive_cte_max_depth` to prevent infinite recursion.

- **Execution Constraints**
  - Currently, multi-level nested recursive CTEs are not supported.
  - Complex recursive CTEs may lead to degraded performance.
  - Constants in `anchor_query` should have types consistent with `recursive_query` output types.

### Configurations

The following system variables are required for using Recursive CTE:

| Variable Name             | Type    | Default | Description                                            |
| ------------------------- | ------- | ------- | ------------------------------------------------------ |
| `enable_recursive_cte`    | BOOLEAN | false   | Whether to enable Recursive CTE.                       |
| `recursive_cte_max_depth` | INT     | 5       | Maximum recursion depth to prevent infinite recursion. |

### Examples

**Example 1: Querying Organizational Hierarchy**

Querying organizational hierarchy is one of the most common use cases for Recursive CTE. The following example queries employee organizational hierarchy relationships.

1. Prepare data:

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

2. Query Organizational Hierarchy:

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

Result:

```Plain
+-------------+---------+----------------------+-------+-----------------------------------------+
| employee_id | name    | title                | level | path                                    |
+-------------+---------+----------------------+-------+-----------------------------------------+
|           1 | Alicia  | CEO                  |     1 | Alicia                                  |
|           2 | Bob     | CTO                  |     2 | Alicia -> Bob                           |
|           3 | Carol   | CFO                  |     2 | Alicia -> Carol                         |
|           4 | David   | VP of Engineering    |     3 | Alicia -> Bob -> David                  |
|           5 | Eve     | VP of Research       |     3 | Alicia -> Bob -> Eve                    |
|           6 | Frank   | VP of Finance        |     3 | Alicia -> Carol -> Frank                |
|           7 | Grace   | Engineering Manager  |     4 | Alicia -> Bob -> David -> Grace         |
|           8 | Heidi   | Tech Lead            |     4 | Alicia -> Bob -> David -> Heidi         |
|           9 | Ivan    | Research Manager     |     4 | Alicia -> Bob -> Eve -> Ivan            |
|          10 | Judy    | Senior Engineer      |     5 | Alicia -> Bob -> David -> Grace -> Judy |
+-------------+---------+----------------------+-------+-----------------------------------------+
```

**Example 2: Multiple Recursive CTEs**

You can define multiple recursive CTEs in a single query.

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
