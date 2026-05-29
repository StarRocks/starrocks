---
displayed_sidebar: docs
sidebar_label: "CTE"
---

# Common Table Expression

您可以使用 Common Table Expression（CTE）来定义一个临时结果集，并在 SQL 语句的作用域内多次引用该结果集。

## WITH

一个可以添加到 SELECT 语句之前的子句，用于为一个复杂的表达式定义别名，该表达式在 SELECT 内部被多次引用。

类似于 CREATE VIEW，但是子句中定义的表名和列名在查询结束后不会持久存在，并且不会与实际表或 VIEW 中的名称冲突。

使用 WITH 子句的好处是：

方便且易于维护，减少查询中的重复。

通过将查询中最复杂的部分抽象成单独的块，可以更容易地阅读和理解 SQL 代码。

示例：

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the initial stage of the UNION ALL query.

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

## RECURSIVE

从 v4.1 版本开始，StarRocks 支持递归 CTE，它使用迭代执行方法来高效地处理各种层级和树状结构数据。

递归 CTE 是一种特殊的 CTE，它可以引用自身，从而实现递归查询。递归 CTE 特别适用于处理层级数据结构，例如组织结构图、文件系统、图遍历等。

递归 CTE 由以下组件构成：

- **Anchor Member（起始成员）**：一个非递归的初始查询，为递归提供起始数据集。
- **Recursive Member（递归成员）**：一个引用 CTE 自身的递归查询。
- **Termination Condition（终止条件）**：一个防止无限递归的条件，通常通过 WHERE 子句实现。

递归 CTE 的执行过程如下：

1. 执行起始成员以获得初始结果集（第 0 层）。
2. 使用第 0 层的结果作为输入，执行递归成员以获得第 1 层的结果。
3. 使用第 1 层的结果作为输入，再次执行递归成员以获得第 2 层的结果。
4. 重复此过程，直到递归成员不返回任何行或达到最大递归深度。
5. 使用 UNION ALL（或 UNION）合并所有层级的结果。

:::tip
您必须先将系统变量 `enable_recursive_cte` 设置为 `true` 才能启用此功能。
:::

### 语法

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

### 参数

- `cte_name`: CTE 的名称。
- `column_list` (可选): CTE 结果集的列名列表。
- `anchor_query`: 初始查询，必须是非递归的，并且不能引用 CTE 本身。
- `UNION`: Union 操作符。
  - `UNION ALL`: 保留所有行（包括重复行），建议使用以获得更好的查询性能。
  - `UNION` 或 `UNION DISTINCT`: 删除重复行。
- `recursive_query`: 引用 CTE 本身的递归查询。

### 限制

StarRocks 中的递归 CTE 具有以下限制：

- **需要开启功能标志**

  您必须手动开启递归 CTE，方法是将系统变量 `enable_recursive_cte` 设置为 `true`。

- **结构要求**
  - 必须使用 UNION 或 UNION ALL 来连接初始成员和递归成员。
  - 初始成员不能引用 CTE 本身。
  - 如果递归成员不引用 CTE 本身，则将其作为常规 CTE 执行。

- **递归深度限制**
  - 默认情况下，最大递归深度为 5（层）。
  - 可以通过系统变量 `recursive_cte_max_depth` 调整最大深度，以防止无限递归。

- **执行约束**
  - 目前，不支持多层嵌套递归 CTE。
  - 复杂的递归 CTE 可能会导致性能下降。
  - `anchor_query` 中的常量应具有与 `recursive_query` 输出类型一致的类型。

### 配置

使用递归 CTE 需要以下系统变量：

| 变量名                      | 类型    | 默认值  | 描述                                               |
| --------------------------- | ------- | ------- | -------------------------------------------------- |
| `enable_recursive_cte`      | BOOLEAN | false   | 是否开启递归 CTE。                                 |
| `recursive_cte_max_depth`   | INT     | 5       | 最大递归深度，以防止无限递归。                       |

### 示例

**示例 1：查询组织层级**

查询组织层级是递归 CTE 最常见的用例之一。以下示例查询员工组织层级关系。

1. 准备数据：

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

2. 查询数据库模式层次结构：

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

结果：

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

**示例 2：多个递归 CTE**

您可以在单个查询中定义多个递归 CTE。

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
