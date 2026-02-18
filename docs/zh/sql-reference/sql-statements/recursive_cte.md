# 递归 CTE（Recursive Common Table Expression）

## 功能介绍

递归公共表表达式（Recursive CTE）是一种特殊的 CTE（Common Table Expression），它可以引用自身，从而实现递归查询。递归 CTE 在处理层次结构数据（如组织架构、文件系统、图遍历等）时非常有用。

StarRocks 从 4.1 版本开始支持递归 CTE 功能，使用迭代执行方式实现递归查询，可以高效处理各种层次结构和树形数据。

## 基本概念

递归 CTE 由以下几个部分组成：

1. **锚点成员（Anchor Member）**：非递归的初始查询，提供递归的起始数据集
2. **递归成员（Recursive Member）**：引用 CTE 自身的递归查询
3. **终止条件**：防止无限递归的条件，通常通过 WHERE 子句实现

递归 CTE 的执行过程：

1. 执行锚点成员，获得初始结果集（第 0 层）
2. 将初始结果作为输入，执行递归成员，得到第 1 层结果
3. 将第 1 层结果作为输入，再次执行递归成员，得到第 2 层结果
4. 重复此过程，直到递归成员不返回任何行，或达到最大递归深度
5. 使用 UNION ALL（或 UNION）合并所有层的结果

## 语法

```sql
WITH RECURSIVE cte_name [(column_list)] AS (
    -- 锚点成员（非递归部分）
    anchor_query
    UNION [ALL | DISTINCT]
    -- 递归成员（递归部分）
    recursive_query
)
SELECT ... FROM cte_name ...;
```

**参数说明：**

- `cte_name`：CTE 的名称
- `column_list`（可选）：CTE 结果集的列名列表
- `anchor_query`：非递归的初始查询，不能引用 CTE 自身
- `UNION [ALL | DISTINCT]`：合并操作符
  - `UNION ALL`：保留所有行（包括重复行），推荐使用，性能更好
  - `UNION`：去除重复行
- `recursive_query`：递归查询，引用 CTE 自身

## 使用限制
递归 CTE 在 StarRocks 中有以下限制：

1. **必须启用功能开关**：需要设置 `enable_recursive_cte = true` 才能使用递归 CTE

2. **结构要求**：
   - 递归 CTE 必须都使用 UNION 或 UNION ALL 连接锚点成员和递归成员
   - 锚点成员不能引用 CTE 自身
   - 递归成员没有引用 CTE 自身时，作为普通 CTE 执行

3. **递归深度限制**：
   - 默认最大递归深度为 5 层
   - 可通过 `recursive_cte_max_depth` 参数调整，防止无限递归

4. **执行限制**：
   - 当前版本不支持多层嵌套的递归 CTE
   - 复杂的递归 CTE 性能较弱
   - `anchor_query` 上的常量需要注意类型与 `recursive_query` 输出类型一致

## 会话变量

使用递归 CTE 需要配置以下会话变量：

| 变量名                    | 类型    | 默认值 | 说明                         |
| ------------------------- | ------- | ------ | ---------------------------- |
| `enable_recursive_cte`    | BOOLEAN | false  | 是否启用递归 CTE 功能        |
| `recursive_cte_max_depth` | INT     | 5      | 递归的最大深度，防止无限递归 |


## 使用示例

### 示例 1：查询组织架构层次
这是递归 CTE 最常见的应用场景之一，用于查询员工的组织层级关系。

**准备数据：**

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

**查询组织层次结构：**

```sql
WITH RECURSIVE org_hierarchy AS (
    -- 锚点成员：从 CEO 开始（没有上级的员工）
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
    
    -- 递归成员：查找下一级员工
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

**结果：**

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

### 示例 2：多个递归 CTE

在一个查询中可以定义多个递归 CTE：

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
