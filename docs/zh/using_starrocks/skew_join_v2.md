---
displayed_sidebar: docs
---

# Skew Join V2

Skew Join V2 是 StarRocks 中的一项高级优化功能，通过广播倾斜值来解决 JOIN 操作中的数据倾斜问题。当处理严重倾斜的数据分布时，该功能显著提高了查询性能。

## 概述

数据倾斜发生在某些连接列中的值比其他值出现得更频繁，导致节点间数据分布不均，进而造成性能瓶颈。Skew Join V2 通过以下方式解决此问题：

1. **识别倾斜值**：手动指定导致数据倾斜的值
2. **广播倾斜值**：将这些特定值广播到所有节点，以确保数据分布均匀
3. **混合执行**：使用 shuffle 和 broadcast joins 的组合以实现最佳性能

Skew Join V2 创建了一个混合执行计划，结合了：

1. **Shuffle Join**：对于非倾斜数据，使用标准的基于 shuffle 的 join
2. **Broadcast Join**：对于倾斜值，将右表数据广播到所有节点

## 启用 Skew Join V2

默认情况下，Skew Join V2 优化是禁用的，其前身 Skew Join V1 是启用的。如果发现 Skew Join V1 的性能不满意，必须在启用 Skew Join V2 之前禁用 Skew Join V1。

```SQL
-- 禁用 Skew Join V1
SET enable_optimize_skew_join_v1 = false;

-- 启用 Skew Join V2
SET enable_optimize_skew_join_v2 = true;
```

启用 Skew Join V2 后，可以根据[语法](#usage)更新查询，明确指定倾斜值。

:::note
目前，Skew Join V2 不支持基于统计信息的自动计划改写。仅支持基于提示的手动 SQL 改写。
:::

## 用法

与 Skew Join V1 的查询改写方法相比，Skew Join V2 提供了一种新语法，允许您使用 Broadcast 明确指定倾斜值。

语法：

```SQL
SELECT select_list FROM
table1 JOIN [skew|table1.column(skew_value1, skew_value2, ...)] table2
ON join_condition
[WHERE where_clause]
```

参数：

`[skew|table1.column(skew_value1, skew_value2, ...)]`：倾斜提示。包括：
- `table1.column`：左表中包含倾斜值的列。
- `skew_value1, skew_value2, ...`：导致数据倾斜的值的逗号分隔列表。

:::note
不要省略倾斜提示中的括号。
:::

## 示例

### 基本用法

1. 创建测试表。

    ```SQL
    CREATE TABLE orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        amount DECIMAL(10,2)
    ) DUPLICATE KEY(order_id)
    DISTRIBUTED BY HASH(order_id) BUCKETS 8;

    CREATE TABLE customers (
        customer_id INT,
        customer_name VARCHAR(100),
        city VARCHAR(50)
    ) DUPLICATE KEY(customer_id)
    DISTRIBUTED BY HASH(customer_id) BUCKETS 8;
    ```

2. 插入带有倾斜的数据样本。

    ```SQL
    INSERT INTO orders VALUES 
    (1, 1001, '2024-01-01', 100.00),
    (2, 1001, '2024-01-02', 200.00),
    (3, 1001, '2024-01-03', 150.00),
    (4, 1002, '2024-01-01', 300.00),
    (5, 1003, '2024-01-01', 250.00);

    INSERT INTO customers VALUES 
    (1001, 'John Doe', 'New York'),
    (1002, 'Jane Smith', 'Los Angeles'),
    (1003, 'Bob Johnson', 'Chicago');
    ```

3. 使用 Skew Join V2 查询数据。

    ```SQL
    SELECT o.order_id, c.customer_name, o.amount
    FROM orders o 
    JOIN [skew|o.customer_id(1001)] c 
    ON o.customer_id = c.customer_id;
    ```

### 多个倾斜值

Skew Join V2 支持多个倾斜值。

```SQL
SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001, 1002, 1003)] c 
ON o.customer_id = c.customer_id;
```

### 不同的数据类型

Skew Join V2 支持各种数据类型的倾斜值。

```SQL
-- 字符串值
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics', 'clothing', 'books')] t2 
ON t1.category = t2.category;

-- 日期值
SELECT t1.id, t2.event_name
FROM events t1 
JOIN [skew|t1.event_date('2024-01-01', '2024-01-02')] t2 
ON t1.event_date = t2.event_date;

-- 数值
SELECT t1.id, t2.region
FROM sales t1 
JOIN [skew|t1.region_id(1, 2, 3)] t2 
ON t1.region_id = t2.region_id;
```

### 复杂的 Join 条件

Skew Join V2 支持复杂的 Join 条件。

```SQL
-- 具有复杂表达式的 Join 条件
SELECT t1.id, t2.value
FROM table1 t1 
JOIN [skew|t1.key(abs(t1.key))] t2 
ON abs(t1.key) = abs(t2.key);

-- 多个 Join 条件
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category AND t1.region = t2.region;
```

### 不同的 Join 类型

Skew Join V2 支持多种 Join 类型。

```SQL
-- 左连接
SELECT t1.id, t2.name
FROM table1 t1 
LEFT JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- 左半连接
SELECT t1.id
FROM table1 t1 
LEFT SEMI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- 左反连接
SELECT t1.id
FROM table1 t1 
LEFT ANTI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;
```

## 最佳实践

### 步骤 1. 识别倾斜值

在使用 Skew Join V2 之前，识别导致数据倾斜的值：

```SQL
-- 分析数据分布
SELECT customer_id, COUNT(*) as cnt
FROM orders 
GROUP BY customer_id 
ORDER BY cnt DESC 
LIMIT 10;
```

### 步骤 2. 选择合适的倾斜值

遵循以下规则选择合适的倾斜值：

- 仅包括导致显著倾斜的最频繁值。
- 避免包含过多值，因为这可能增加广播开销。
- 考虑倾斜优化与广播成本之间的权衡。

## 限制

- **支持的 Join 类型**
  目前，仅支持 INNER JOIN、LEFT JOIN、LEFT SEMI JOIN 和 LEFT ANTI JOIN。
- **数据类型**
  目前，仅支持基本数据类型（INT、BIGINT、STRING、DATE、DATETIME 和字符串类型）。
- **复杂表达式**
  对 Join 条件中复杂表达式的支持有限。
- **倾斜表**
  Skew Join V2 只能处理 JOIN 操作中大表倾斜的场景，并且大表必须用作左表。
- **Join 重排序**
  使用 `skew` Hint 会阻止优化器对该 Join 进行重排序（Join Reorder）。该 Join 将按照 SQL 中指定的顺序执行，优化器不会尝试更改连接顺序，也不会交换包含该 Hint 的 Join 节点的左右表。

## 故障排除

### 调试信息

对查询执行 EXPLAIN VERBOSE 以收集其执行计划：

```SQL
EXPLAIN VERBOSE 
SELECT ... FROM ... JOIN [skew|...] ...;
```

检查计划中的以下字段：

- `SplitCastDataSink`：数据拆分。
- `BROADCAST` 和 `SHUFFLE`：分布类型。
- `UNION`：两种 Join 类型的结果合并。

## 相关主题

- [Query Planning](../best_practices/query_tuning/query_planning.md)
- [Query Profile Tuning](../best_practices/query_tuning/query_profile_tuning_recipes.md)
- [System Variables](../sql-reference/System_variable.md)
- [JOIN Operations](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md#join)
