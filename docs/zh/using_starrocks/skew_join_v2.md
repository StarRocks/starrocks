---
displayed_sidebar: docs
---

# Skew Join V2

Skew Join V2 是 StarRocks 中的高级优化功能，通过广播倾斜值来解决 JOIN 操作中的数据倾斜问题。当处理严重倾斜的数据分布时，此功能可以显著提高查询性能。

## 概述

数据倾斜是指连接列中的某些值比其他值出现频率高得多，导致数据在节点间分布不均，从而造成性能瓶颈。Skew Join V2 通过以下方式解决这个问题：

1. **识别倾斜值**：手动指定导致数据倾斜的值
2. **广播倾斜值**：将这些特定值广播到所有节点，确保数据均匀分布
3. **混合执行**：结合使用 shuffle 和 broadcast join 以获得最佳性能

## 语法

```sql
SELECT select_list FROM
table1 JOIN [skew|table1.column(skew_value1, skew_value2, ...)] table2
ON join_condition
[WHERE where_clause]
```

### 语法组件

- `[skew|table1.column(skew_value1, skew_value2, ...)]`：倾斜提示，指定：
  - `table1.column`：包含倾斜值的左表列
  - `skew_value1, skew_value2, ...`：导致数据倾斜的值的逗号分隔列表

## 配置

### 会话变量

使用以下会话变量配置 Skew Join V2：

```sql
-- 启用 Skew Join V2 优化
SET enable_optimize_skew_join_v2 = true;
```

### 配置参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `enable_optimize_skew_join_v2` | 启用 Skew Join V2 优化 | `false` |
| `enable_optimize_skew_join_v1` | 启用旧的查询重写方法 | `true` |

## 示例

### 基本用法

```sql
-- 创建测试表
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

-- 插入带有倾斜的示例数据
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

-- 使用 Skew Join V2 的查询
SET enable_optimize_skew_join_v2 = true;

SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001)] c 
ON o.customer_id = c.customer_id;
```

### 多个倾斜值

```sql
-- 处理多个倾斜值
SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001, 1002, 1003)] c 
ON o.customer_id = c.customer_id;
```

### 不同数据类型

Skew Join V2 支持各种数据类型的倾斜值：

```sql
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

### 复杂连接条件

```sql
-- 连接条件中使用复杂表达式
SELECT t1.id, t2.value
FROM table1 t1 
JOIN [skew|t1.key(abs(t1.key))] t2 
ON abs(t1.key) = abs(t2.key);

-- 多个连接条件
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category AND t1.region = t2.region;
```

### 不同连接类型

```sql
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

## 工作原理

### 执行计划

Skew Join V2 创建一个混合执行计划，结合了：

1. **Shuffle Join**：对于非倾斜数据，使用标准的基于 shuffle 的连接
2. **Broadcast Join**：对于倾斜值，将右表数据广播到所有节点

## 最佳实践

### 1. 识别倾斜值

在使用 Skew Join V2 之前，识别导致数据倾斜的值：

```sql
-- 分析数据分布
SELECT customer_id, COUNT(*) as cnt
FROM orders 
GROUP BY customer_id 
ORDER BY cnt DESC 
LIMIT 10;
```

### 2. 选择合适的倾斜值

- 只包含导致显著倾斜的最频繁值
- 避免包含过多值，因为这可能增加广播开销
- 考虑倾斜优化和广播成本之间的权衡


## 限制

1. **支持的连接类型**：目前支持 INNER JOIN、LEFT JOIN、LEFT SEMI JOIN 和 LEFT ANTI JOIN
2. **数据类型**：支持所有基本数据类型（INT、BIGINT、STRING、DATE、DATETIME 等）
3. **复杂表达式**：对连接条件中的复杂表达式支持有限
4. **倾斜表**：只能处理Join左右表中大表倾斜的场景，此时需要将大表作为Join的左表

## 故障排除
### 调试信息

启用详细解释以查看执行计划：

```sql
EXPLAIN VERBOSE 
SELECT ... FROM ... JOIN [skew|...] ...;
```

查找：
- `SplitCastDataSink` 节点，指示数据分割
- `BROADCAST` 和 `SHUFFLE` 分布类型
- `UNION` 节点，合并两种连接类型的结果

## 从 Skew Join V1 迁移

如果您当前使用旧的Skew Join优化，且性能不佳，可以考虑默认打开enable_optimize_skew_join_v2：

```sql
-- 禁用旧方法
SET enable_optimize_skew_join_v1 = false;

-- 启用 V2
SET enable_optimize_skew_join_v2 = true;

-- 更新查询以使用新语法
-- 旧：查询重写方法
-- 新：显式指定倾斜值并使用广播
```

### 限制
当前Skew Join V2尚不支持基于统计信息自动改写Plan, 只能使用hint的方式手动改写SQL

## 相关主题

- [查询规划](../query_planning.md)
- [查询配置文件调优](../query_profile_tuning_recipes.md)
- [系统变量](../../sql-reference/System_variable.md)
- [JOIN 操作](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md#join)