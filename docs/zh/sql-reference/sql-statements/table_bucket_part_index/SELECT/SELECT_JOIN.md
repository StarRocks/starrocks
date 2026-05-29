---
displayed_sidebar: docs
sidebar_label: "JOIN"
---

# Join

Join 操作用于组合来自两个或多个表的数据，然后返回结果集中某些表的某些列。

StarRocks 支持以下 Join 类型：
- [Self Join](#self-join)
- [Cross Join](#cross-join)
- [Inner Join](#inner-join)
- [Outer Join](#outer-join) (包括 Left Join、Right Join 和 Full Join)
- [Semi Join](#semi-join)
- [Anti Join](#anti-join)
- [Equi-join 和 Non-equi-join](#equi-join-和-non-equi-join)
- [使用 USING 语句进行 JOIN](#使用-using-语句进行-join)
- [ASOF Join](#asof-join)

## 语法

```sql
SELECT select_list FROM
table_or_subquery1 [INNER] JOIN table_or_subquery2 |
table_or_subquery1 {LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} SEMI JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} ANTI JOIN table_or_subquery2 |
[ ON col1 = col2 [AND col3 = col4 ...] |
USING (col1 [, col2 ...]) ]
[other_join_clause ...]
[ WHERE where_clauses ]
```

```sql
SELECT select_list FROM
table_or_subquery1, table_or_subquery2 [, table_or_subquery3 ...]
[other_join_clause ...]
WHERE
col1 = col2 [AND col3 = col4 ...]
```

```sql
SELECT select_list FROM
table_or_subquery1 CROSS JOIN table_or_subquery2
[other_join_clause ...]
[ WHERE where_clauses ]
```

## Self Join

StarRocks 支持 Self Join。例如，同一张表的不同列进行连接。

实际上，没有特殊的语法来标识 Self Join。 Self Join 中，连接两侧的条件都来自同一张表。

我们需要为它们分配不同的别名。

示例：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

## Cross Join

Cross Join 可能会产生大量结果，因此应谨慎使用。

即使您需要使用 Cross Join，也需要使用过滤条件，并确保返回较少的结果。例如：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

## Inner Join

Inner Join 是最广为人知和常用的 Join。如果两个相似表的列包含相同的值，则返回两个相似表中请求的列的结果。

如果两个表的列名相同，我们需要使用全名（格式为 table_name.column_name）或为列名设置别名。

例如：

以下三个查询是等效的。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

## Outer Join

Outer Join 返回左表或右表的所有行，或者两表的所有行。如果另一张表中没有匹配的数据，则设置为 NULL。例如：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

## 等值 Join 和非等值 Join

通常，等值 Join 是最常用的 Join 方式。它要求 Join 条件的运算符为等号。

非等值 JOIN 使用 `!=` 作为 Join 条件。非等值 Join 会产生大量的计算结果，并可能在计算期间超出内存限制。

请谨慎使用。非等值 Join 仅支持 Inner Join。例如：

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

## Semi Join

Left Semi Join 仅返回左表中与右表中的数据匹配的行，而不管右表中有多少行与该数据匹配。

左表的这一行最多返回一次。Right Semi Join 的工作方式类似，只不过返回的数据是右表。

例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

## Anti Join

Left Anti Join（左反连接）仅返回左表中与右表不匹配的行。

Right Anti Join（右反连接）反转此比较，仅返回右表中与左表不匹配的行。例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

## Equi-join 和 Non-equi-join

根据 JOIN 中指定的 JOIN 条件，StarRocks 支持的各种 JOIN 可以分为 Equi-join 和 Non-equi-join。

| **Join 类型**   | **种类**                                                          |
| -------------- | ----------------------------------------------------------------- |
| Equi-join      | Self join、cross join、inner join、outer join、semi join、anti join |
| Non-equi-join  | cross join、inner join、left semi join、left anti join、outer join  |

- Equi-join

  Equi-join 使用的 JOIN 条件中，两个 JOIN 项通过 `=` 运算符组合。例如：`a JOIN b ON a.id = b.id`。

- Non-equi-join

  Non-equi-join 使用的 JOIN 条件中，两个 JOIN 项通过 `<`、`<=`、`>`、`>=` 或 `<>` 等比较运算符组合。例如：`a JOIN b ON a.id < b.id`。Non-equi-join 的运行速度比 Equi-join 慢。建议您在使用 Non-equi-join 时要谨慎。

  以下两个示例展示了如何运行 Non-equi-join：

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

## 使用 USING 语句进行 JOIN

从 v4.0.2 版本开始，除了 `ON` 之外，StarRocks 还支持通过 `USING` 语句指定 JOIN 条件。这有助于简化具有相同名称的列的等值 JOIN。例如：`SELECT * FROM t1 JOIN t2 USING (id)`。

**不同版本之间的差异：**

- **v4.0.2 之前的版本**
  
  `USING` 被视为语法糖，并在内部转换为 `ON` 条件。结果将包括来自左表和右表的 USING 列作为单独的列，并且在引用 USING 列时允许使用表别名限定符（例如，`t1.id`）。

  示例：

  ```SQL
  SELECT t1.id, t2.id FROM t1 JOIN t2 USING (id);  -- Returns two separate id columns
  ```

- **v4.0.2 及更高版本**
  
  StarRocks 实现了 SQL 标准的 `USING` 语义。主要功能包括：
  
  - 支持所有 JOIN 类型，包括 `FULL OUTER JOIN`。
  - USING 列在结果中显示为单个合并列。对于 FULL OUTER JOIN，使用 `COALESCE(left.col, right.col)` 语义。
  - USING 列不再支持表别名限定符（例如，`t1.id`）。您必须使用非限定列名（例如，`id`）。
  - 对于 `SELECT *` 的结果，列顺序为 `[USING 列, 左表非 USING 列, 右表非 USING 列]`。

  示例：

  ```SQL
  SELECT t1.id FROM t1 JOIN t2 USING (id);        -- ❌ Error: Column 'id' is ambiguous
  SELECT id FROM t1 JOIN t2 USING (id);           -- ✅ Correct: Returns a single coalesced 'id' column
  SELECT * FROM t1 FULL OUTER JOIN t2 USING (id); -- ✅ FULL OUTER JOIN is supported
  ```

这些更改使 StarRocks 的行为与符合 SQL 标准的数据库保持一致。

## ASOF Join

ASOF Join 是一种时间或范围相关的 JOIN 操作，通常用于时序分析。它允许基于某些键的相等性以及时间或序列字段上的非相等条件（例如 `t1.time >= t2.time`）来连接两个表。ASOF Join 从右侧表中为左侧表的每一行选择最近的匹配行。从 v4.0 版本开始支持。

在实际场景中，涉及时序数据分析通常会遇到以下挑战：
- 数据收集时间不一致（例如，不同的传感器采样时间）
- 事件发生和记录时间之间存在细微差异
- 需要为给定的时间戳查找最接近的历史记录

传统的等值 JOIN (INNER Join) 在处理此类数据时通常会导致大量数据丢失，而不等值 JOIN 可能会导致性能问题。ASOF Join 旨在解决这些特定挑战。

ASOF Join 通常用于以下情况：

- **金融市场分析**
  - 将股票价格与交易量进行匹配
  - 对齐来自不同市场的数据
  - 衍生品定价参考数据匹配
- **物联网数据处理**
  - 对齐多个传感器数据流
  - 关联设备状态变化
  - 时序数据插值
- **日志分析**
  - 将系统事件与用户操作相关联
  - 匹配来自不同服务的日志
  - 故障分析和问题跟踪

语法：

```SQL
SELECT [select_list]
FROM left_table [AS left_alias]
ASOF LEFT JOIN right_table [AS right_alias]
    ON equality_condition
    AND asof_condition
[WHERE ...]
[ORDER BY ...]
```

- `ASOF LEFT JOIN`: 基于时间或序列中最接近的匹配项执行非等式连接。`ASOF LEFT JOIN` 返回左表中的所有行，并将不匹配的右侧行填充为 NULL。
- `equality_condition`: 标准等式约束（例如，匹配的股票代码或 ID）。
- `asof_condition`: 范围条件，通常写为 `left.time >= right.time`，表示搜索不超过 `left.time` 的最近 `right.time` 记录。

:::note
`asof_condition` 仅支持 DATE 和 DATETIME 类型。并且仅支持一个 `asof_condition`。
:::

示例：

```SQL
SELECT *
FROM holdings h ASOF LEFT JOIN prices p             
ON h.ticker = p.ticker            
AND h.when >= p.when
ORDER BY ALL;
```

限制：

- 目前仅支持 Inner Join (默认) 和 Left Outer Join。
- `asof_condition` 中仅支持 DATE 和 DATETIME 类型。
- 仅支持一个 `asof_condition`。
