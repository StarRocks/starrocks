---
displayed_sidebar: docs
---

# SELECT

SELECT 语句用于从一个或多个表、视图或物化视图中查询数据。SELECT 语句通常由以下子句组成：

- [WITH](#with)
- [Joins](#join)
- [ORDER BY](#order-by)
- [GROUP BY](#group-by)
- [HAVING](#having)
- [LIMIT](#limit)
- [OFFSET](#offset)
- [UNION](#union)
- [INTERSECT](#intersect)
- [EXCEPT/MINUS](#exceptminus)
- [DISTINCT](#distinct)
- [子查询](#子查询)
- [WHERE and operators](#where-和-operator)
- [别名](#别名)
- [PIVOT](#pivot)
- [EXCLUDE](#exclude)
- [RECURSIVE](#recursive)

SELECT 可以作为一个独立的语句执行，也可以作为嵌套在其他语句中的子句使用。SELECT 子句的输出可以作为其他语句的输入。

StarRocks 的查询语句基本上符合 SQL92 标准。以下是支持的 SELECT 用法的简要说明。

> **NOTE**
>
> 要从 StarRocks 内表中的表、视图或物化视图查询数据，您必须拥有对这些对象的 SELECT 权限。要从外部数据源中的表、视图或物化视图查询数据，您必须拥有对相应 external catalog 的 USAGE 权限。

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

## Join

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

语法：

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

### Self Join

StarRocks 支持 Self Join。例如，同一张表的不同列进行连接。

实际上，没有特殊的语法来标识 Self Join。 Self Join 中，连接两侧的条件都来自同一张表。

我们需要为它们分配不同的别名。

示例：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

### Cross Join

Cross Join 可能会产生大量结果，因此应谨慎使用。

即使您需要使用 Cross Join，也需要使用过滤条件，并确保返回较少的结果。例如：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

### Inner Join

Inner Join 是最广为人知和常用的 Join。如果两个相似表的列包含相同的值，则返回两个相似表中请求的列的结果。

如果两个表的列名相同，我们需要使用全名（格式为 table_name.column_name）或为列名设置别名。

例如：

以下三个查询是等效的。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

### Outer Join

Outer Join 返回左表或右表的所有行，或者两表的所有行。如果另一张表中没有匹配的数据，则设置为 NULL。例如：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

### 等值 Join 和非等值 Join

通常，等值 Join 是最常用的 Join 方式。它要求 Join 条件的运算符为等号。

非等值 JOIN 使用 `!=` 作为 Join 条件。非等值 Join 会产生大量的计算结果，并可能在计算期间超出内存限制。

请谨慎使用。非等值 Join 仅支持 Inner Join。例如：

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

### Semi Join

Left Semi Join 仅返回左表中与右表中的数据匹配的行，而不管右表中有多少行与该数据匹配。

左表的这一行最多返回一次。Right Semi Join 的工作方式类似，只不过返回的数据是右表。

例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

### Anti Join

Left Anti Join（左反连接）仅返回左表中与右表不匹配的行。

Right Anti Join（右反连接）反转此比较，仅返回右表中与左表不匹配的行。例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

### Equi-join 和 Non-equi-join

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

### 使用 USING 语句进行 JOIN

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

### ASOF Join

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

局限性：

- 目前仅支持 Inner Join (默认) 和 Left Outer Join。
- `asof_condition` 中仅支持 DATE 和 DATETIME 类型。
- 仅支持一个 `asof_condition`。

## ORDER BY

SELECT 语句的 ORDER BY 子句通过比较一列或多列的值对结果集进行排序。

ORDER BY 是一项非常消耗时间和资源的操作，因为所有结果都必须发送到一个节点进行合并，然后才能对结果进行排序。与没有 ORDER BY 的查询相比，排序会消耗更多的内存资源。

因此，如果您只需要排序结果集中的前 `N` 个结果，则可以使用 LIMIT 子句，这样可以减少内存使用和网络开销。如果未指定 LIMIT 子句，则默认返回前 65535 个结果。

**语法**

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

**参数**

- `ASC` 指定结果应按升序返回。
- `DESC` 指定结果应按降序返回。如果未指定顺序，则默认为 ASC（升序）。
- `NULLS FIRST` 表示 NULL 值应在非 NULL 值之前返回。
- `NULLS LAST` 表示 NULL 值应在非 NULL 值之后返回。

**示例**

```sql
select * from big_table order by tiny_column, short_column desc;
select  *  from  sales_record  order by  employee_id  nulls first;
```

## GROUP BY

GROUP BY 语句通常与聚合函数一起使用。GROUP BY 语句中指定的列不参与聚合运算。

**语法**

```sql
SELECT
...
aggregate_function() [ FILTER ( where boolean_expression ) ]
...
FROM ...
[ ... ]
GROUP BY [
    , ... |
    GROUPING SETS [, ...] (  groupSet [ , groupSet [ , ... ] ] ) |
    ROLLUP(expr  [ , expr [ , ... ] ]) |
    CUBE(expr  [ , expr [ , ... ] ])
    ]
[ ... ]
```

**参数**

- `FILTER` 可以与聚合函数一起使用。只有经过筛选的行才会参与聚合函数的计算。

  > **注意**
  >
  > - `FILTER` 子句仅支持 AVG、COUNT、MAX、MIN、SUM、ARRAY_AGG 和 ARRAY_AGG_DISTINCT 函数。
  > - `FILTER` 子句不支持 COUNT DISTINCT。
  > - 指定 `FILTER` 子句后，ARRAY_AGG 和 ARRAY_AGG_DISTINCT 函数中不允许使用 ORDER BY 子句。

- `GROUPING SETS`、`CUBE` 和 `ROLLUP` 是 GROUP BY 子句的扩展。在 GROUP BY 子句中，它们可用于实现多组分组聚合。结果等同于多个 GROUP BY 子句的 UNION 结果。

**示例**

示例 1：`FILTER`

  以下两个查询是等效的。

  ```sql
  SELECT
    COUNT(*) AS total_users,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_users,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_users
  FROM users;
  ```

  ```sql
  SELECT
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE gender = 'M') AS male_users,
    COUNT(*) FILTER (WHERE gender = 'F') AS female_users
  FROM users;
  ```

示例 2: `GROUPING SETS`、`CUBE` 和 `ROLLUP`

  `ROLLUP(a,b,c)` 等价于以下 `GROUPING SETS` 语句。

    ```sql
    GROUPING SETS (
    (a,b,c),
    (a,b  ),
    (a    ),
    (     )
    )
    ```

`CUBE (a, b, c)` 等价于以下 `GROUPING SETS` 语句。

    ```sql
    GROUPING SETS (
    ( a, b, c ),
    ( a, b    ),
    ( a,    c ),
    ( a       ),
    (    b, c ),
    (    b    ),
    (       c ),
    (         )
    )
    ```

在一个真实的数据集中进行测试。

    ```sql
    SELECT * FROM t;
    +------+------+------+
    | k1   | k2   | k3   |
    +------+------+------+
    | a    | A    |    1 |
    | a    | A    |    2 |
    | a    | B    |    1 |
    | a    | B    |    3 |
    | b    | A    |    1 |
    | b    | A    |    4 |
    | b    | B    |    1 |
    | b    | B    |    5 |
    +------+------+------+
    8 rows in set (0.01 sec)
  
    SELECT k1, k2, SUM(k3) FROM t
    GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
    +------+------+-----------+
    | k1   | k2   | sum(`k3`) |
    +------+------+-----------+
    | b    | B    |         6 |
    | a    | B    |         4 |
    | a    | A    |         3 |
    | b    | A    |         5 |
    | NULL | B    |        10 |
    | NULL | A    |         8 |
    | a    | NULL |         7 |
    | b    | NULL |        11 |
    | NULL | NULL |        18 |
    +------+------+-----------+
    9 rows in set (0.06 sec)
  
    SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t
    GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
    +------+------+---------------+----------------+
    | k1   | k2   | grouping_id(k1,k2) | sum(`k3`) |
    +------+------+---------------+----------------+
    | a    | A    |             0 |              3 |
    | a    | B    |             0 |              4 |
    | a    | NULL |             1 |              7 |
    | b    | A    |             0 |              5 |
    | b    | B    |             0 |              6 |
    | b    | NULL |             1 |             11 |
    | NULL | A    |             2 |              8 |
    | NULL | B    |             2 |             10 |
    | NULL | NULL |             3 |             18 |
    +------+------+---------------+----------------+
    9 rows in set (0.02 sec)
    ```

## HAVING

HAVING 子句不用于过滤表中的行数据，而是用于过滤聚合函数的结果。

通常来说，HAVING 与聚合函数（例如 COUNT()、SUM()、AVG()、MIN()、MAX()）和 GROUP BY 子句一起使用。

**Examples**

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having sum(short_column) = 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|         2   |        1            |
+-------------+---------------------+

1 row in set (0.07 sec)
```

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having tiny_column > 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|      2      |          1          |
+-------------+---------------------+

1 row in set (0.07 sec)
```

## LIMIT

LIMIT 语句用于限制返回的最大行数。设置返回的最大行数可以帮助 StarRocks 优化内存使用。

此语句主要用于以下场景：

返回 top-N 查询的结果。

考虑一下下表包含的内容。

由于表中的数据量很大，或者因为 WHERE 语句没有过滤掉太多的数据，所以需要限制查询结果集的大小。

使用说明：LIMIT 语句的值必须是数字字面常量。

**示例**

```plain text
mysql> select tiny_column from small_table limit 1;

+-------------+
|tiny_column  |
+-------------+
|     1       |
+-------------+

1 row in set (0.02 sec)
```

```plain text
mysql> select tiny_column from small_table limit 10000;

+-------------+
|tiny_column  |
+-------------+
|      1      |
|      2      |
+-------------+

2 rows in set (0.01 sec)
```

## OFFSET

`OFFSET` 子句使结果集跳过前几行，然后直接返回后面的结果。

结果集默认从第 0 行开始，因此 `OFFSET 0` 和没有 `OFFSET` 返回相同的结果。

一般来说，`OFFSET` 子句需要与 `ORDER BY` 和 `LIMIT` 子句一起使用才有效。

示例：

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 3;

+----------------+
| varchar_column | 
+----------------+
|    beijing     | 
|    chongqing   | 
|    tianjin     | 
+----------------+

3 rows in set (0.02 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

注意：允许在没有 order by 的情况下使用 offset 语法，但此时 offset 没有意义。

在这种情况下，仅采用 limit 值，而忽略 offset 值。因此，没有 order by。

Offset 超过结果集中的最大行数，但仍然会返回结果。建议用户将 offset 与 order by 一起使用。

## UNION

将多个查询的结果组合在一起。

**语法**

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

**参数**

- `DISTINCT` (默认): 仅返回唯一行。UNION 等同于 UNION DISTINCT。
- `ALL`: 合并所有行，包括重复行。由于去重操作会消耗大量内存，因此使用 UNION ALL 的查询速度更快，内存消耗更少。为了获得更好的性能，请使用 UNION ALL。

> **注意**
>
> 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

**示例**

创建表 `select1` 和 `select2`。

```SQL
CREATE TABLE select1(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select1 VALUES
    (1,2),
    (1,2),
    (2,3),
    (5,6),
    (5,6);

CREATE TABLE select2(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select2 VALUES
    (2,3),
    (3,4),
    (5,6),
    (7,8);
```

示例 1：返回两个表中的所有 ID，包括重复项。

```Plaintext
mysql> (select id from select1) union all (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    1 |
|    2 |
|    2 |
|    3 |
|    5 |
|    5 |
|    5 |
|    7 |
+------+
11 rows in set (0.02 sec)
```

示例 2：返回两个表中所有不重复的 ID。以下两个语句是等效的。

```Plaintext
mysql> (select id from select1) union (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
6 rows in set (0.01 sec)

mysql> (select id from select1) union distinct (select id from select2) order by id;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
5 rows in set (0.02 sec)
```

示例 3：返回两个表中所有唯一 ID 中的前三个 ID。以下两个语句是等效的。

```SQL
mysql> (select id from select1) union distinct (select id from select2)
order by id
limit 3;
++------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
4 rows in set (0.11 sec)

mysql> select * from (select id from select1 union distinct select id from select2) as t1
order by id
limit 3;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.01 sec)
```

## INTERSECT

计算多个查询结果的交集，即出现在所有结果集中的结果。该子句仅返回结果集中唯一的行。不支持 ALL 关键字。

**语法**

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **注意**
>
> - INTERSECT 等同于 INTERSECT DISTINCT。
> - 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

**示例**

使用 UNION 中的两个表。

返回两个表中通用的不同 `(id, price)` 组合。以下两个语句是等效的。

```Plaintext
mysql> (select id, price from select1) intersect (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+

mysql> (select id, price from select1) intersect distinct (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+
```

## EXCEPT/MINUS

返回左侧查询中存在但右侧查询中不存在的不同结果。EXCEPT 等同于 MINUS。

**语法**

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **说明**
>
> - `EXCEPT` 等同于 `EXCEPT DISTINCT`。不支持 `ALL` 关键字。
> - 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

**示例**

以下示例使用 `UNION` 中的两个表。

返回 `select1` 中找不到的 `(id, price)` 的不同组合。

```Plaintext
mysql> (select id, price from select1) except (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+

mysql> (select id, price from select1) minus (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+
```

## DISTINCT

DISTINCT 关键字可以对结果集进行去重。例如：

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

`DISTINCT` 可以与聚合函数（通常是计数函数）一起使用，`count (distinct)` 用于计算一列或多列中包含多少不同的组合。

```SQL
-- Counts the unique values from one column.
select count(distinct tiny_column) from small_table;
```

```plain text
+-------------------------------+
| count(DISTINCT 'tiny_column') |
+-------------------------------+
|             2                 |
+-------------------------------+
1 row in set (0.06 sec)
```

```SQL
-- Counts the unique combinations of values from multiple columns.
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks 支持同时使用多个 `distinct` 聚合函数。

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

## 子查询

根据相关性，子查询分为以下两种类型：

- 非相关子查询：独立于外部查询获得结果。
- 相关子查询：需要来自外部查询的值。

#### 非相关子查询

非相关子查询支持 [NOT] IN 和 EXISTS。

**示例**

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

从 v3.0 版本开始，您可以在 `SELECT... FROM... WHERE... [NOT] IN` 的 WHERE 子句中指定多个字段，例如，第二个 SELECT 语句中的 `WHERE (x,y)`。

#### 相关子查询

相关子查询支持 [NOT] IN 和 [NOT] EXISTS。

**示例**

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

子查询也支持标量量化查询。它可以分为不相关标量量化查询、相关标量量化查询和作为通用函数参数的标量量化查询。

**示例**

1. 具有 = 符号的非相关标量量化查询。例如，输出工资最高的人的信息。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. 具有谓词 `>`, `<` 等的不相关标量量化查询。例如，输出关于工资高于平均水平的人员的信息。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 相关的标量量子查询。例如，输出每个部门的最高工资信息。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. 标量量子查询用作普通函数的参数。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

## Where 和 Operator

SQL operator 是一系列用于比较的函数，广泛用于 select 语句的 where 子句中。

### 算术运算符

算术运算符通常出现在包含左操作数、右操作数以及最常见的左操作数的表达式中。

**+ 和 -**：可以用作一元运算符或二元运算符。当用作一元运算符时，例如 +1、-2.5 或 -col_name，表示该值乘以 +1 或 -1。

因此，一元运算符 + 返回不变的值，而一元运算符 - 更改该值的符号位。

用户可以叠加两个一元运算符，例如 +5（返回正值）、-+2 或 +-2（返回负值），但用户不能使用两个连续的 - 符号。

因为 -- 在以下语句中被解释为注释（当用户可以使用两个符号时，两个符号之间需要一个空格或括号，例如 -(-2) 或 - -2，实际上会产生 +2）。

当 + 或 - 是二元运算符时，例如 2+2、3+1.5 或 col1+col2，表示左值加上或减去右值。左值和右值都必须是数值类型。

**\* 和 /**：分别表示乘法和除法。两侧的操作数必须是数据类型。当两个数字相乘时。

如果需要，可以提升较小的操作数（例如，将 SMALLINT 提升为 INT 或 BIGINT），并且表达式的结果将提升为下一个更大的类型。

例如，TINYINT 乘以 INT 将产生 BIGINT 类型的结果。当两个数字相乘时，操作数和表达式结果都被解释为 DOUBLE 类型，以避免精度损失。

如果用户想要将表达式的结果转换为另一种类型，则需要使用 CAST 函数进行转换。

**%**：求模运算符。返回左操作数除以右操作数的余数。左操作数和右操作数都必须是整数。

**&、| 和 ^**：按位运算符返回对两个操作数执行按位与、按位或、按位异或运算的结果。两个操作数都需要整数类型。

如果按位运算符的两个操作数的类型不一致，则将较小类型的操作数提升为较大类型的操作数，并执行相应的按位运算。

一个表达式中可以出现多个算术运算符，用户可以将相应的算术表达式括在括号中。算术运算符通常没有相应的数学函数来表达与算术运算符相同的功能。

例如，我们没有 MOD() 函数来表示 % 运算符。相反，数学函数没有相应的算术运算符。例如，幂函数 POW() 没有对应的 ** 指数运算符。

用户可以通过数学函数部分了解我们支持哪些算术函数。

### Between Operator

在 WHERE 子句中，表达式可以与上限和下限进行比较。如果表达式大于或等于下限，且小于或等于上限，则比较结果为 true。

语法：

```sql
expression BETWEEN lower_bound AND upper_bound
```

数据类型：通常表达式会评估为数值类型，但也支持其他数据类型。如果必须确保下限和上限都是可比较的字符，则可以使用 cast() 函数。

使用说明：如果操作数是字符串类型，请注意，以上限开头的长字符串将与大于上限的上限不匹配。例如，"between'A'and'M' 将不匹配 'MJ'"。

如果需要确保表达式正常工作，可以使用 upper()、lower()、substr()、trim() 等函数。

示例：

```sql
select c1 from t1 where month between 1 and 6;
```

### 比较运算符

比较运算符用于比较两个值。`=`、`!=`、`>=` 适用于所有数据类型。

`<>` 和 `!=` 运算符是等效的，表示两个值不相等。

### In Operator

In 操作符用于与 VALUE 集合进行比较，如果 VALUE 集合中存在任何一个元素与参数匹配，则返回 TRUE。

参数和 VALUE 集合必须是可比较的。所有使用 IN 操作符的表达式都可以写成用 OR 连接的等效比较，但 IN 的语法更简单、更精确，并且更容易让 StarRocks 进行优化。

示例：

```sql
select * from small_table where tiny_column in (1,2);
```

### Like Operator

该 operator 用于字符串的比较。`_`（下划线）匹配单个字符，`%` 匹配多个字符。参数必须与完整字符串匹配。通常，将 `%` 放在字符串的末尾会更实用。

示例：

```plain text
mysql> select varchar_column from small_table where varchar_column like 'm%';

+----------------+
|varchar_column  |
+----------------+
|     milan      |
+----------------+

1 row in set (0.02 sec)
```

```plain
mysql> select varchar_column from small_table where varchar_column like 'm____';

+----------------+
| varchar_column | 
+----------------+
|    milan       | 
+----------------+

1 row in set (0.01 sec)
```

### 逻辑运算符

逻辑运算符返回 BOOL 值，包括单元和多元运算符，每个运算符处理的参数都是返回 BOOL 值的表达式。支持的运算符包括：

AND：二元运算符，如果左右参数的计算结果都为 TRUE，则 AND 运算符返回 TRUE。

OR：二元运算符，如果左右参数之一的计算结果为 TRUE，则返回 TRUE。如果两个参数都为 FALSE，则 OR 运算符返回 FALSE。

NOT：单元运算符，反转表达式的结果。如果参数为 TRUE，则运算符返回 FALSE；如果参数为 FALSE，则运算符返回 TRUE。

示例：

```plain text
mysql> select true and true;

+-------------------+
| (TRUE) AND (TRUE) | 
+-------------------+
|         1         | 
+-------------------+

1 row in set (0.00 sec)
```

```plain text
mysql> select true and false;

+--------------------+
| (TRUE) AND (FALSE) | 
+--------------------+
|         0          | 
+--------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select true or false;

+-------------------+
| (TRUE) OR (FALSE) | 
+-------------------+
|        1          | 
+-------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select not true;

+----------+
| NOT TRUE | 
+----------+
|     0    | 
+----------+

1 row in set (0.01 sec)
```

### 正则表达式运算符

确定是否匹配正则表达式。 使用 POSIX 标准正则表达式，“^”匹配字符串的第一部分，“$”匹配字符串的结尾。

“.” 匹配任何单个字符，“*” 匹配零个或多个选项，“+” 匹配一个或多个选项，“？” 表示贪婪表示等等。 正则表达式需要匹配完整的值，而不仅仅是字符串的一部分。

如果要匹配中间部分，则正则表达式的前面部分可以写成“^.” 或“.”。“^”和“$”通常被省略。 RLIKE 运算符和 REGEXP 运算符是同义词。

“|”运算符是一个可选运算符。“|”两侧的正则表达式只需要满足一个侧面条件。“|”运算符和两侧的正则表达式通常需要用 () 括起来。

例子：

```plain text
mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |       
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from small_table where varchar_column regexp 'm.*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |  
+----------------+

1 row in set (0.01 sec)
```

## 别名

在查询中编写表名、列名或包含列的表达式时，您可以为它们分配别名。别名通常比原始名称更短，更容易记住。

当需要别名时，您只需在 SELECT 列表或 FROM 列表中的表名、列名和表达式名称后添加 AS 子句。AS 关键字是可选的。您也可以直接在原始名称后指定别名，而无需使用 AS。

如果别名或其他标识符与内部 [StarRocks 关键字](../keywords.md) 同名，则需要将名称用一对反引号括起来，例如 `rank`。

别名区分大小写，但列别名和表达式别名不区分大小写。

示例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

## PIVOT

该功能从 v3.3 版本开始支持。

PIVOT 操作是 SQL 中的一项高级功能，允许您将表中的行转换为列，这对于创建数据透视表特别有用。当处理数据库报告或分析时，尤其是在需要汇总或分类数据以进行演示时，此功能非常方便。

实际上，PIVOT 是一种语法糖，可以简化 `sum(case when ... then ... end)` 之类的查询语句的编写。

**语法**

```sql
pivot:
SELECT ...
FROM ...
PIVOT (
  aggregate_function(<expr>) [[AS] alias] [, aggregate_function(<expr>) [[AS] alias] ...]
  FOR <pivot_column>
  IN (<pivot_value>)
)

pivot_column:
<column_name> 
| (<column_name> [, <column_name> ...])

pivot_value:
<literal> [, <literal> ...]
| (<literal>, <literal> ...) [, (<literal>, <literal> ...)]
```

**参数**

在 PIVOT 操作中，您需要指定几个关键组件：

- aggregate_function()：一个聚合函数，例如 SUM、AVG、COUNT 等，用于汇总数据。
- alias：聚合结果的别名，使结果更易于理解。
- FOR pivot_column：指定将执行行到列转换的列名。
- IN (pivot_value)：指定 pivot_column 的特定值，这些值将被转换为列。

**示例**

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- The result is equivalent to the following query:
SELECT SUM(CASE WHEN c3 = 1 THEN c1 ELSE NULL END) AS sum_c1_1,
       AVG(CASE WHEN c3 = 1 THEN c2 ELSE NULL END) AS avg_c2_1,
       SUM(CASE WHEN c3 = 2 THEN c1 ELSE NULL END) AS sum_c1_2,
       AVG(CASE WHEN c3 = 2 THEN c2 ELSE NULL END) AS avg_c2_2,
       SUM(CASE WHEN c3 = 3 THEN c1 ELSE NULL END) AS sum_c1_3,
       AVG(CASE WHEN c3 = 3 THEN c2 ELSE NULL END) AS avg_c2_3,
       SUM(CASE WHEN c3 = 4 THEN c1 ELSE NULL END) AS sum_c1_4,
       AVG(CASE WHEN c3 = 4 THEN c2 ELSE NULL END) AS avg_c2_4,
       SUM(CASE WHEN c3 = 5 THEN c1 ELSE NULL END) AS sum_c1_5,
       AVG(CASE WHEN c3 = 5 THEN c2 ELSE NULL END) AS avg_c2_5
FROM t1
GROUP BY c0;
```

## EXCLUDE

该功能从 4.0 版本开始支持。

`EXCLUDE` 关键字用于从查询结果中排除指定的列，从而简化 SQL 语句，尤其适用于处理包含大量列的表，避免了显式列出所有要保留的列。

**语法**

```sql  
SELECT  
  * EXCLUDE (<column_name> [, <column_name> ...])  
  | <table_alias>.* EXCLUDE (<column_name> [, <column_name> ...])  
FROM ...  
```

**参数**

- **`* EXCLUDE`**  
  选择所有列，使用通配符 `*`，后跟 `EXCLUDE` 和要排除的列名列表。
- **`<table_alias>.* EXCLUDE`**  
  当存在表别名时，允许从该表中排除特定列（必须与别名一起使用）。
- **`<column_name>`**  
  要排除的列名。多个列名用逗号分隔。列必须存在于表中；否则，将返回错误。

**示例**

- 基本用法：

```sql  
-- Create test_table.
CREATE TABLE test_table (  
  id INT,  
  name VARCHAR(50),  
  age INT,  
  email VARCHAR(100)  
) DUPLICATE KEY(id);  

-- Exclude a single column (age).
SELECT * EXCLUDE (age) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, email FROM test_table;  

-- Exclude multiple columns (name, email).
SELECT * EXCLUDE (name, email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, age FROM test_table;  

-- Exclude columns using a table alias.
SELECT test_table.* EXCLUDE (email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, age FROM test_table;  
```

## RECURSIVE

从 v4.1 版本开始，StarRocks 支持递归公共表表达式 (CTE)，它使用迭代执行方法来高效地处理各种层级和树状结构数据。

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

**Syntax**

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

**参数**

- `cte_name`: CTE 的名称。
- `column_list` (可选): CTE 结果集的列名列表。
- `anchor_query`: 初始查询，必须是非递归的，并且不能引用 CTE 本身。
- `UNION`: Union 操作符。
  - `UNION ALL`: 保留所有行（包括重复行），建议使用以获得更好的查询性能。
  - `UNION` 或 `UNION DISTINCT`: 删除重复行。
- `recursive_query`: 引用 CTE 本身的递归查询。

**限制**

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

**配置**

使用递归 CTE 需要以下系统变量：

| 变量名                      | 类型    | 默认值  | 描述                                               |
| --------------------------- | ------- | ------- | -------------------------------------------------- |
| `enable_recursive_cte`      | BOOLEAN | false   | 是否开启递归 CTE。                                 |
| `recursive_cte_max_depth`   | INT     | 5       | 最大递归深度，以防止无限递归。                       |

**示例**

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
