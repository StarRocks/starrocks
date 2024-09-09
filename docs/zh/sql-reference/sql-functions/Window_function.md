---
displayed_sidebar: docs
keywords: ['analytic']
sidebar_position: 0.9
---

# 使用窗口函数组织过滤数据

- [使用窗口函数组织过滤数据](#使用窗口函数组织过滤数据)
  - [窗口函数语法及参数](#窗口函数语法及参数)
    - [语法](#语法)
    - [参数说明](#参数说明)
  - [窗口函数建表示例](#窗口函数建表示例)
  - [使用 AVG() 窗口函数](#使用-avg-窗口函数)
  - [使用 COUNT() 窗口函数](#使用-count-窗口函数)
  - [使用 CUME\_DIST() 窗口函数](#使用-cume_dist-窗口函数)
  - [使用 DENSE\_RANK() 窗口函数](#使用-dense_rank-窗口函数)
  - [使用 FIRST\_VALUE() 窗口函数](#使用-first_value-窗口函数)
  - [使用 LAST\_VALUE() 窗口函数](#使用-last_value-窗口函数)
  - [使用 LAG() 窗口函数](#使用-lag-窗口函数)
  - [使用 LEAD() 窗口函数](#使用-lead-窗口函数)
  - [使用 MAX() 窗口函数](#使用-max-窗口函数)
  - [使用 MIN() 窗口函数](#使用-min-窗口函数)
  - [使用 NTILE() 窗口函数](#使用-ntile-窗口函数)
  - [使用 PERCENT\_RANK() 函数](#使用-percent_rank-函数)
  - [使用 RANK() 窗口函数](#使用-rank-窗口函数)
  - [使用 ROW\_NUMBER() 窗口函数](#使用-row_number-窗口函数)
  - [使用 QUALIFY 窗口函数](#使用-qualify-窗口函数)
  - [使用 SUM() 窗口函数](#使用-sum-窗口函数)
  - [使用 VARIANCE, VAR\_POP, VARIANCE\_POP 窗口函数](#使用-variance-var_pop-variance_pop-窗口函数)
  - [使用 VAR\_SAMP, VARIANCE\_SAMP 窗口函数](#使用-var_samp-variance_samp-窗口函数)
  - [使用 STD, STDDEV, STDDEV\_POP 窗口函数](#使用-std-stddev-stddev_pop-窗口函数)
  - [使用 STDDEV\_SAMP 窗口函数](#使用-stddev_samp-窗口函数)
  - [使用 COVAR\_SAMP 窗口函数](#使用-covar_samp-窗口函数)
  - [使用 COVAR\_POP 窗口函数](#使用-covar_pop-窗口函数)
  - [使用 CORR 窗口函数](#使用-corr-窗口函数)

本文介绍如何使用 StarRocks 窗口函数。

窗口函数是 StarRocks 内置的特殊函数。和聚合函数类似，窗口函数通过对多行数据进行计算得到一个数据值。不同的是，窗口函数使用 OVER() 子句对**当前窗口**内的数据进行排序和分组，同时**对结果集的每一行**计算出一个单独的值，而不是对每个 GROUP BY 分组计算一个值。这种灵活的方式允许您在 SELECT 子句中增加额外的列，对结果集进行重新组织和过滤。

窗口函数在金融和科学计算领域较为常用，常被用来分析趋势、计算离群值以及对大量数据进行分桶分析等。

## 窗口函数语法及参数

### 语法

```SQL
FUNCTION(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

> 注意：窗口函数只能出现在 SELECT 列表和最外层的 ORDER BY 子句中。在查询过程中，窗口函数会在最后生效，也就是在执行完 Join，Where 和 GROUP BY 等操作之后生效。

### 参数说明

- **partition_by_clause**：Partition By 子句。该子句将输入行按照指定的一列或多列分组，相同值的行会被分到一组。
- **order_by_clause**：Order By 子句。与外层的 Order By 类似，Order By 子句定义了输入行的排列顺序，如果指定了 Partition By，则 Order By 定义了每个 Partition 分组内的顺序。与外层 Order By 的唯一不同在于，OVER() 子句中的 `Order By n`（`n` 是正整数）相当于不做任何操作，而外层的 `Order By n` 表示按照第 `n` 列排序。

  以下示例展示了在 SELECT 列表中增加一个 `id` 列，它的值是 `1`，`2`，`3` 等，顺序按照 `events` 表中的 `date_and_time` 列排序。

  ```SQL
  SELECT row_number() 
  OVER (ORDER BY date_and_time) 
    AS id,
       c1, c2, c3, c4
  FROM events;
  ```

- **window_clause**：Window 子句，可以用来为窗口函数指定一个运算范围，以当前行为准，前后若干行作为窗口函数运算的对象。Window 子句支持的函数有：`AVG()`、`COUNT()`、`FIRST_VALUE()`、`LAST_VALUE()` 和 `SUM()`。对于 `MAX()` 和 `MIN()`，Window 子句可以通过 UNBOUNDED、PRECEDING 关键词指定开始范围。

    Window 子句语法：

    ```SQL
    ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
    ```

    > 注意：Window 子句必须在 Order By 子句之内。

## 窗口函数建表示例

本节创建的 `scores` 表将用于下面多个函数的示例。

```SQL
CREATE TABLE `scores` (
    `id` int(11) NULL,
    `name` varchar(11) NULL,
    `subject` varchar(11) NULL,
    `score` int(11) NULL
  )
DISTRIBUTED BY HASH(`score`) BUCKETS 10;

INSERT INTO `scores` VALUES
  (1, "lily", "math", NULL),
  (1, "lily", "english", 100),
  (1, "lily", "physics", 60),
  (2, "tom", "math", 80),
  (2, "tom", "english", 98),
  (2, "tom", "physics", NULL),
  (3, "jack", "math", 95),
  (3, "jack", "english", NULL),
  (3, "jack", "physics", 99),
  (4, "amy", "math", 80),
  (4, "amy", "english", 92),
  (4, "amy", "physics", 99),
  (5, "mike", "math", 70),
  (5, "mike", "english", 85),
  (5, "mike", "physics", 85),
  (6, "amber", "math", 92),
  (6, "amber", NULL, 90),
  (6, "amber", "physics", 100);
  ```

## 使用 AVG() 窗口函数

`AVG()` 函数用于计算特定窗口内选中字段的平均值。该函数忽略 NULL 值。

**语法**：

```SQL
AVG( expr ) [OVER (*analytic_clause*)]
```

**示例**：

以下示例模拟如下的股票数据，股票代码是 `JDR`，`closing price` 代表其每天的收盘价。

```SQL
CREATE TABLE stock_ticker (
    stock_symbol  STRING,
    closing_price DECIMAL(8,2),
    closing_date  DATETIME
)
DUPLICATE KEY(stock_symbol)
COMMENT "OLAP"
DISTRIBUTED BY HASH(closing_date);

INSERT INTO stock_ticker VALUES 
    ("JDR", 12.86, "2014-10-02 00:00:00"), 
    ("JDR", 12.89, "2014-10-03 00:00:00"), 
    ("JDR", 12.94, "2014-10-04 00:00:00"), 
    ("JDR", 12.55, "2014-10-05 00:00:00"), 
    ("JDR", 14.03, "2014-10-06 00:00:00"), 
    ("JDR", 14.75, "2014-10-07 00:00:00"), 
    ("JDR", 13.98, "2014-10-08 00:00:00")
;
```

计算该股票每日与其前后一日的收盘价均值。

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

返回：

```Plain Text
+--------------+---------------------+---------------+----------------+
| stock_symbol | closing_date        | closing_price | moving_average |
+--------------+---------------------+---------------+----------------+
| JDR          | 2014-10-02 00:00:00 |         12.86 |    12.87500000 |
| JDR          | 2014-10-03 00:00:00 |         12.89 |    12.89666667 |
| JDR          | 2014-10-04 00:00:00 |         12.94 |    12.79333333 |
| JDR          | 2014-10-05 00:00:00 |         12.55 |    13.17333333 |
| JDR          | 2014-10-06 00:00:00 |         14.03 |    13.77666667 |
| JDR          | 2014-10-07 00:00:00 |         14.75 |    14.25333333 |
| JDR          | 2014-10-08 00:00:00 |         13.98 |    14.36500000 |
+--------------+---------------------+---------------+----------------+
```

比如，第一行的 `moving_average` 取值 `12.87500000`，是 "2014-10-02" 的值 `12.86`，加前一天 "2014-10-02" 的值 null，再加后一天 "2014-10-03" 的值 `12.89` 之后的平均值。

## 使用 COUNT() 窗口函数

`COUNT()` 函数用于返回特定窗口内满足要求的行的数目。

**语法**：

```SQL
COUNT(expr) [OVER (analytic_clause)]
```

**示例**：

以下示例计算从**当前行到第一行**科目 `math` 分数大于 90 分的个数。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    count(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and current row
        ) as 'score_count'
from scores where subject in ('math') and score > 90;
```

返回：

```plaintext
+------+-------+---------+-------+-------------+
| id   | name  | subject | score | score_count |
+------+-------+---------+-------+-------------+
|    6 | amber | math    |    92 |           1 |
|    3 | jack  | math    |    95 |           2 |
+------+-------+---------+-------+-------------+
```

## 使用 CUME_DIST() 窗口函数

计算某个窗口或分区中某个值的累积分布，取值范围 0 到 1。常用于统计一个记录集中最高或者最低值的分布情况，即一个值在该记录集中的相对位置。比如，收入或销量前 10% 的人、考试排名后 5% 的学生等。

- 如果数据按升序排列，则统计小于等于当前值的数据在分区的占比。
- 如果数据按降序排列，则统计大于等于当前值的数据在分区的占比。

该函数从 3.2 版本开始支持。

**语法**：

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

- `partition_by_clause`: 可选。如果省略该子句，CUME_DIST() 函数会将整个结果集视为单个分区。
- `order_by_clause`：必填。该函数必须与 ORDER BY 一起使用，对排序后的数据进行分布统计。

CUME_DIST() 将 NULL 值作为最小值处理。

**示例**：

以下示例计算各个科目下每个得分按照升序排序后的累积分布情况。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```sql
SELECT *, 
    cume_dist() 
      OVER (
        PARTITION BY subject
        ORDER BY score
      ) AS cume_dist 
FROM scores;
```

返回：

```plaintext
+------+-------+---------+-------+---------------------+
| id   | name  | subject | score | cume_dist           |
+------+-------+---------+-------+---------------------+
|    6 | amber | NULL    |    90 |                   1 |
|    3 | jack  | english |  NULL |                 0.2 |
|    5 | mike  | english |    85 |                 0.4 |
|    4 | amy   | english |    92 |                 0.6 |
|    2 | tom   | english |    98 |                 0.8 |
|    1 | lily  | english |   100 |                   1 |
|    1 | lily  | math    |  NULL | 0.16666666666666666 |
|    5 | mike  | math    |    70 |  0.3333333333333333 |
|    2 | tom   | math    |    80 |  0.6666666666666666 |
|    4 | amy   | math    |    80 |  0.6666666666666666 |
|    6 | amber | math    |    92 |  0.8333333333333334 |
|    3 | jack  | math    |    95 |                   1 |
|    2 | tom   | physics |  NULL | 0.16666666666666666 |
|    1 | lily  | physics |    60 |  0.3333333333333333 |
|    5 | mike  | physics |    85 |                 0.5 |
|    4 | amy   | physics |    99 |  0.8333333333333334 |
|    3 | jack  | physics |    99 |  0.8333333333333334 |
|    6 | amber | physics |   100 |                   1 |
+------+-------+---------+-------+---------------------+
```

- 对于第一行 `cume_dist` 数据 `1`，分组 NULL 中只有这一行数据，且只有这一行自身满足 ”小于等于当前行“ 的要求，所以累积分布为 1。
- 对于第二行数据 `0.2`，分组 `english` 中有 5 行数据，且只有这一行自身满足 ”小于等于当前行“ 的要求，所以累积分布为 0.2。
- 对于第三行数据 `0.4`，分组 `english` 中有 5 行数据，且有两行（85 和 NULL）满足 ”小于等于当前行“ 的要求，所以累积分布为 0.4。

## 使用 DENSE_RANK() 窗口函数

`DENSE_RANK()` 函数用来为特定窗口中的数据排名。当函数中出现相同排名时，下一行的排名为相同排名数加 1。因此，`DENSE_RANK()` 返回的序号**是连续的数字**。而 `RANK()` 返回的序号**有可能是不连续的数字**。举例：如果前面有两个排名 1，DENSE_RANK() 第三行仍然会返回排名 2，但是 RANK() 第三行会返回 3。

**语法**：

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

**示例**：

以下示例使用 `DENSE_RANK()` 对 math 科目的得分排名（采用降序）。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    dense_rank()
        over (
            partition by subject
            order by score desc
        ) as `rank`
from scores where subject in ('math');
```

返回：

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    2 | tom   | math    |    80 |    3 |
|    4 | amy   | math    |    80 |    3 |
|    5 | mike  | math    |    70 |    4 |
|    1 | lily  | math    |  NULL |    5 |
+------+-------+---------+-------+------+
```

示例中有两个得分 80，排名都为 3，下一行的 70 排名是 4，排名是连续的。

## 使用 FIRST_VALUE() 窗口函数

`FIRST_VALUE()` 函数返回窗口范围内的**第一个**值。

**语法**：

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

从 2.5 版本开始支持 `IGNORE NULLS`，即是否在计算结果中忽略 NULL 值。如果不指定 `IGNORE NULLS`，默认会包含 NULL 值。比如，如果第一个值为 NULL，则返回 NULL。如果指定了 `IGNORE NULLS`，会返回第一个非 NULL 值。如果所有值都为 NULL，那么即使指定了 `IGNORE NULLS`，也会返回 NULL。

**示例**：

以下示例使用 `FIRST_VALUE()` 函数和 IGNORE NULLS，根据 `subject` 列分组，按照降序返回每个分组中的最高分。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    first_value(score IGNORE NULLS)
        over (
            partition by subject
            order by score desc
        ) as first
from scores;
```

返回：

```Plain Text
+------+-------+---------+-------+-------+
| id   | name  | subject | score | first |
+------+-------+---------+-------+-------+
|    1 | lily  | english |   100 |   100 |
|    2 | tom   | english |    98 |   100 |
|    4 | amy   | english |    92 |   100 |
|    5 | mike  | english |    85 |   100 |
|    3 | jack  | english |  NULL |   100 |
|    6 | amber | physics |   100 |   100 |
|    3 | jack  | physics |    99 |   100 |
|    4 | amy   | physics |    99 |   100 |
|    5 | mike  | physics |    85 |   100 |
|    1 | lily  | physics |    60 |   100 |
|    2 | tom   | physics |  NULL |   100 |
|    6 | amber | NULL    |    90 |    90 |
|    3 | jack  | math    |    95 |    95 |
|    6 | amber | math    |    92 |    95 |
|    2 | tom   | math    |    80 |    95 |
|    4 | amy   | math    |    80 |    95 |
|    5 | mike  | math    |    70 |    95 |
|    1 | lily  | math    |  NULL |    95 |
+------+-------+---------+-------+-------+
```

## 使用 LAST_VALUE() 窗口函数

`LAST_VALUE()` 返回窗口范围内的**最后一个**值。与 `FIRST_VALUE()` 相反。

**语法**：

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

从 2.5 版本开始支持 `IGNORE NULLS`，即是否在计算结果中忽略 NULL 值。如果不指定 `IGNORE NULLS`，默认会包含 NULL 值。比如，如果最后一个值为 NULL，则返回 NULL。如果指定了 `IGNORE NULLS`，会返回最后一个非 NULL 值。如果所有值都为 NULL，那么即使指定了 `IGNORE NULLS`，也会返回 NULL。

LAST_VALUE() 默认会统计 `rows between unbounded preceding and current row`，即会对比当前行与之前所有行。如果每个分区只想显示一个结果，可以在 ORDER BY 后使用 `rows between unbounded preceding and unbounded following`.

**示例**：

以下示例使用 `LAST_VALUE()` 函数，根据 `subject` 列分组，按照降序返回每个分组中的最低得分。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    last_value(score IGNORE NULLS)
        over (
            partition by subject
            order by score desc
            rows between unbounded preceding and unbounded following
        ) as last
from scores;
```

返回：

```Plain Text
+------+-------+---------+-------+------+
| id   | name  | subject | score | last |
+------+-------+---------+-------+------+
|    1 | lily  | english |   100 |   85 |
|    2 | tom   | english |    98 |   85 |
|    4 | amy   | english |    92 |   85 |
|    5 | mike  | english |    85 |   85 |
|    3 | jack  | english |  NULL |   85 |
|    6 | amber | physics |   100 |   60 |
|    3 | jack  | physics |    99 |   60 |
|    4 | amy   | physics |    99 |   60 |
|    5 | mike  | physics |    85 |   60 |
|    1 | lily  | physics |    60 |   60 |
|    2 | tom   | physics |  NULL |   60 |
|    6 | amber | NULL    |    90 |   90 |
|    3 | jack  | math    |    95 |   70 |
|    6 | amber | math    |    92 |   70 |
|    2 | tom   | math    |    80 |   70 |
|    4 | amy   | math    |    80 |   70 |
|    5 | mike  | math    |    70 |   70 |
|    1 | lily  | math    |  NULL |   70 |
+------+-------+---------+-------+------+
```

## 使用 LAG() 窗口函数

用来计算当前行**之前**若干行的值。该函数可用于直接比较行间差值或进行数据过滤。

`LAG()` 函数支持查询以下数据类型：

- 数值类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL
- 字符串类型：CHAR、VARCHAR
- 时间类型：DATE、DATETIME
- 从 2.5 版本开始，`LAG()` 函数支持查询 BITMAP 和 HLL 类型的数据。

**语法**：

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**参数说明**：

- `expr`: 需要计算的目标字段。
- `offset`: 偏移量，表示向前查找的行数，必须为**正整数**。如果未指定，默认按照 1 处理。
- `default`: 没有找到符合条件的行时，返回的默认值。如果未指定 `default`，默认返回 NULL。`default` 的数据类型必须和 `expr` 兼容。
- `IGNORE NULLS`：从 3.0 版本开始，`LAG()` 支持 `IGNORE NULLS`，即是否在计算结果中忽略 NULL 值。如果不指定 `IGNORE NULLS`，默认返回结果会包含 NULL 值。比如，如果指定的当前行之前的第 `offset` 行的值为 NULL，则返回 NULL，参考示例一。如果指定了 `IGNORE NULLS`，向前遍历 `offset` 行时会忽略取值为 NULL 的行，继续向前遍历非 NULL 值。如果指定了 IGNORE NULLS，但是在当前行之前并不存在 offset 个非 NULL 值，则返回 NULL 或 `default` (如果指定)，参考示例二。

**示例**：

示例一：LAG() 中未指定 IGNORE NULLS

建表并插入数据：

```SQL
CREATE TABLE test_tbl (col_1 INT, col_2 INT)
DISTRIBUTED BY HASH(col_1);

INSERT INTO test_tbl VALUES 
    (1, NULL),
    (2, 4),
    (3, NULL),
    (4, 2),
    (5, NULL),
    (6, 7),
    (7, 6),
    (8, 5),
    (9, NULL),
    (10, NULL);
```

查询数据，指定 `offset` 为 2，向前查找 2 行；`default` 为 0，表示如果没有符合条件的行，则返回 0。

返回：

```SQL
SELECT col_1, col_2, LAG(col_2,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+---------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+---------------------------------------------+
|     1 |  NULL |                                           0 |
|     2 |     4 |                                           0 |
|     3 |  NULL |                                        NULL |
|     4 |     2 |                                           4 |
|     5 |  NULL |                                        NULL |
|     6 |     7 |                                           2 |
|     7 |     6 |                                        NULL |
|     8 |     5 |                                           7 |
|     9 |  NULL |                                           6 |
|    10 |  NULL |                                           5 |
+-------+-------+---------------------------------------------+
```

可以看到对于前两行，往前遍历时不存在 2 个 非 NULL 值，因此返回默认值 0。

对于第 3 行数据 NULL，往前遍历两行对应的值是 NULL，因为未指定 IGNORE NULLS，允许返回结果包含 NULL，所以返回 NULL。

示例二：LAG() 中指定了 IGNORE NULLS

依然使用上面的数据表。

```SQL
SELECT col_1, col_2, LAG(col_2 IGNORE NULLS,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+---------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+---------------------------------------------+
|     1 |  NULL |                                           0 |
|     2 |     4 |                                           0 |
|     3 |  NULL |                                           0 |
|     4 |     2 |                                           0 |
|     5 |  NULL |                                           4 |
|     6 |     7 |                                           4 |
|     7 |     6 |                                           2 |
|     8 |     5 |                                           7 |
|     9 |  NULL |                                           6 |
|    10 |  NULL |                                           6 |
+-------+-------+---------------------------------------------+
```

可以看到对于第 1-4 行，因为在当前行之前不存在 2 个 非 NULL 值，因此返回默认值 0。

对于第 7 行数据 6，往前遍历两行对应的值是 NULL，因为指定了 IGNORE NULLS，会忽略这一行，继续往前遍历，因此返回第 4 行的 2。

## 使用 LEAD() 窗口函数

用来计算当前行**之后**若干行的值。该函数可用于直接比较行间差值或进行数据过滤。

`LEAD()` 支持的数据类型与 [LAG](#使用-lag-窗口函数) 相同。

**语法**：

```Haskell
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**参数说明**：

- `expr`: 需要计算的目标字段。
- `offset`: 偏移量，表示向后查找的行数，必须为**正整数**。如果未指定，默认按照 1 处理。
- `default`: 没有找到符合条件的行时，返回的默认值。如果未指定 `default`，默认返回 NULL。`default` 的数据类型必须和 `expr` 兼容。
- `IGNORE NULLS`：从 3.0 版本开始，`LEAD()` 支持 `IGNORE NULLS`，即是否在计算结果中忽略 NULL 值。如果不指定 `IGNORE NULLS`，默认返回结果会包含 NULL 值。比如，如果指定的当前行之后的第 `offset` 行的值为 NULL，则返回 NULL，参考示例一。如果指定了 `IGNORE NULLS`，向后遍历 `offset` 行时会忽略取值为 NULL 的行，继续向后遍历非 NULL 值。如果指定了 IGNORE NULLS，但是在当前行之后并不存在 offset 个非 NULL 值，则返回 NULL 或 `default` (如果指定)，参考示例二。

**示例**：

示例一：LEAD() 中未指定 IGNORE NULLS

建表并插入数据：

```SQL
CREATE TABLE test_tbl (col_1 INT, col_2 INT)
DISTRIBUTED BY HASH(col_1);

INSERT INTO test_tbl VALUES 
    (1, NULL),
    (2, 4),
    (3, NULL),
    (4, 2),
    (5, NULL),
    (6, 7),
    (7, 6),
    (8, 5),
    (9, NULL),
    (10, NULL);
```

查询数据，指定 `offset` 为 2，向后查找 2 行；`default` 为 0，表示如果没有符合条件的行，则返回 0。

返回结果：

```SQL
SELECT col_1, col_2, LEAD(col_2,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+----------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+----------------------------------------------+
|     1 |  NULL |                                         NULL |
|     2 |     4 |                                            2 |
|     3 |  NULL |                                         NULL |
|     4 |     2 |                                            7 |
|     5 |  NULL |                                            6 |
|     6 |     7 |                                            5 |
|     7 |     6 |                                         NULL |
|     8 |     5 |                                         NULL |
|     9 |  NULL |                                            0 |
|    10 |  NULL |                                            0 |
+-------+-------+----------------------------------------------+
```

可以看到对于第 1 行数据 NULL，往后遍历两行对应的数据是 NULL，因为未指定 IGNORE NULLS，允许返回结果包含 NULL，所以返回 NULL。

对于最后两行，因为往后遍历时不存在 2 个 非 NULL 值，因此返回默认值 0。

示例二：LEAD() 中指定了 IGNORE NULLS

依然使用上面的数据表。

```SQL
SELECT col_1, col_2, LEAD(col_2 IGNORE NULLS,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+----------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+----------------------------------------------+
|     1 |  NULL |                                            2 |
|     2 |     4 |                                            7 |
|     3 |  NULL |                                            7 |
|     4 |     2 |                                            6 |
|     5 |  NULL |                                            6 |
|     6 |     7 |                                            5 |
|     7 |     6 |                                            0 |
|     8 |     5 |                                            0 |
|     9 |  NULL |                                            0 |
|    10 |  NULL |                                            0 |
+-------+-------+----------------------------------------------+
```

可以看到对于第 7-10 行，往后遍历时不存在 2 个 非 NULL 值，因此返回默认值 0。

对于第 1 行数据 NULL，往后遍历两行对应的值是 NULL，因为指定了 IGNORE NULLS，会忽略这一行，继续往前遍历，因此返回第 4 行的 2。

## 使用 MAX() 窗口函数

`MAX()` 函数返回当前窗口指定行数内数据的最大值。

**语法**：

```SQL
MAX(expr) [OVER (analytic_clause)]
```

**示例**：

以下示例计算**从第一行到当前行之后一行中**的 math 科目的得分最大值。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *, 
    max(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and 1 following
        ) as max
from scores
where subject in ('math');

+------+-------+---------+-------+------+
| id   | name  | subject | score | max  |
+------+-------+---------+-------+------+
|    1 | lily  | math    |  NULL |   70 |
|    5 | mike  | math    |    70 |   80 |
|    2 | tom   | math    |    80 |   80 |
|    4 | amy   | math    |    80 |   92 |
|    6 | amber | math    |    92 |   95 |
|    3 | jack  | math    |    95 |   95 |
+------+-------+---------+-------+------+
```

以下示例计算 `math` 科目所有行中的最大值。

```sql
select *,
    max(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following
        ) as max
from scores
where subject in ('math');
```

从 2.4 版本开始，该函数支持设置 `rows between n preceding and n following`，即支持计算当前行前n行及后 `n` 行中的最大值。比如要计算当前行前 3 行和后 2 行中的最大值，语句可写为：

```sql
select *,
    max(score)
        over (
            partition by subject
            order by score
            rows between 3 preceding and 2 following) as max
from scores
where subject in ('math');
```

## 使用 MIN() 窗口函数

`MIN()` 函数返回当前窗口指定行数内数据的最小值。

**语法**：

```sql
MIN(expr) [OVER (analytic_clause)]
```

**示例**

以下示例计算所有行中的 math 科目得分的最小值。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    min(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following) as min
from scores
where subject in ('math');
```

返回：

```Plain Text
+------+-------+---------+-------+------+
| id   | name  | subject | score | min  |
+------+-------+---------+-------+------+
|    1 | lily  | math    |  NULL |   70 |
|    5 | mike  | math    |    70 |   70 |
|    2 | tom   | math    |    80 |   70 |
|    4 | amy   | math    |    80 |   70 |
|    6 | amber | math    |    92 |   70 |
|    3 | jack  | math    |    95 |   70 |
+------+-------+---------+-------+------+
```

从 2.4 版本开始，该函数支持设置 `rows between n preceding and n following`，即支持计算当前行前n行以及后 `n` 行中的最小值。比如要计算当前行前 3 行和后 2 行中的最小值，语句可写为：

```SQL
select *,
    min(score)
        over (
            partition by subject
            order by score
            rows between 3 preceding and 2 following) as max
from scores
where subject in ('math');
```

## 使用 NTILE() 窗口函数

`NTILE()` 函数将分区中已排序的数据**尽可能均匀**地分配至指定数量（`num_buckets`）的桶中，并返回每一行所在的桶号。桶的编号从 `1` 开始直至 `num_buckets`。`NTILE()` 的返回类型为 BIGINT。

:::tip

- 如果分区包含的行数无法被 `num_buckets` 整除，那么会存在两个不同的分桶大小，它们的差值为 1。较大的分桶位于较小的分桶之前。
- 如果分区包含的行数可以被 `num_buckets` 整除，那么所有分桶的大小相同。

:::

**语法**：

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

其中，`num_buckets` 是要划分桶的数量，必须是一个常量正整数，最大值为 BIGINT 的最大值，即 `2^63 - 1`。

> 注意
>
> `NTILE()` 函数不能使用 Window 子句。

**示例**：

以下示例使用 `NTILE()` 函数将当前窗口中的数据划分至 `2` 个桶中，划分结果见 `bucket_id` 列。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```sql
select *,
    ntile(2)
        over (
            partition by subject
            order by score
        ) as bucket_id
from scores;
```

返回：

```Plain Text
+------+-------+---------+-------+-----------+
| id   | name  | subject | score | bucket_id |
+------+-------+---------+-------+-----------+
|    6 | amber | NULL    |    90 |         1 |
|    1 | lily  | math    |  NULL |         1 |
|    5 | mike  | math    |    70 |         1 |
|    2 | tom   | math    |    80 |         1 |
|    4 | amy   | math    |    80 |         2 |
|    6 | amber | math    |    92 |         2 |
|    3 | jack  | math    |    95 |         2 |
|    3 | jack  | english |  NULL |         1 |
|    5 | mike  | english |    85 |         1 |
|    4 | amy   | english |    92 |         1 |
|    2 | tom   | english |    98 |         2 |
|    1 | lily  | english |   100 |         2 |
|    2 | tom   | physics |  NULL |         1 |
|    1 | lily  | physics |    60 |         1 |
|    5 | mike  | physics |    85 |         1 |
|    3 | jack  | physics |    99 |         2 |
|    4 | amy   | physics |    99 |         2 |
|    6 | amber | physics |   100 |         2 |
+------+-------+---------+-------+-----------+
```

如上述例子所示，`num_buckets` 为 `2`，此时：

- 第 1 行是一个分区，划分在一个分桶中。
- 2-7 行是一个分区，其中前 3 行在第一个分桶中、后 3 行在第二个分桶中。

## 使用 PERCENT_RANK() 函数

计算当前行在所在的分区内的相对排名，百分比。计算公式为 `(Rank - 1)/(Rows in partition - 1)`。`Rank` 表示该行数据在该分区内的排名。

返回一个介于 0 和 1 之间的数。该函数常用于计算百分位和数据分布。

**语法：**

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

该函数必须与 ORDER BY 一起使用，对排序后的数据进行分布统计。NULL 值作为最小值处理。

**示例：**

以下示例计算科目 math 下得分的排名情况。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
SELECT *,
    PERCENT_RANK()
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS `percent_rank`
FROM scores where subject in ('math');
```

```plaintext
+------+-------+---------+-------+--------------+
| id   | name  | subject | score | percent_rank |
+------+-------+---------+-------+--------------+
|    1 | lily  | math    |  NULL |            0 |
|    5 | mike  | math    |    70 |          0.2 |
|    2 | tom   | math    |    80 |          0.4 |
|    4 | amy   | math    |    80 |          0.4 |
|    6 | amber | math    |    92 |          0.8 |
|    3 | jack  | math    |    95 |            1 |
+------+-------+---------+-------+--------------+
```

## 使用 RANK() 窗口函数

`RANK()` 函数用来对当前窗口内的数据进行排名，返回结果集是对分区内每行的排名，行的排名是相关行之前的排名数加一。与 `DENSE_RANK()` 不同的是，`RANK()` 返回的序号**有可能是不连续的数字**，而 `DENSE_RANK()` 返回的序号**是连续的数字**。举例：如果前面有两个排名 1，RANK() 第三行会返回 3，而 DENSE_RANK() 第三行仍然会返回排名 2。

**语法**：

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

**示例**：

以下示例对 math 科目的得分进行排名。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *, 
    rank() over(
        partition by subject
        order by score desc
    ) as `rank`
from scores where subject in ('math');
```

返回：

```Plain Text
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    4 | amy   | math    |    80 |    3 |
|    2 | tom   | math    |    80 |    3 |
|    5 | mike  | math    |    70 |    5 |
|    1 | lily  | math    |  NULL |    6 |
+------+-------+---------+-------+------+
```

示例中有两个得分 80，排名都为 3，下一行的 70 排名是 5。

## 使用 ROW_NUMBER() 窗口函数

`ROW_NUMBER()` 函数为每个 Partition 的每一行返回一个从 `1` 开始连续递增的整数。与 `RANK()` 和 `DENSE_RANK()` 不同的是，`ROW_NUMBER()` 返回的值**不会重复也不会出现空缺**，是**连续递增**的。

**语法**：

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

**示例**：

以下示例对以 `subject` 列为分区的 math 科目的得分进行排名。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *, 
    row_number() over(
        partition by subject 
        order by score desc
    ) as `rank`
from scores where subject in ('math');
```

返回：

```Plain Text
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    2 | tom   | math    |    80 |    3 |
|    4 | amy   | math    |    80 |    4 |
|    5 | mike  | math    |    70 |    5 |
|    1 | lily  | math    |  NULL |    6 |
+------+-------+---------+-------+------+
```

## 使用 QUALIFY 窗口函数

QUALIFY 子句用于过滤窗口函数的结果。在 SELECT 语句中，可以使用 QUALIFY 来设置过滤条件，从多条记录中筛选符合条件的记录。QUALIFY 与聚合函数中的 HAVING 子句功能类似。该函数从 2.5 版本开始支持。

QUALIFY 提供了一种更为简洁的数据筛选方式。比如，如果不使用 QUALIFY，过滤语句比较复杂：

```SQL
SELECT *
FROM (SELECT DATE,
             PROVINCE_CODE,
             TOTAL_SCORE,
             ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) AS SCORE_ROWNUMBER
      FROM example_table) T1
WHERE T1.SCORE_ROWNUMBER = 1;
```

使用 QUALIFY 之后，语句可以简化成这样：

```SQL
SELECT DATE, PROVINCE_CODE, TOTAL_SCORE
FROM example_table 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) = 1;
```

当前 QUALIFY 仅支持如下窗口函数：ROW_NUMBER()，RANK()，DENSE_RANK()。

**语法**：

```SQL
SELECT <column_list>
FROM <data_source>
[GROUP BY ...]
[HAVING ...]
QUALIFY <window_function>
[ ... ]
```

**参数说明**：

- `<column_list>`: 要获取数据的列，多列使用逗号隔开。
- `<data_source>`: 数据源，一般是表。
- `<window_function>`: 用于过滤数据的窗口函数。当前仅支持 ROW_NUMBER()，RANK()，DENSE_RANK()。

**示例**：

```SQL
CREATE TABLE sales_record (
   city_id INT,
   item STRING,
   sales INT
) DISTRIBUTED BY HASH(`city_id`);

INSERT INTO sales_record VALUES
(1,'fruit',95),
(2,'drinks',70),
(3,'fruit',87),
(4,'drinks',98);

SELECT * FROM sales_record ORDER BY city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       2 | drinks |    70 |
|       3 | fruit  |    87 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

示例一：获取表中行号大于 1 的记录，无分区。

```SQL
SELECT city_id, item, sales
FROM sales_record
QUALIFY row_number() OVER (ORDER BY city_id) > 1;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       2 | drinks |    70 |
|       3 | fruit  |    87 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

示例二：按照 `item` 将表分为 2 个分区，获取每个分区中 row number 为`1`的记录。

```SQL
SELECT city_id, item, sales
FROM sales_record 
QUALIFY ROW_NUMBER() OVER (PARTITION BY item ORDER BY city_id) = 1
ORDER BY city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       2 | drinks |    70 |
+---------+--------+-------+
```

示例三：按照 `item` 将表分为 2 个分区，使用 RANK() 获取每个分区里销量 `sales` 排名第一的记录。

```SQL
SELECT city_id, item, sales
FROM sales_record
QUALIFY RANK() OVER (PARTITION BY item ORDER BY sales DESC) = 1
ORDER BY city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

**注意事项**：

- 当前 QUALIFY 仅支持如下窗口函数：ROW_NUMBER()，RANK()，DENSE_RANK()。

- 带 QUALIFY 的查询语句中，子句的执行顺序如下：

1. FROM
2. WHERE
3. GROUP BY
4. HAVING
5. Window
6. QUALIFY
7. DISTINCT
8. ORDER BY
9. LIMIT

## 使用 SUM() 窗口函数

`SUM()` 函数对特定窗口内指定行求和。

**语法**：

```SQL
SUM(expr) [OVER (analytic_clause)]
```

**示例**：

以下示例将数据按照 `subject` 列进行分组，并在组内计算所有行 `score` 列数据的和。该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```SQL
select *,
    sum(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following
        ) as 'sum'
from scores;
```

返回：

```Plain Text
+------+-------+---------+-------+------+
| id   | name  | subject | score | sum  |
+------+-------+---------+-------+------+
|    6 | amber | NULL    |    90 |   90 |
|    1 | lily  | math    |  NULL |  417 |
|    5 | mike  | math    |    70 |  417 |
|    2 | tom   | math    |    80 |  417 |
|    4 | amy   | math    |    80 |  417 |
|    6 | amber | math    |    92 |  417 |
|    3 | jack  | math    |    95 |  417 |
|    3 | jack  | english |  NULL |  375 |
|    5 | mike  | english |    85 |  375 |
|    4 | amy   | english |    92 |  375 |
|    2 | tom   | english |    98 |  375 |
|    1 | lily  | english |   100 |  375 |
|    2 | tom   | physics |  NULL |  443 |
|    1 | lily  | physics |    60 |  443 |
|    5 | mike  | physics |    85 |  443 |
|    3 | jack  | physics |    99 |  443 |
|    4 | amy   | physics |    99 |  443 |
|    6 | amber | physics |   100 |  443 |
+------+-------+---------+-------+------+
```

## 使用 VARIANCE, VAR_POP, VARIANCE_POP 窗口函数

VARIANCE() 窗口函数用于统计表达式的总体方差。VAR_POP 和 VARIANCE_POP 是 VARIANCE 窗口函数的别名。

**语法**：

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *,
    variance(score)
        over (
            partition by subject
            order by score
        ) as 'variance'
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | variance           |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 | 22.222222222222225 |
|    4 | amy   | math    |    80 | 22.222222222222225 |
|    6 | amber | math    |    92 |  60.74999999999997 |
|    3 | jack  | math    |    95 |  82.23999999999998 |
+------+-------+---------+-------+--------------------+
```

## 使用 VAR_SAMP, VARIANCE_SAMP 窗口函数

VAR_SAMP() 窗口函数用于统计表达式的样本方差。

**语法**：

```sql
VAR_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *,
    VAR_SAMP(score)
       over (partition by subject
            order by score) as VAR_SAMP
from scores where subject in ('math');

+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | VAR_SAMP           |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 | 33.333333333333336 |
|    4 | amy   | math    |    80 | 33.333333333333336 |
|    6 | amber | math    |    92 |  80.99999999999996 |
|    3 | jack  | math    |    95 | 102.79999999999997 |
+------+-------+---------+-------+--------------------+
```

## 使用 STD, STDDEV, STDDEV_POP 窗口函数

STD() 窗口函数用于统计表达式的总体标准差。

**语法**：

```sql
STD(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *, STD(score)
    over (
        partition by subject
        order by score) as std
from scores where subject in ('math');
+------+-------+---------+-------+-------------------+
| id   | name  | subject | score | std               |
+------+-------+---------+-------+-------------------+
|    1 | lily  | math    |  NULL |              NULL |
|    5 | mike  | math    |    70 |                 0 |
|    4 | amy   | math    |    80 | 4.714045207910317 |
|    2 | tom   | math    |    80 | 4.714045207910317 |
|    6 | amber | math    |    92 | 7.794228634059946 |
|    3 | jack  | math    |    95 | 9.068627239003707 |
+------+-------+---------+-------+-------------------+
```

## 使用 STDDEV_SAMP 窗口函数

STDDEV_SAMP() 窗口函数用于统计表达式的样本标准差。

**语法**：

```sql
STDDEV_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *, STDDEV_SAMP(score)
    over (
        partition by subject
        order by score
        ) as STDDEV_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | STDDEV_SAMP        |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 |  5.773502691896258 |
|    4 | amy   | math    |    80 |  5.773502691896258 |
|    6 | amber | math    |    92 |  8.999999999999998 |
|    3 | jack  | math    |    95 | 10.139033484509259 |
+------+-------+---------+-------+--------------------+

select *, STDDEV_SAMP(score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as STDDEV_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | STDDEV_SAMP        |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |                  0 |
|    5 | mike  | math    |    70 | 7.0710678118654755 |
|    2 | tom   | math    |    80 |  5.773502691896258 |
|    4 | amy   | math    |    80 |  8.999999999999998 |
|    6 | amber | math    |    92 | 10.139033484509259 |
|    3 | jack  | math    |    95 | 10.139033484509259 |
+------+-------+---------+-------+--------------------+
```

## 使用 COVAR_SAMP 窗口函数

COVAR_SAMP() 窗口函数用于统计表达式的样本协方差。

**语法**：

```sql
COVAR_SAMP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *, COVAR_SAMP(id, score) 
    over (
        partition by subject
        order by score) as covar_samp
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | covar_samp           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                 NULL |
|    5 | mike  | math    |    70 |                    0 |
|    2 | tom   | math    |    80 |   -6.666666666666668 |
|    4 | amy   | math    |    80 |   -6.666666666666668 |
|    6 | amber | math    |    92 |                  4.5 |
|    3 | jack  | math    |    95 | -0.24999999999999822 |
+------+-------+---------+-------+----------------------+

select *, COVAR_SAMP(id,score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as COVAR_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | COVAR_SAMP           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                    0 |
|    5 | mike  | math    |    70 |                   -5 |
|    4 | amy   | math    |    80 |   -6.666666666666661 |
|    2 | tom   | math    |    80 |    4.500000000000004 |
|    6 | amber | math    |    92 | -0.24999999999999467 |
|    3 | jack  | math    |    95 | -0.24999999999999467 |
+------+-------+---------+-------+----------------------+
```

## 使用 COVAR_POP 窗口函数

COVAR_POP() 窗口函数用于统计表达式的总体协方差。

**语法**：

```sql
COVAR_POP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *, COVAR_POP(id, score)
    over (
        partition by subject
        order by score) as covar_pop
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | covar_pop            |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                 NULL |
|    5 | mike  | math    |    70 |                    0 |
|    2 | tom   | math    |    80 |  -4.4444444444444455 |
|    4 | amy   | math    |    80 |  -4.4444444444444455 |
|    6 | amber | math    |    92 |                3.375 |
|    3 | jack  | math    |    95 | -0.19999999999999857 |
+------+-------+---------+-------+----------------------+
```

## 使用 CORR 窗口函数

CORR() 窗口函数用于统计表达式的相关系数。

**语法**：

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13，3.0.7，3.1.4 版本起，该窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数说明**：

当表达式 `expr` 为列值时，支持以下数据类型: TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL

**示例**：

该示例使用 [`scores`](#窗口函数建表示例) 表中的数据。

```plaintext
select *, CORR(id, score)
    over (
        partition by subject
        order by score) as corr
FROM scores where subject in ('math');
+------+-------+---------+-------+-----------------------+
| id   | name  | subject | score | corr                  |
+------+-------+---------+-------+-----------------------+
|    5 | mike  | math    |    70 | -0.015594571538795355 |
|    1 | lily  | math    |  NULL | -0.015594571538795355 |
|    2 | tom   | math    |    80 | -0.015594571538795355 |
|    4 | amy   | math    |    80 | -0.015594571538795355 |
|    3 | jack  | math    |    95 | -0.015594571538795355 |
|    6 | amber | math    |    92 | -0.015594571538795355 |
+------+-------+---------+-------+-----------------------+

select *, CORR(id,score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as corr 
from scores where subject in ('math');
+------+-------+---------+-------+-------------------------+
| id   | name  | subject | score | corr                    |
+------+-------+---------+-------+-------------------------+
|    1 | lily  | math    |  NULL | 1.7976931348623157e+308 |
|    5 | mike  | math    |    70 |                      -1 |
|    2 | tom   | math    |    80 |     -0.7559289460184546 |
|    4 | amy   | math    |    80 |     0.29277002188455997 |
|    6 | amber | math    |    92 |   -0.015594571538795024 |
|    3 | jack  | math    |    95 |   -0.015594571538795024 |
+------+-------+---------+-------+-------------------------+
```
