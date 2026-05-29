---
displayed_sidebar: docs
keywords: ['窗口函数', '窗口']
sidebar_position: 0.9
---

# 窗口函数

## 背景

窗口函数是一种特殊的内置函数。与聚合函数类似，它也对多个输入行进行计算以获得单个数据值。不同之处在于，窗口函数在特定窗口内处理输入数据，而不是使用“group by”方法。每个窗口中的数据可以使用 over() 子句进行排序和分组。窗口函数**为每一行计算一个单独的值**，而不是为每个组计算一个值。这种灵活性允许用户在 select 子句中添加额外的列并进一步过滤结果集。窗口函数只能出现在 select 列表和子句的最外层位置。它在查询的末尾生效，即在执行 `join`、`where` 和 `group by` 操作之后。窗口函数常用于分析趋势、计算异常值以及对大规模数据执行分桶分析。

## 用法

### 语法

```SQL
function(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### PARTITION BY 子句

Partition By 子句类似于 Group By。它根据一个或多个指定的列对输入行进行分组。具有相同值的行被分到一起。

### ORDER BY 子句

`Order By` 子句与外部 `Order By` 基本相同。它定义了输入行的顺序。如果指定了 `Partition By`，则 `Order By` 定义了每个 Partition 分组内的顺序。唯一的区别是，`OVER` 子句中的 `Order By n` (n 是一个正整数) 等同于无操作，而外部 `Order By` 中的 `n` 表示按第 n 列排序。

示例：

此示例展示了在 select 列表中添加一个 id 列，其值为 1、2、3 等，并按 events 表中的 `date_and_time` 列排序。

```SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
```

### 窗口子句

窗口子句用于指定操作的行范围（基于当前行的前导行和后续行）。它支持以下语法：AVG()、COUNT()、FIRST_VALUE()、LAST_VALUE() 和 SUM()。对于 MAX() 和 MIN()，窗口子句可以指定从开始到 `UNBOUNDED PRECEDING`。

**语法：**

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
RANGE BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

:::note
**ARRAY_AGG() 窗口帧限制：**

当使用 ARRAY_AGG() 作为窗口函数时，只支持 RANGE 帧。不支持 ROWS 帧。例如：

```SQL
-- 支持：RANGE 帧
array_agg(col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 不支持：ROWS 帧（将导致错误）
array_agg(col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```

:::

## 窗口函数示例表

本节创建一个示例表 `scores`。您可以使用此表测试下面的许多窗口函数。

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

## 函数示例

本节描述 StarRocks 中支持的窗口函数。

### AVG()

计算给定窗口中字段的平均值。此函数忽略 NULL 值。

**语法：**

```SQL
AVG([DISTINCT] expr) [OVER (*analytic_clause*)]
```

`DISTINCT` 从 StarRocks v4.0 开始支持。指定后，AVG() 只计算窗口中不同值的平均值。

:::note
**窗口帧限制：**

当使用 AVG(DISTINCT) 作为窗口函数时，只支持 RANGE 帧。不支持 ROWS 帧。
:::

**示例**

**示例 1：基本用法**

以下示例使用股票数据作为例子。

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

计算当前行以及其前后各行的平均收盘价。

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

输出：

```plaintext
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

例如，第一行中的 `12.87500000` 是“2014-10-02” (`12.86`)、其前一天“2014-10-01”（null）和其后一天“2014-10-03” (`12.89`) 的收盘价的平均值。

**示例 2：在整个窗口上使用 AVG(DISTINCT)**

此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

计算所有行中不同分数的平均值：

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER () AS distinct_avg
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_avg|
+----+---------+-------+-------------+
|  1 | math    |    80 |       85.00 |
|  2 | math    |    85 |       85.00 |
|  3 | math    |    80 |       85.00 |
|  4 | english |    90 |       85.00 |
|  5 | english |    85 |       85.00 |
|  6 | english |    90 |       85.00 |
+----+---------+-------+-------------+
```

不同分数的平均值为 85.00 (`(80 + 85 + 90) / 3`)。

**示例 3：在带有 RANGE 帧的帧窗口上使用 AVG(DISTINCT)**

此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

使用 RANGE 帧计算每个主题分区中不同分数的平均值：

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_avg
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_avg|
+----+---------+-------+-------------+
|  1 | math    |    80 |       80.00 |
|  3 | math    |    80 |       80.00 |
|  2 | math    |    85 |       82.50 |
|  5 | english |    85 |       85.00 |
|  4 | english |    90 |       87.50 |
|  6 | english |    90 |       87.50 |
+----+---------+-------+-------------+
```

对于每一行，该函数计算从分区开始到当前行分数（包括当前行分数）的不同分数的平均值。

### ARRAY_AGG()

将窗口中的值（包括 NULL 值）聚合到数组中。您可以使用可选的 `ORDER BY` 子句对数组中的元素进行排序。

此函数从 v3.4 开始支持。

:::tip
**窗口帧限制：**

ARRAY_AGG() 作为窗口函数仅支持 RANGE 窗口帧。不支持 ROWS 窗口帧。如果未指定窗口帧，则使用默认的 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`。
:::

**语法：**

```SQL
ARRAY_AGG([DISTINCT] expr [ORDER BY expr [ASC | DESC]]) OVER([partition_by_clause] [order_by_clause] [window_clause])
```

**参数：**

- `expr`：要聚合的表达式。它可以是任何受支持数据类型的列。
- `DISTINCT`：可选。从结果数组中消除重复值。
- `ORDER BY`：可选。指定数组中元素的顺序。

**返回值：**

返回一个包含窗口中所有值的 ARRAY。

**使用说明：**

- **不支持 ROWS 帧。** 只有 RANGE 帧可以与 ARRAY_AGG() 作为窗口函数一起使用。使用 ROWS 帧将导致错误。
- NULL 值包含在结果数组中。
- 当指定 `DISTINCT` 时，重复值将从数组中移除。
- 当在 ARRAY_AGG() 中指定 `ORDER BY` 时，结果数组中的元素将相应地排序。

**示例**

这些示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

**示例 1：基本用法**

收集每个主题分区中的所有分数：

```SQL
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS score_array
FROM scores
WHERE subject = 'math';
```

输出：

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | score_array          |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [null]               |
|    5 | mike  | math    |    70 | [null,70]            |
|    2 | tom   | math    |    80 | [null,70,80,80]      |
|    4 | amy   | math    |    80 | [null,70,80,80]      |
|    6 | amber | math    |    92 | [null,70,80,80,92]   |
|    3 | jack  | math    |    95 | [null,70,80,80,92,95]|
+------+-------+---------+-------+----------------------+
```

注意：由于 RANGE 帧语义，具有相同 `score` 值（tom 和 amy 都有 80）的行会收到相同的数组。

**示例 2: 窗口函数 ARRAY_AGG(DISTINCT)**

在每个科目分区中收集不同的分数：

```SQL
SELECT *,
    array_agg(DISTINCT score)
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS distinct_scores
FROM scores
WHERE subject = 'math';
```

输出：

```plaintext
+------+-------+---------+-------+-------------------+
| id   | name  | subject | score | distinct_scores   |
+------+-------+---------+-------+-------------------+
|    1 | lily  | math    |  NULL | [null]            |
|    5 | mike  | math    |    70 | [null,70]         |
|    2 | tom   | math    |    80 | [null,70,80]      |
|    4 | amy   | math    |    80 | [null,70,80]      |
|    6 | amber | math    |    92 | [null,70,80,92]   |
|    3 | jack  | math    |    95 | [null,70,80,92,95]|
+------+-------+---------+-------+-------------------+
```

**示例 3: 带 ORDER BY 的 ARRAY_AGG()**

在数组中收集按降序排序的分数：

```SQL
SELECT *,
    array_agg(score ORDER BY score DESC)
        OVER (
            PARTITION BY subject
        ) AS scores_desc
FROM scores
WHERE subject = 'math';
```

输出：

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | scores_desc          |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [95,92,80,80,70,null]|
|    5 | mike  | math    |    70 | [95,92,80,80,70,null]|
|    2 | tom   | math    |    80 | [95,92,80,80,70,null]|
|    4 | amy   | math    |    80 | [95,92,80,80,70,null]|
|    6 | amber | math    |    92 | [95,92,80,80,70,null]|
|    3 | jack  | math    |    95 | [95,92,80,80,70,null]|
+------+-------+---------+-------+----------------------+
```

**示例 4: 带 RANGE 帧的 ARRAY_AGG()**

使用 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 收集整个分区中的所有分数：

```SQL
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS all_scores
FROM scores
WHERE subject = 'math';
```

输出：

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | all_scores           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [null,70,80,80,92,95]|
|    5 | mike  | math    |    70 | [null,70,80,80,92,95]|
|    2 | tom   | math    |    80 | [null,70,80,80,92,95]|
|    4 | amy   | math    |    80 | [null,70,80,80,92,95]|
|    6 | amber | math    |    92 | [null,70,80,80,92,95]|
|    3 | jack  | math    |    95 | [null,70,80,80,92,95]|
+------+-------+---------+-------+----------------------+
```

**示例 5: 按分数范围分区收集名称**

使用 `stock_ticker` 表，在移动窗口中收集股票代码：

```SQL
SELECT
    stock_symbol,
    closing_date,
    closing_price,
    array_agg(closing_price)
        OVER (
            PARTITION BY stock_symbol
            ORDER BY closing_date
        ) AS price_history
FROM stock_ticker;
```

输出：

```plaintext
+--------------+---------------------+---------------+---------------------------------------+
| stock_symbol | closing_date        | closing_price | price_history                         |
+--------------+---------------------+---------------+---------------------------------------+
| JDR          | 2014-10-02 00:00:00 |         12.86 | [12.86]                               |
| JDR          | 2014-10-03 00:00:00 |         12.89 | [12.86,12.89]                         |
| JDR          | 2014-10-04 00:00:00 |         12.94 | [12.86,12.89,12.94]                   |
| JDR          | 2014-10-05 00:00:00 |         12.55 | [12.86,12.89,12.94,12.55]             |
| JDR          | 2014-10-06 00:00:00 |         14.03 | [12.86,12.89,12.94,12.55,14.03]       |
| JDR          | 2014-10-07 00:00:00 |         14.75 | [12.86,12.89,12.94,12.55,14.03,14.75] |
| JDR          | 2014-10-08 00:00:00 |         13.98 | [12.86,12.89,12.94,12.55,14.03,14.75,13.98] |
+--------------+---------------------+---------------+---------------------------------------+
```

**示例 6: 无效用法 - ROWS 帧（将导致错误）**

以下查询将失败，因为不支持 ROWS 帧：

```SQL
-- 这将导致错误！
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  -- NOT SUPPORTED!
        ) AS score_array
FROM scores;
```

错误信息：

```plaintext
ERROR: array_agg as window function does not support ROWS frame type. Please use RANGE frame instead.
```

### COUNT()

计算给定窗口中满足指定条件的行总数。

**语法：**

```SQL
COUNT([DISTINCT] expr) [OVER (analytic_clause)]
```

StarRocks v4.0 及更高版本支持 `DISTINCT`。指定后，COUNT() 仅计算窗口中的不同值。

:::note
**窗口帧限制：**

当使用 COUNT(DISTINCT) 作为窗口函数时，只支持 RANGE 帧。不支持 ROWS 帧。例如：

```SQL
-- 支持：RANGE 帧
count(distinct col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 不支持：ROWS 帧（将导致错误）
count(distinct col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```

:::

**示例**

这些示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

**示例 1: 基本用法**

计算数学分区中从当前行到第一行，分数大于 90 的数学成绩的出现次数。此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

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

```plaintext
+------+-------+---------+-------+-------------+
| id   | name  | subject | score | score_count |
+------+-------+---------+-------+-------------+
|    6 | amber | math    |    92 |           1 |
|    3 | jack  | math    |    95 |           2 |
+------+-------+---------+-------+-------------+
```

**示例 2: 在整个窗口上使用 COUNT(DISTINCT)**

计算所有行中不同的分数：

```SQL
CREATE TABLE test_scores (
    id INT,
    subject VARCHAR(20),
    score INT
) DISTRIBUTED BY HASH(id);

INSERT INTO test_scores VALUES
    (1, 'math', 80),
    (2, 'math', 85),
    (3, 'math', 80),
    (4, 'english', 90),
    (5, 'english', 85),
    (6, 'english', 90);
```

```SQL
SELECT id, subject, score,
    COUNT(DISTINCT score) OVER () AS distinct_count
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+---------------+
| id | subject | score | distinct_count|
+----+---------+-------+---------------+
|  1 | math    |    80 |             4 |
|  2 | math    |    85 |             4 |
|  3 | math    |    80 |             4 |
|  4 | english |    90 |             4 |
|  5 | english |    85 |             4 |
|  6 | english |    90 |             4 |
+----+---------+-------+---------------+
```

不同的计数是 4（值：80、85、90，如果存在则包括 NULL）。

**示例 3: 在带 RANGE 帧的框架窗口上使用 COUNT(DISTINCT)**

使用 RANGE 帧计算每个科目分区中不同的分数：

```SQL
SELECT id, subject, score,
    COUNT(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_count
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+---------------+
| id | subject | score | distinct_count|
+----+---------+-------+---------------+
|  1 | math    |    80 |             1 |
|  3 | math    |    80 |             1 |
|  2 | math    |    85 |             2 |
|  5 | english |    85 |             1 |
|  4 | english |    90 |             2 |
|  6 | english |    90 |             2 |
+----+---------+-------+---------------+
```

对于每一行，该函数计算从分区开始到当前行分数（包括当前行分数）的不同分数。

### CUME_DIST()

CUME_DIST() 函数计算分区或窗口中值的累积分布，表示其在分区中的相对百分比位置。它常用于计算组中最高或最低值的分布。

- 如果数据按升序排序，此函数计算小于或等于当前行值的百分比。
- 如果数据按降序排序，此函数计算大于或等于当前行值的百分比。

累积分布的范围是 0 到 1。它对于百分位数计算和数据分布分析很有用。

此函数从 v3.2 开始支持。

**语法：**

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

- `partition_by_clause`：可选。如果未指定此子句，则将整个结果集作为一个分区进行处理。
- `order_by_clause`：**此函数必须与 ORDER BY 一起使用，以将分区行按所需顺序排序。**

CUME_DIST() 包含 NULL 值，并将其视为最低值。

**示例**

以下示例显示了每个 `subject` 组中每个分数的累积分布。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```plaintext
SELECT *, 
    cume_dist() 
      OVER (
        PARTITION BY subject
        ORDER BY score
      ) AS cume_dist 
FROM scores;
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

- 对于第一行中的 `cume_dist`，`NULL` 组只有一行，并且只有这一行本身满足“小于或等于当前行”的条件。累积分布为 1。
- 对于第二行中的 `cume_dist`，`english` 组有五行，并且只有这一行本身 (NULL) 满足“小于或等于当前行”的条件。累积分布为 0.2。
- 对于第三行中的 `cume_dist`，`english` 组有五行，并且有两行（85 和 NULL）满足“小于或等于当前行”的条件。累积分布为 0.4。

### DENSE_RANK()

DENSE_RANK() 函数用于表示排名。与 RANK() 不同，DENSE_RANK() **没有空缺** 数字。例如，如果有两个 1，DENSE_RANK() 的第三个数字仍然是 2，而 RANK() 的第三个数字是 3。

**语法：**

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

**示例**

以下示例显示了数学分数的排名（按降序排序）。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```SQL
select *,
    dense_rank()
        over (
            partition by subject
            order by score desc
        ) as `rank`
from scores where subject in ('math');
```

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

结果数据中有两行的分数为 80。它们的排名都是 3。下一个分数 70 的排名是 4。这表明 DENSE_RANK() **没有空缺** 数字。

### FIRST_VALUE()

FIRST_VALUE() 返回窗口范围的**第一个** 值。

**语法：**

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` 从 v2.5.0 开始支持。它用于确定是否从计算中排除 `expr` 的 NULL 值。默认情况下，NULL 值包含在内，这意味着如果过滤结果中的第一个值为 NULL，则返回 NULL。如果指定 IGNORE NULLS，则返回过滤结果中的第一个非 NULL 值。如果所有值都为 NULL，即使指定 IGNORE NULLS，也返回 NULL。

StarRocks v3.5 开始支持 ARRAY 类型。您可以将 FIRST_VALUE() 与 ARRAY 列一起使用，以获取窗口中的第一个数组值。

**示例**

**示例 1：基本用法**

返回每个组中每个成员的第一个 `score` 值（降序），按 `subject` 分组。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```SQL
select *,
    first_value(score IGNORE NULLS)
        over (
            partition by subject
            order by score desc
        ) as first
from scores;
```

```plaintext
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

**示例 2：将 FIRST_VALUE() 与 ARRAY 类型结合使用**

创建包含 ARRAY 列的表：

```SQL
CREATE TABLE test_array_value (
    col_1 INT,
    arr1 ARRAY<INT>
) DISTRIBUTED BY HASH(col_1);

INSERT INTO test_array_value (col_1, arr1) VALUES
    (1, [1, 11]),
    (2, [2, 22]),
    (3, [3, 33]),
    (4, NULL),
    (5, [5, 55]);
```

使用 FIRST_VALUE() 查询 ARRAY 类型数据：

```SQL
SELECT col_1, arr1, 
    FIRST_VALUE(arr1) OVER (ORDER BY col_1) AS first_array
FROM test_array_value;
```

输出：

```plaintext
+-------+--------+------------+
| col_1 | arr1   | first_array|
+-------+--------+------------+
|     1 | [1,11] | [1,11]     |
|     2 | [2,22] | [1,11]     |
|     3 | [3,33] | [1,11]     |
|     4 | NULL   | [1,11]     |
|     5 | [5,55] | [1,11]     |
+-------+--------+------------+
```

窗口中所有行都返回第一个数组值 `[1,11]`。

### LAST_VALUE()

LAST_VALUE() 返回 **最后一个** 窗口范围的值。它与 FIRST_VALUE() 相反。

**语法：**

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` 从 v2.5.0 开始支持。它用于确定是否从计算中排除 `expr` 的 NULL 值。默认情况下，NULL 值包含在内，这意味着如果过滤结果中的最后一个值为 NULL，则返回 NULL。如果指定 IGNORE NULLS，则返回过滤结果中的最后一个非 NULL 值。如果所有值都为 NULL，即使指定 IGNORE NULLS，也返回 NULL。

默认情况下，LAST_VALUE() 计算 `rows between unbounded preceding and current row`，它将当前行与其所有先行行进行比较。如果只想为每个分区显示一个值，请在 ORDER BY 之后使用 `rows between unbounded preceding and unbounded following`。

StarRocks v3.5 开始支持 ARRAY 类型。您可以将 LAST_VALUE() 与 ARRAY 列结合使用，以获取窗口中的最后一个数组值。

**示例**

**示例 1：基本用法**

返回组中每个成员的最后一个 `score`（降序），按 `subject` 分组。此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

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

```plaintext
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

**示例 2：将 LAST_VALUE() 与 ARRAY 类型结合使用**

使用 FIRST_VALUE() 示例 2 中的相同表：

```SQL
SELECT col_1, arr1, 
    LAST_VALUE(arr1) OVER (
        ORDER BY col_1 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_array
FROM test_array_value;
```

输出：

```plaintext
+-------+--------+-----------+
| col_1 | arr1   | last_array|
+-------+--------+-----------+
|     1 | [1,11] | [5,55]    |
|     2 | [2,22] | [5,55]    |
|     3 | [3,33] | [5,55]    |
|     4 | NULL   | [5,55]    |
|     5 | [5,55] | [5,55]    |
+-------+--------+-----------+
```

窗口中所有行都返回最后一个数组值 `[5,55]`。

### LAG()

返回比当前行滞后 `offset` 行的行的值。此函数常用于比较行之间的数据并过滤数据。

`LAG()` 可用于查询以下类型的数据：

- 数值型：TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
- 字符串型：CHAR, VARCHAR
- 日期型：DATE, DATETIME
- StarRocks v2.5 开始支持 BITMAP 和 HLL。
- StarRocks v3.5 开始支持 ARRAY 类型。

**语法：**

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**参数：**

- `expr`：要计算的字段。
- `offset`：偏移量。它必须是 **正整数**。如果未指定此参数，则默认为 1。
- `default`：如果未找到匹配行，则返回的默认值。如果未指定此参数，则默认为 NULL。`default` 支持任何与 `expr` 类型兼容的表达式，从 4.0 版本开始，默认值不再必须是常量，它可以是列名。
- `IGNORE NULLS` 从 v3.0 开始支持。它用于确定 `expr` 的 NULL 值是否包含在结果中。默认情况下，在计数 `offset` 行时包含 NULL 值，这意味着如果目标行的值为 NULL，则返回 NULL。请参阅示例 1。如果指定 IGNORE NULLS，则在计数 `offset` 行时忽略 NULL 值，系统会继续搜索 `offset` 个非 NULL 值。如果找不到 `offset` 个非 NULL 值，则返回 NULL 或 `default`（如果已指定）。请参阅示例 2。

**示例**

**示例 1: 未指定 IGNORE NULLS**

创建表并插入值:

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

查询此表中的数据，其中 `offset` 为 2，表示遍历前两行；`default` 为 0，表示如果没有找到匹配的行，则返回 0。

输出:

```plaintext
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

对于前两行，不存在前两行，因此返回默认值 0。

对于第 3 行中的 NULL，向后两行的值为 NULL，并且由于允许 NULL 值，因此返回 NULL。

**示例 2: 指定 IGNORE NULLS**

使用上述表和参数设置。

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

对于第 1 到 4 行，系统无法在前几行中为它们各自找到两个非 NULL 值，因此返回默认值 0。

对于第 7 行中的值 6，向后两行的值为 NULL，并且由于指定了 IGNORE NULLS，因此忽略 NULL。系统继续搜索非 NULL 值，并返回第 4 行中的 2。

**示例 3: 将 LAG() 中的默认值设置为列名**

使用上述表和参数设置。

```SQL
SELECT col_1, col_2, LAG(col_2 ,2,col_1) OVER (ORDER BY col_1)
FROM test_tbl ORDER BY col_1;
+-------+-------+-------------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, col_1) OVER (ORDER BY col_1 ASC ) |
+-------+-------+-------------------------------------------------+
|     1 |  NULL |                                               1 |
|     2 |     4 |                                               2 |
|     3 |  NULL |                                            NULL |
|     4 |     2 |                                               4 |
|     5 |  NULL |                                            NULL |
|     6 |     7 |                                               2 |
|     7 |     6 |                                            NULL |
|     8 |     5 |                                               7 |
|     9 |  NULL |                                               6 |
|    10 |  NULL |                                               5 |
+-------+-------+-------------------------------------------------+
```

如您所见，对于第 1 行和第 2 行，向后扫描时没有两个非 NULL 值，因此返回的默认值是当前行的 col_1 值。

所有其他行的行为与示例 1 相同。

**示例 4: 将 LAG() 与 ARRAY 类型一起使用**

创建具有 ARRAY 列的表:

```SQL
CREATE TABLE test_array_value (
    col_1 INT,
    arr1 ARRAY<INT>,
    arr2 ARRAY<INT> NOT NULL
) DISTRIBUTED BY HASH(col_1);

INSERT INTO test_array_value (col_1, arr1, arr2) VALUES
    (1, [1, 11], [101, 111]),
    (2, [2, 22], [102, 112]),
    (3, [3, 33], [103, 113]),
    (4, NULL,    [104, 114]),
    (5, [5, 55], [105, 115]),
    (6, [6, 66], [106, 116]);
```

使用 LAG() 和 ARRAY 类型查询数据:

```SQL
SELECT col_1, arr1, LAG(arr1, 2, arr2) OVER (ORDER BY col_1) AS lag_result 
FROM test_array_value;
```

输出:

```plaintext
+-------+--------+-------------+
| col_1 | arr1   | lag_result  |
+-------+--------+-------------+
|     1 | [1,11] | [101,111]   |
|     2 | [2,22] | [102,112]   |
|     3 | [3,33] | [1,11]      |
|     4 | NULL   | [2,22]      |
|     5 | [5,55] | [3,33]      |
|     6 | [6,66] | NULL        |
+-------+--------+-------------+
```

对于前两行，不存在前两行，因此返回 `arr2` 中的默认值。

### LEAD()

返回领先当前行 `offset` 行的行的值。此函数通常用于比较行之间的值和筛选数据。

可由 `LEAD()` 查询的数据类型与 [LAG()](#lag)。

**语法:**

```sql
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**参数:**

- `expr`: 要计算的字段。
- `offset`: 偏移量。它必须是一个正整数。如果未指定此参数，则默认为 1。
- `default`: 如果未找到匹配的行，则返回的默认值。如果未指定此参数，则默认为 NULL。`default` 支持任何类型与 `expr` 兼容的表达式，从 4.0 版本开始，默认值不再必须是常量，它可以是列名。
- `IGNORE NULLS` 从 v3.0 开始支持。它用于确定 `expr` 的 NULL 值是否包含在结果中。默认情况下，在计数 `offset` 行时包含 NULL 值，这意味着如果目标行的值为 NULL，则返回 NULL。请参阅示例 1。如果指定 IGNORE NULLS，则在计数 `offset` 行时忽略 NULL 值，系统继续搜索 `offset` 个非 NULL 值。如果找不到 `offset` 个非 NULL 值，则返回 NULL 或 `default`（如果已指定）。请参阅示例 2。

**示例**

**示例 1: 未指定 IGNORE NULLS**

创建表并插入值:

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

查询此表中的数据，其中 `offset` 为 2，表示遍历后续两行；`default` 为 0，表示如果没有找到匹配的行，则返回 0。

输出:

```plaintext
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

对于第一行，向前两行的值为 NULL，并且由于允许 NULL 值，因此返回 NULL。

对于最后两行，不存在后续两行，因此返回默认值 0。

**示例 2: 指定 IGNORE NULLS**

使用上述表和参数设置。

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

对于第 7 到 10 行，系统无法在后续行中找到两个非 NULL 值，因此返回默认值 0。

对于第一行，向前两行的值为 NULL，并且由于指定了 IGNORE NULLS，NULL 被忽略。系统继续搜索第二个非 NULL 值，并返回第 4 行中的 2。

**示例 3：将 LEAD() 中的默认值设置为列名**

使用上述表格和参数设置。

```SQL
SELECT col_1, col_2, LEAD(col_2 ,2,col_1) OVER (ORDER BY col_1)
FROM test_tbl ORDER BY col_1;
+-------+-------+--------------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, col_1) OVER (ORDER BY col_1 ASC ) |
+-------+-------+--------------------------------------------------+
|     1 |  NULL |                                             NULL |
|     2 |     4 |                                                2 |
|     3 |  NULL |                                             NULL |
|     4 |     2 |                                                7 |
|     5 |  NULL |                                                6 |
|     6 |     7 |                                                5 |
|     7 |     6 |                                             NULL |
|     8 |     5 |                                             NULL |
|     9 |  NULL |                                                9 |
|    10 |  NULL |                                               10 |
+-------+-------+--------------------------------------------------+
```

如您所见，对于第 9 行和第 10 行，向前扫描时没有两个非 NULL 值，因此返回的默认值是当前行的 col_1 值。

所有其他行的行为与示例 1 相同。

**示例 4：将 LEAD() 与 ARRAY 类型一起使用**

使用 LAG() 示例 4 中的相同表格：

```SQL
SELECT col_1, arr1, LEAD(arr1, 2, arr2) OVER (ORDER BY col_1) AS lead_result 
FROM test_array_value;
```

输出：

```plaintext
+-------+--------+-------------+
| col_1 | arr1   | lead_result |
+-------+--------+-------------+
|     1 | [1,11] | [3,33]      |
|     2 | [2,22] | NULL        |
|     3 | [3,33] | [5,55]      |
|     4 | NULL   | [6,66]      |
|     5 | [5,55] | [105,115]   |
|     6 | [6,66] | [106,116]   |
+-------+--------+-------------+
```

对于最后两行，不存在后续的两行，因此返回 `arr2` 中的默认值。

### MAX()

返回当前窗口中指定行的最大值。

**语法：**

```SQL
MAX(expr) [OVER (analytic_clause)]
```

**示例**

计算从第一行到当前行之后一行的最大值。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```SQL
select *,
    max(scores)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and 1 following
        ) as max
from scores
where subject in ('math');
```

```plain
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

以下示例计算 `math` 科目所有行中的最高分。

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

从 StarRocks 2.4 开始，您可以将行范围指定为 `rows between n preceding and n following`，这意味着您可以捕获当前行之前的 `n` 行和当前行之后的 `n` 行。

示例语句：

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

### MIN()

返回当前窗口中指定行的最小值。

**语法：**

```SQL
MIN(expr) [OVER (analytic_clause)]
```

**示例**

计算数学科目所有行中的最低分。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```SQL
select *, 
    min(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following)
            as min
from scores
where subject in ('math');
```

```plaintext
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

从 StarRocks 2.4 开始，您可以将行范围指定为 `rows between n preceding and n following`，这意味着您可以捕获当前行之前的 `n` 行和当前行之后的 `n` 行。

示例语句：

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

### NTILE()

NTILE() 函数将分区中已排序的行尽可能平均地划分为指定数量的 `num_buckets`，将划分后的行存储在各自的桶中，从 1 `[1, 2, ..., num_buckets]` 开始，并返回每行所在的桶号。

关于桶的大小：

- 如果行数可以被指定数量的 `num_buckets` 整除，则所有桶的大小都相同。
- 如果行数不能被指定数量的 `num_buckets` 整除，则会有两种不同大小的桶。大小差异为 1。行数较多的桶将排在行数较少的桶前面。

**语法：**

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`：要创建的桶的数量。该值必须是一个正整数常量，其最大值为 `2^63 - 1`。

NTILE() 函数中不允许使用窗口子句。

NTILE() 函数返回 BIGINT 类型的数据。

**示例**

以下示例将分区中的所有行划分为两个桶。此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

```sql
select *,
    ntile(2)
        over (
            partition by subject
            order by score
        ) as bucket_id
from scores;
```

输出：

```plaintext
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

如上例所示，当 `num_buckets` 为 `2` 时：

- 对于第一行，此分区只有此记录，并且只分配给一个桶。
- 对于第 2 到 7 行，该分区有 6 条记录，前 3 条记录分配给桶 1，其他 3 条记录分配给桶 2。

### PERCENT_RANK()

计算结果集中行的相对百分比排名。

PERCENT_RANK() 使用以下公式计算，其中 `Rank` 表示当前行在分区中的排名。

```plaintext
(Rank - 1)/(Rows in partition - 1)
```

返回值范围从 0 到 1。此函数对于百分位数计算和分析数据分布非常有用。它从 v3.2 开始支持。

**语法：**

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

:::note
PERCENT_RANK() 必须与 ORDER BY 一起使用，以将分区行按所需顺序排序。
:::

**示例**

以下示例显示了 `math` 组中每个 `score` 的相对排名。此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

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

### RANK()

RANK() 函数用于表示排名。与 DENSE_RANK() 不同，RANK() 将会**出现空缺**数字。例如，如果出现两个并列的 1，则 RANK() 的第三个数字将是 3 而不是 2。

**语法：**

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

**示例**

对组中的数学分数进行排名。此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

```SQL
select *, 
    rank() over(
        partition by subject
        order by score desc
        ) as `rank`
from scores where subject in ('math');
```

```plain
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

结果数据有两行分数为 80。它们都排名第 3。下一个分数 70 的排名是 5。

### ROW_NUMBER()

为分区的每一行返回一个从 1 开始连续递增的整数。与 RANK() 和 DENSE_RANK() 不同，ROW_NUMBER() 返回的值**不重复或有空缺**并且**连续递增**。

**语法：**

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

**示例**

对组中的数学分数进行排名。此示例使用 [示例表](#窗口函数示例表) `scores` 中的数据。

```SQL
select *, row_number() over(
    partition by subject
    order by score desc) as `rank`
from scores where subject in ('math');
```

```plaintext
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

### QUALIFY()

QUALIFY 子句用于过滤窗口函数的结果。在 SELECT 语句中，您可以使用 QUALIFY 子句对列应用条件以过滤结果。QUALIFY 类似于聚合函数中的 HAVING 子句。此函数从 v2.5 开始支持。

QUALIFY 简化了 SELECT 语句的编写。

在使用 QUALIFY 之前，SELECT 语句可能如下所示：

```SQL
SELECT *
FROM (SELECT DATE,
             PROVINCE_CODE,
             TOTAL_SCORE,
             ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) AS SCORE_ROWNUMBER
      FROM example_table) T1
WHERE T1.SCORE_ROWNUMBER = 1;
```

使用 QUALIFY 后，语句缩短为：

```SQL
SELECT DATE, PROVINCE_CODE, TOTAL_SCORE
FROM example_table 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) = 1;
```

QUALIFY 仅支持以下三种窗口函数：ROW_NUMBER()、RANK() 和 DENSE_RANK()。

**语法：**

```SQL
SELECT <column_list>
FROM <data_source>
[GROUP BY ...]
[HAVING ...]
QUALIFY <window_function>
[ ... ]
```

**参数：**

`<column_list>`：您要从中获取数据的列。

`<data_source>`：数据源通常是一个表。

`<window_function>`：`QUALIFY` 子句只能后跟窗口函数，包括 ROW_NUMBER()、RANK() 和 DENSE_RANK()。

**示例**

```SQL
-- 创建表。
CREATE TABLE sales_record (
   city_id INT,
   item STRING,
   sales INT
) DISTRIBUTED BY HASH(`city_id`);

-- 将数据插入表中。
insert into sales_record values
(1,'fruit',95),
(2,'drinks',70),
(3,'fruit',87),
(4,'drinks',98);

-- 从表中查询数据。
select * from sales_record order by city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       2 | drinks |    70 |
|       3 | fruit  |    87 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

**示例 1：从表中获取行号大于 1 的记录**

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

**示例 2：从表的每个分区中获取行号为 1 的记录**

该表通过 `item` 分为两个分区，并返回每个分区的首行。

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
2 rows in set (0.01 sec)
```

**示例 3：从表的每个分区中获取销售排名第一的记录**

该表通过 `item` 分为两个分区，并返回每个分区中销售额最高的行。

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

**使用说明：**

- QUALIFY 仅支持以下三种窗口函数：ROW_NUMBER()、RANK() 和 DENSE_RANK()。

- 包含 QUALIFY 的查询中子句的执行顺序如下：

1. FROM
2. WHERE
3. GROUP BY
4. HAVING
5. Window
6. QUALIFY
7. DISTINCT
8. ORDER BY
9. LIMIT

### SUM()

计算指定行的总和。

**语法：**

```SQL
SUM([DISTINCT] expr) [OVER (analytic_clause)]
```

`DISTINCT` 从 StarRocks v4.0 开始支持。指定时，SUM() 仅对窗口中的不同值求和。

:::note
**窗口帧限制：**

当使用 SUM(DISTINCT) 作为窗口函数时，仅支持 RANGE 帧。不支持 ROWS 帧。
:::

**示例**

这些示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

**示例 1：基本用法**

按 `subject` 对数据进行分组，并计算组内所有行的分数总和。

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

```plaintext
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

**示例 2：在整个窗口上使用 SUM(DISTINCT)**

计算所有行的不同分数总和：

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER () AS distinct_sum
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_sum|
+----+---------+-------+-------------+
|  1 | math    |    80 |          255|
|  2 | math    |    85 |          255|
|  3 | math    |    80 |          255|
|  4 | english |    90 |          255|
|  5 | english |    85 |          255|
|  6 | english |    90 |          255|
+----+---------+-------+-------------+
```

不同的总和为 255 (80 + 85 + 90)。

**示例 3：在带 RANGE 帧的框架窗口上使用 SUM(DISTINCT)**

使用 RANGE 帧计算每个主题分区中不同分数的总和：

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_sum
FROM test_scores;
```

输出：

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_sum|
+----+---------+-------+-------------+
|  1 | math    |    80 |           80|
|  3 | math    |    80 |           80|
|  2 | math    |    85 |          165|
|  5 | english |    85 |           85|
|  4 | english |    90 |          175|
|  6 | english |    90 |          175|
+----+---------+-------+-------------+
```

对于每一行，该函数计算从分区开始到当前行分数（包括当前行分数）的所有不同分数的总和。

### VARIANCE, VAR_POP, VARIANCE_POP

返回表达式的总体方差。VAR_POP 和 VARIANCE_POP 是 VARIANCE 的别名。自 v2.5.10 起，这些函数可用作窗口函数。

**语法：**

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须评估为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

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

### VAR_SAMP, VARIANCE_SAMP

返回表达式的样本方差。自 v2.5.10 起，这些函数可用作窗口函数。

**语法：**

```sql
VAR_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须评估为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

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

### STD, STDDEV, STDDEV_POP

返回表达式的标准差。自 v2.5.10 起，这些函数可用作窗口函数。

**语法：**

```sql
STD(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须评估为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

此示例使用以下数据：[示例表](#窗口函数示例表) `scores`。

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

### STDDEV_SAMP

返回表达式的样本标准差。自 v2.5.10 起，此函数可用作窗口函数。

**语法：**

```sql
STDDEV_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须评估为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

本示例使用[示例表](#窗口函数示例表)`scores` 中的数据。

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

### COVAR_SAMP

返回两个表达式的样本协方差。此函数从 v2.5.10 开始支持。它也是一个聚合函数。

**语法：**

```sql
COVAR_SAMP(expr1,expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须求值为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

本示例使用[示例表](#窗口函数示例表)`scores` 中的数据。

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
```

### COVAR_POP

返回两个表达式的总体协方差。此函数从 v2.5.10 开始支持。它也是一个聚合函数。

**语法：**

```sql
COVAR_POP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须求值为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

本示例使用[示例表](#窗口函数示例表)`scores` 中的数据。

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

### CORR

返回两个表达式之间的 Pearson 相关系数。此函数从 v2.5.10 开始支持。它也是一个聚合函数。

**语法：**

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
从 2.5.13、3.0.7、3.1.4 及更高版本开始，此窗口函数支持 ORDER BY 和 Window 子句。
:::

**参数：**

如果 `expr` 是表列，则它必须求值为 TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE 或 DECIMAL。

**示例**

本示例使用[示例表](#窗口函数示例表)`scores` 中的数据。

```plaintext
select *, CORR(id, score)
    over (
        partition by subject
        order by score) as corr
from scores where subject in ('math');
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
