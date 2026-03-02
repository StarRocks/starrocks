---
displayed_sidebar: docs
keywords: ['analytic']
sidebar_position: 0.9
---

# Window functions

## Background

The window function is a special class of built-in functions. Similar to the aggregation function, it also does calculations on multiple input rows to get a single data value. The difference is that the window function processes the input data within a specific window, rather than using the "group by" method. The data in each window can be sorted and grouped using the over() clause. The window function **computes a separate value for each row**, rather than computing one value for each group. This flexibility allows users to add additional columns to the select clause and further filter the result set. The window function can only appear in the select list and the outermost position of a clause. It takes effect at the end of the query, that is, after the `join`, `where`, and `group by` operations are performed. The window function is often used to analyze trends, calculate outliers, and perform bucketing analyses on large-scale data.

## Usage

### Syntax

```SQL
function(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### PARTITION BY clause

The Partition By clause is similar to Group By. It groups the input rows by one or more specified columns. Rows with the same value are grouped together.

### ORDER BY clause

The `Order By` clause is basically the same as the outer `Order By`. It defines the order of the input rows. If `Partition By` is specified, `Order By` defines the order within each Partition grouping. The only difference is that `Order By n` (n is a positive integer) in the `OVER` clause is equivalent to no operation, whereas `n` in the outer `Order By` indicates sorting by the nth column.

Example:

This example shows adding an id column to the select list with values of 1, 2, 3, etc., sorted by the `date_and_time` column in the events table.

```SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
```

### Window clause

The window clause is used to specify a range of rows for operations (the preceding and following rows based on the current row). It supports the following syntaxes: AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE(), and SUM(). For MAX() and MIN(), the window clause can specify the start to `UNBOUNDED PRECEDING`.

**Syntax:**

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
RANGE BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

:::note
**ARRAY_AGG() window frame limitation:** When using ARRAY_AGG() as a window function, only RANGE frames are supported. ROWS frames are NOT supported. For example:

```SQL
-- Supported: RANGE frame
array_agg(col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- NOT Supported: ROWS frame (will cause error)
array_agg(col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```
:::

## Window function sample table

This section creates a sample table `scores`. You can use this table to test many window functions below.

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

## Function examples

This section describes the window functions supported in StarRocks.

### AVG()

Calculates the average value of a field in a given window. This function ignores NULL values.

**Syntax:**

```SQL
AVG([DISTINCT] expr) [OVER (*analytic_clause*)]
```

`DISTINCT` is supported from StarRocks v4.0. When specified, AVG() calculates the average of only distinct values in the window.

:::note
**Window frame limitation:** When using AVG(DISTINCT) as a window function, only RANGE frames are supported. ROWS frames are NOT supported.
:::

**Examples:**

The following example uses stock data as an example.

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

Calculate the average closing price in the current row and each row before and after it.

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

Output:

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

For example, `12.87500000` in the first row is the average value of closing prices on "2014-10-02" (`12.86`), its previous day "2014-10-01" (null), and its following day "2014-10-03" (`12.89`).

**Example 2: Using AVG(DISTINCT) over overall window**

Calculate the average of distinct scores across all rows:

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER () AS distinct_avg
FROM test_scores;
```

Output:

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

The distinct average is 85.00 ((80 + 85 + 90) / 3).

**Example 3: Using AVG(DISTINCT) over framed window with RANGE frame**

Calculate the average of distinct scores within each subject partition using a RANGE frame:

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_avg
FROM test_scores;
```

Output:

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

For each row, the function calculates the average of distinct scores from the beginning of the partition up to and including the current row's score value.

### ARRAY_AGG()

Aggregates values (including NULL values) in a window into an array. You can use the optional `ORDER BY` clause to sort the elements within the array.

This function is supported from v3.4.

:::tip
**Important limitation:** ARRAY_AGG() as a window function **only supports RANGE window frames**. ROWS window frames are NOT supported. If no window frame is specified, the default `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used.
:::

**Syntax:**

```SQL
ARRAY_AGG([DISTINCT] expr [ORDER BY expr [ASC | DESC]]) OVER([partition_by_clause] [order_by_clause] [window_clause])
```

**Parameters:**

- `expr`: The expression to aggregate. It can be a column of any supported data type.
- `DISTINCT`: Optional. Eliminates duplicate values from the result array.
- `ORDER BY`: Optional. Specifies the order of elements within the array.

**Return value:**

Returns an ARRAY containing all values in the window.

**Usage notes:**

- **ROWS frames are NOT supported.** Only RANGE frames can be used with ARRAY_AGG() as a window function. Using ROWS frames will result in an error.
- NULL values are included in the result array.
- When `DISTINCT` is specified, duplicate values are removed from the array.
- When `ORDER BY` is specified within ARRAY_AGG(), the elements in the resulting array are sorted accordingly.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

**Example 1: Basic ARRAY_AGG() over window**

Collect all scores within each subject partition:

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

Output:

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

Note: Rows with the same `score` value (tom and amy both have 80) receive the same array due to the RANGE frame semantics.

**Example 2: ARRAY_AGG(DISTINCT) over window**

Collect distinct scores within each subject partition:

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

Output:

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

**Example 3: ARRAY_AGG() with ORDER BY**

Collect scores sorted in descending order within the array:

```SQL
SELECT *,
    array_agg(score ORDER BY score DESC)
        OVER (
            PARTITION BY subject
        ) AS scores_desc
FROM scores
WHERE subject = 'math';
```

Output:

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

**Example 4: ARRAY_AGG() with RANGE frame**

Collect all scores in the entire partition using RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:

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

Output:

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

**Example 5: Collect names partitioned by score ranges**

Using the `stock_ticker` table, collect stock symbols in a moving window:

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

Output:

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

**Example 6: Invalid usage - ROWS frame (will cause error)**

The following query will fail because ROWS frames are not supported:

```SQL
-- This will cause an error!
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  -- NOT SUPPORTED!
        ) AS score_array
FROM scores;
```

Error message:

```plaintext
ERROR: array_agg as window function does not support ROWS frame type. Please use RANGE frame instead.
```

### COUNT()

Calculates the total number of rows that meet the specified conditions in a give window.

**Syntax:**

```SQL
COUNT([DISTINCT] expr) [OVER (analytic_clause)]
```

`DISTINCT` is supported from StarRocks v4.0. When specified, COUNT() counts only distinct values in the window.

:::note
**Window frame limitation:** When using COUNT(DISTINCT) as a window function, only RANGE frames are supported. ROWS frames are NOT supported. For example:

```SQL
-- Supported: RANGE frame
count(distinct col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- NOT Supported: ROWS frame (will cause error)
count(distinct col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```
:::

**Examples:**

Count the occurrence of math scores that are greater than 90 from the current row to the first row in the math partition. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

**Example 2: Using COUNT(DISTINCT) over overall window**

Count distinct scores across all rows:

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

Output:

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

The distinct count is 4 (values: 80, 85, 90, and NULL if any).

**Example 3: Using COUNT(DISTINCT) over framed window with RANGE frame**

Count distinct scores within each subject partition using a RANGE frame:

```SQL
SELECT id, subject, score,
    COUNT(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_count
FROM test_scores;
```

Output:

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

For each row, the function counts distinct scores from the beginning of the partition up to and including the current row's score value.

### CUME_DIST()

The CUME_DIST() function calculates the cumulative distribution of a value within a partition or window, indicating its relative position as a percentage in the partition. It is often used to calculate the distribution of highest or lowest values in a group.

- If data is sorted in ascending order, this function calculates the percentage of values less than or equal to the value in the current row.
- If data is sorted in descending order, this function calculates the percentage of values greater than or equal to the value in the current row.

The cumulative distribution is in the range of 0 to 1. It is useful for percentile calculation and data distribution analysis.

This function is supported from v3.2.

**Syntax:**

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

- `partition_by_clause`: optional. If this clause is not specified, the entire result set is processed as a single partition.
- `order_by_clause`: **This function must be used with ORDER BY to sort partition rows into the desired order.**

CUME_DIST() contains NULL values and treats them as the lowest values.

**Examples:**

The following example shows the cumulative distribution of each score within each `subject` group. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

- For `cume_dist` in the first row, the `NULL` group has only one row, and only this row itself meets the condition of "less than or equal to the current row". The cumulative distribution is 1。
- For `cume_dist` in the second row, the `english` group has five rows, and only this row itself (NULL) meets the condition of "less than or equal to the current row". The cumulative distribution is 0.2.
- For `cume_dist` in the third row, the `english` group has five rows, and two rows (85 and NULL) meet the condition of "less than or equal to the current row". The cumulative distribution is 0.4.

### DENSE_RANK()

The DENSE_RANK() function is used to represent rankings. Unlike RANK(), DENSE_RANK() **does not have vacant** numbers. For example, if there are two 1s, the third number of DENSE_RANK() is still 2, whereas the third number of RANK() is 3.

**Syntax:**

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

**Examples:**

The following example shows the ranking of math scores (sorted in descending order). This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

The result data has two rows whose score is 80. They all rank 3. The rank for the next score 70 is 4. This shows DENSE_RANK() **does not have vacant** numbers.

### FIRST_VALUE()

FIRST_VALUE() returns the **first** value of the window range.

**Syntax:**

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` is supported from v2.5.0. It is used to determine whether NULL values of `expr` are eliminated from the calculation. By default, NULL values are included, which means NULL is returned if the first value in the filtered result is NULL. If you specify IGNORE NULLS, the first non-null value in the filtered result is returned. If all the values are NULL, NULL is returned even if you specify IGNORE NULLS.

ARRAY types are supported from StarRocks v3.5. You can use FIRST_VALUE() with ARRAY columns to get the first array value in the window.

**Examples:**

Return the first `score` value for each member in each group (descending order), grouping by `subject`. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

**Example 2: Using FIRST_VALUE() with ARRAY types**

Create a table with ARRAY columns:

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

Query data using FIRST_VALUE() with ARRAY types:

```SQL
SELECT col_1, arr1, 
    FIRST_VALUE(arr1) OVER (ORDER BY col_1) AS first_array
FROM test_array_value;
```

Output:

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

The first array value `[1,11]` is returned for all rows in the window.

### LAST_VALUE()

LAST_VALUE() returns the **last** value of the window range. It is the opposite of FIRST_VALUE().

**Syntax:**

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` is supported from v2.5.0. It is used to determine whether NULL values of `expr` are eliminated from the calculation. By default, NULL values are included, which means NULL is returned if the last value in the filtered result is NULL. If you specify IGNORE NULLS, the last non-null value in the filtered result is returned. If all the values are NULL, NULL is returned even if you specify IGNORE NULLS.

By default, LAST_VALUE() calculates `rows between unbounded preceding and current row`, which compares the current row with all its preceding rows. If you want to show only one value for each partition, use `rows between unbounded preceding and unbounded following` after ORDER BY.

ARRAY types are supported from StarRocks v3.5. You can use LAST_VALUE() with ARRAY columns to get the last array value in the window.

**Examples:**

Returns the last `score` for each member in the group (descending order), grouping by `subject`. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

**Example 2: Using LAST_VALUE() with ARRAY types**

Use the same table from FIRST_VALUE() Example 2:

```SQL
SELECT col_1, arr1, 
    LAST_VALUE(arr1) OVER (
        ORDER BY col_1 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_array
FROM test_array_value;
```

Output:

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

The last array value `[5,55]` is returned for all rows in the window.

### LAG()

Returns the value of the row that lags the current row by `offset` rows. This function is often used to compare values between rows and filter data.

`LAG()` can be used to query data of the following types:

- Numeric: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
- String: CHAR, VARCHAR
- Date: DATE, DATETIME
- BITMAP and HLL are supported from StarRocks v2.5.
- ARRAY types are supported from StarRocks v3.5.

**Syntax:**

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**Parameters:**

- `expr`: the field you want to compute.
- `offset`: the offset. It must be a **positive integer**. If this parameter is not specified, 1 is the default.
- `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`, since version 4.0, default no longer has to be a constant—it can be a column name.
- `IGNORE NULLS` is supported from v3.0. It is used to determine whether NULL values of `expr` are included in the result. By default, NULL values are included when `offset` rows are counted, which means NULL is returned if the value of the destination row is NULL. See Example 1. If you specify IGNORE NULLS, NULL values are ignored when `offset` rows are counted and the system continues to search for `offset` non-null values. If `offset` non-null values cannot be found, NULL or `default` (if specified) is returned. See Example 2.

**Example 1: IGNORE NULLS is not specified**

Create a table and insert values:

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

Query data from this table, where `offset` is 2, which means traversing the previous two rows; `default` is 0, which means 0 is returned if no matching rows are found.

Output:

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

For the first two rows, no previous two rows exist and the default value 0 is returned.

For NULL in row 3, the value two rows backward is NULL and NULL is returned because NULL values are allowed.

**Example 2: IGNORE NULLS is specified**

Use the preceding table and parameter settings.

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

For rows 1 to 4, the system cannot find two non-NULL values for each of them in the previous rows and the default value 0 is returned.

For value 6 in row 7, the value two rows backward is NULL and NULL is ignored because IGNORE NULLS is specified. The system continues to search for non-null values and 2 in row 4 is returned.

**Example 3: Setting the default value in LAG() to a column name**

Use the preceding table and parameter settings.

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

As you can see, for rows 1 and 2 there are not two non-NULL values when scanning backward, so the default returned is the current row’s col_1 value.

All other rows behave the same as in Example 1.

**Example 4: Using LAG() with ARRAY types**

Create a table with ARRAY columns:

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

Query data using LAG() with ARRAY types:

```SQL
SELECT col_1, arr1, LAG(arr1, 2, arr2) OVER (ORDER BY col_1) AS lag_result 
FROM test_array_value;
```

Output:

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

For the first two rows, no previous two rows exist, so the default value from `arr2` is returned.

### LEAD()

Returns the value of the row that leads the current row by `offset` rows. This function is often used to compare values between rows and filter data.

Data types that can be queried by `LEAD()` are the same as those supported by [LAG()](#lag).

**Syntax:**

```sql
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**Parameters:**

- `expr`: the field you want to compute.
- `offset`: the offset. It must be a positive integer. If this parameter is not specified, 1 is the default.
- `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`, since version 4.0, default no longer has to be a constant—it can be a column name.
- `IGNORE NULLS` is supported from v3.0. It is used to determine whether NULL values of `expr` are included in the result. By default, NULL values are included when `offset` rows are counted, which means NULL is returned if the value of the destination row is NULL. See Example 1. If you specify IGNORE NULLS, NULL values are ignored when `offset` rows are counted and the system continues to search for `offset` non-null values. If `offset` non-null values cannot be found, NULL or `default` (if specified) is returned. See Example 2.

**Example 1: IGNORE NULLS is not specified**

Create a table and insert values:

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

Query data from this table, where `offset` is 2, which means traversing the subsequent two rows; `default` is 0, which means 0 is returned if no matching rows are found.

Output:

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

For the first row, the value two rows forward is NULL and NULL is returned because NULL values are allowed.

For the last two rows, no subsequent two rows exist and the default value 0 is returned.

**Example 2: IGNORE NULLS is specified**

Use the preceding table and parameter settings.

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

For rows 7 to 10, the system cannot find two non-null values in the subsequent rows and the default value 0 is returned.

For the first row, the value two rows forward is NULL and NULL is ignored because IGNORE NULLS is specified. The system continues to search for the second non-null value and 2 in row 4 is returned.

**Example 3: Setting the default value in LEAD() to a column name**

Use the preceding table and parameter settings.

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

As you can see, for rows 9 and 10 there are not two non-NULL values when scanning forward, so the default returned is the current row’s col_1 value.

All other rows behave the same as in Example 1.

**Example 4: Using LEAD() with ARRAY types**

Use the same table from LAG() Example 4:

```SQL
SELECT col_1, arr1, LEAD(arr1, 2, arr2) OVER (ORDER BY col_1) AS lead_result 
FROM test_array_value;
```

Output:

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

For the last two rows, no subsequent two rows exist, so the default value from `arr2` is returned.

### MAX()

Returns the maximum value of the specified rows in the current window.

**Syntax:**

```SQL
MAX(expr) [OVER (analytic_clause)]
```

**Examples:**

Calculate the maximum value of rows from the first row to the row after the current row. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

The following example calculates the maximum score among all rows for the `math` subject.

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

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

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

Returns the minimum value of the specified rows in the current window.

**Syntax:**

```SQL
MIN(expr) [OVER (analytic_clause)]
```

**Examples:**

Calculate the lowest score among all rows for the math subject. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

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

NTILE() function divides the sorted rows in a partition by the specified number of `num_buckets` as equally as possible, stores the divided rows in the respective buckets, starting from 1 `[1, 2, ..., num_buckets]`, and returns the bucket number that each row is in.

About the size of the bucket:

- If the row counts can be divided by the specified number of `num_buckets` exactly, all the buckets will be of the same size.
- If the row counts cannot be divided by the specified number of `num_buckets` exactly, there will be buckets of two different sizes. The difference between sizes is 1. The buckets with more rows will be listed ahead of the one with fewer rows.

**Syntax:**

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`: Number of the buckets to be created. The value must be a constant positive integer whose maximum is `2^63 - 1`.

Window clause is not allowed in NTILE() function.

NTILE() function returns BIGINT type of data.

**Examples:**

The following example divides all rows in the partition into two buckets. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

```sql
select *,
    ntile(2)
        over (
            partition by subject
            order by score
        ) as bucket_id
from scores;
```

Output:

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

As the above example shown, when `num_buckets` is `2`:

- For the first row, this partition has only this record and it is assigned to only one bucket.
- For rows 2 to 7, the partition has 6 records and the first 3 records are assigned to bucket 1 and other 3 records are assigned to bucket 2.

### PERCENT_RANK()

Calculates the relative rank of a row within a result set as a percentage.

PERCENT_RANK() is calculated using the following formula, where `Rank` represents the rank of the current row in the partition.

```plaintext
(Rank - 1)/(Rows in partition - 1)
```

The return values range from 0 to 1. This function is useful for percentile calculation and analyzing data distribution. It is supported from v3.2.

**Syntax:**

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

**This function must be used with ORDER BY to sort partition rows into the desired order.**

**Examples:**

The following example shows the relative rank of each `score` within the group of `math`. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

The RANK() function is used to represent rankings. Unlike DENSE_RANK(), RANK() will **appear as a vacant** number. For example, if two tied 1s appear, the third number of RANK() will be 3 instead of 2.

**Syntax:**

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

**Examples:**

Ranking math scores in the group. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

The result data has two rows whose score is 80. They all rank 3. The rank for the next score 70 is 5.

### ROW_NUMBER()

Returns a continuously increasing integer starting from 1 for each row of a Partition. Unlike RANK() and DENSE_RANK(), the value returned by ROW_NUMBER() **does not repeat or have gaps** and is **continuously incremented**.

**Syntax:**

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

**Examples:**

Rank math scores in the group. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

The QUALIFY clause filters the results of window functions. In a SELECT statement, you can use the QUALIFY clause to apply conditions to a column to filter results. QUALIFY is analogous to the HAVING clause in aggregate functions. This function is supported from v2.5.

QUALIFY simplifies the writing of SELECT statements.

Before QUALIFY is used, a SELECT statement may go like this:

```SQL
SELECT *
FROM (SELECT DATE,
             PROVINCE_CODE,
             TOTAL_SCORE,
             ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) AS SCORE_ROWNUMBER
      FROM example_table) T1
WHERE T1.SCORE_ROWNUMBER = 1;
```

After QUALIFY is used, the statement is shortened to:

```SQL
SELECT DATE, PROVINCE_CODE, TOTAL_SCORE
FROM example_table 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) = 1;
```

QUALIFY supports only the following three window functions: ROW_NUMBER(), RANK(), and DENSE_RANK().

**Syntax:**

```SQL
SELECT <column_list>
FROM <data_source>
[GROUP BY ...]
[HAVING ...]
QUALIFY <window_function>
[ ... ]
```

**Parameters:**

`<column_list>`: columns from which you want to obtain data.

`<data_source>`: The data source is generally a table.

`<window_function>`: The `QUALIFY` clause can only be followed by a window function, including ROW_NUMBER(), RANK(), and DENSE_RANK().

**Examples:**

```SQL
-- Create a table.
CREATE TABLE sales_record (
   city_id INT,
   item STRING,
   sales INT
) DISTRIBUTED BY HASH(`city_id`);

-- Insert data into the table.
insert into sales_record values
(1,'fruit',95),
(2,'drinks',70),
(3,'fruit',87),
(4,'drinks',98);

-- Query data from the table.
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

Example 1: Obtain records whose row number is greater than 1 from the table.

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

Example 2: Obtain records whose row number is 1 from each partition of the table. The table is divided into two partitions by `item` and the first row in each partition is returned.

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

Example 3: Obtain records whose sales rank No.1 from each partition of the table. The table is divided into two partitions by `item` and the row with the highest sales in each partition is returned.

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

**Usage notes:**

- QUALIFY supports only the following three window functions: ROW_NUMBER(), RANK(), and DENSE_RANK().

- The execution order of clauses in a query with QUALIFY is evaluated in the following order:

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

Calculates the sum of specified rows.

**Syntax:**

```SQL
SUM([DISTINCT] expr) [OVER (analytic_clause)]
```

`DISTINCT` is supported from StarRocks v4.0. When specified, SUM() sums only distinct values in the window.

:::note
**Window frame limitation:** When using SUM(DISTINCT) as a window function, only RANGE frames are supported. ROWS frames are NOT supported.
:::

**Examples:**

Group data by `subject` and calculate the sum of scores of all rows within the group. This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

**Example 2: Using SUM(DISTINCT) over overall window**

Sum distinct scores across all rows:

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER () AS distinct_sum
FROM test_scores;
```

Output:

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

The distinct sum is 255 (80 + 85 + 90).

**Example 3: Using SUM(DISTINCT) over framed window with RANGE frame**

Sum distinct scores within each subject partition using a RANGE frame:

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_sum
FROM test_scores;
```

Output:

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

For each row, the function sums distinct scores from the beginning of the partition up to and including the current row's score value.

### VARIANCE, VAR_POP, VARIANCE_POP

Returns the population variance of an expression. VAR_POP and VARIANCE_POP are aliases of VARIANCE. These functions can be used as window functions since v2.5.10.

**Syntax:**

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the sample variance of an expression. These functions can be used as window functions since v2.5.10.

**Syntax:**

```sql
VAR_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the standard deviation of an expression. These functions can be used as window functions since v2.5.10.

**Syntax:**

```sql
STD(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the sample standard deviation of an expression. This function can be used as a window function since v2.5.10.

**Syntax:**

```sql
STDDEV_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the sample covariance of two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
COVAR_SAMP(expr1,expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the population covariance of two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
COVAR_POP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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

Returns the Pearson correlation coefficient between two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
:::

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

This example uses the data in the [Sample table](#window-function-sample-table) `scores`.

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
