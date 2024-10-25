---
displayed_sidebar: docs
keywords: ['analytic']
sidebar_position: 0.9
---

# Window functions

- [Window functions](#window-functions)
  - [Background](#background)
  - [Usage](#usage)
    - [Syntax](#syntax)
    - [PARTITION BY clause](#partition-by-clause)
    - [ORDER BY clause](#order-by-clause)
    - [Window clause](#window-clause)
  - [Window function sample table](#window-function-sample-table)
  - [Function examples](#function-examples)
    - [AVG()](#avg)
    - [COUNT()](#count)
    - [CUME\_DIST()](#cume_dist)
    - [DENSE\_RANK()](#dense_rank)
    - [FIRST\_VALUE()](#first_value)
    - [LAST\_VALUE()](#last_value)
    - [LAG()](#lag)
    - [LEAD()](#lead)
    - [MAX()](#max)
    - [MIN()](#min)
    - [NTILE()](#ntile)
    - [PERCENT\_RANK()](#percent_rank)
    - [RANK()](#rank)
    - [ROW\_NUMBER()](#row_number)
    - [QUALIFY()](#qualify)
    - [SUM()](#sum)
    - [VARIANCE, VAR\_POP, VARIANCE\_POP](#variance-var_pop-variance_pop)
    - [VAR\_SAMP, VARIANCE\_SAMP](#var_samp-variance_samp)
    - [STD, STDDEV, STDDEV\_POP](#std-stddev-stddev_pop)
    - [STDDEV\_SAMP](#stddev_samp)
    - [COVAR\_SAMP](#covar_samp)
    - [COVAR\_POP](#covar_pop)
    - [CORR](#corr)

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
```

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
AVG(expr) [OVER (*analytic_clause*)]
```

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

### COUNT()

Calculates the total number of rows that meet the specified conditions in a give window.

**Syntax:**

```SQL
COUNT(expr) [OVER (analytic_clause)]
```

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

### LAST_VALUE()

LAST_VALUE() returns the **last** value of the window range. It is the opposite of FIRST_VALUE().

**Syntax:**

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` is supported from v2.5.0. It is used to determine whether NULL values of `expr` are eliminated from the calculation. By default, NULL values are included, which means NULL is returned if the last value in the filtered result is NULL. If you specify IGNORE NULLS, the last non-null value in the filtered result is returned. If all the values are NULL, NULL is returned even if you specify IGNORE NULLS.

By default, LAST_VALUE() calculates `rows between unbounded preceding and current row`, which compares the current row with all its preceding rows. If you want to show only one value for each partition, use `rows between unbounded preceding and unbounded following` after ORDER BY.

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

### LAG()

Returns the value of the row that lags the current row by `offset` rows. This function is often used to compare values between rows and filter data.

`LAG()` can be used to query data of the following types:

- Numeric: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
- String: CHAR, VARCHAR
- Date: DATE, DATETIME
- BITMAP and HLL are supported from StarRocks v2.5.

**Syntax:**

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**Parameters:**

- `expr`: the field you want to compute.
- `offset`: the offset. It must be a **positive integer**. If this parameter is not specified, 1 is the default.
- `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`.
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
- `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`.
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
SUM(expr) [OVER (analytic_clause)]
```

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
