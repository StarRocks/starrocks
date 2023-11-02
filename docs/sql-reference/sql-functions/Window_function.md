# Window function

## Background

The window function is a special class of built-in functions. Similar to the aggregation function, it also does calculations on multiple input rows to get a single data value. The difference is that the window function processes the input data within a specific window, rather than using the "group by" method. The data in each window can be sorted and grouped using the over() clause. The window function **computes a separate value for each row**, rather than computing one value for each group. This flexibility allows users to add additional columns to the select clause and further filter the result set. The window function can only appear in the select list and the outermost position of a clause. It takes effect at the end of the query, that is, after the `join`, `where`, and `group by` operations are performed. The window function is often used to analyze trends, calculate outliers, and perform bucketing analyses on large-scale data.

## Usage

Syntax of the window function:

```SQL
function(args) OVER(partition_by_clause order_by_clause [window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### Functions

Currently supported functions include:

* MIN(), MAX(), COUNT(), SUM(), AVG()
* FIRST_VALUE(), LAST_VALUE(), LEAD(), LAG()
* ROW_NUMBER(), RANK(), DENSE_RANK()
* CUME_DIST(), PERCENT_RANK(), QUALIFY()
* NTILE()
* VARIANCE(), VAR_SAMP(), STD(), STDDEV_SAMP(), COVAR_SAMP(), COVAR_POP(), CORR()

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

Syntax:

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

Example:

Suppose we have the following stock data, the stock symbol is JDR, and the closing price is the daily closing price.

```SQL
create table stock_ticker (
    stock_symbol string,
    closing_price decimal(8,2),
    closing_date timestamp);

-- ...load some data...

select *
from stock_ticker
order by stock_symbol, closing_date
```

The raw data was shown as follows:

```plaintext
+--------------+---------------+---------------------+
| stock_symbol | closing_price | closing_date        |
+--------------+---------------+---------------------+
| JDR          | 12.86         | 2014-10-02 00:00:00 |
| JDR          | 12.89         | 2014-10-03 00:00:00 |
| JDR          | 12.94         | 2014-10-04 00:00:00 |
| JDR          | 12.55         | 2014-10-05 00:00:00 |
| JDR          | 14.03         | 2014-10-06 00:00:00 |
| JDR          | 14.75         | 2014-10-07 00:00:00 |
| JDR          | 13.98         | 2014-10-08 00:00:00 |
+--------------+---------------+---------------------+
```

This query uses the window function to generate the moving_average column whose value is the 3-day (previous day, current day, and next day) average stock price. The first day does not have the value of its previous day, and the last day does not have the value of the day after, so these two rows only calculate the average value of two days. Here `Partition By` does not take effect, because all the data is JDR data. However, if there is other stock information, `Partition By` will ensure that the window function is operated within each Partition.

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

The following data is obtained:

```plaintext
+--------------+---------------------+---------------+----------------+
| stock_symbol | closing_date        | closing_price | moving_average |
+--------------+---------------------+---------------+----------------+
| JDR          | 2014-10-02 00:00:00 | 12.86         | 12.87          |
| JDR          | 2014-10-03 00:00:00 | 12.89         | 12.89          |
| JDR          | 2014-10-04 00:00:00 | 12.94         | 12.79          |
| JDR          | 2014-10-05 00:00:00 | 12.55         | 13.17          |
| JDR          | 2014-10-06 00:00:00 | 14.03         | 13.77          |
| JDR          | 2014-10-07 00:00:00 | 14.75         | 14.25          |
| JDR          | 2014-10-08 00:00:00 | 13.98         | 14.36          |
+--------------+---------------------+---------------+----------------+
```

## Function Examples

This section describes the window functions supported in StarRocks.

### AVG()

Syntax:

```SQL
AVG(expr) [OVER (*analytic_clause*)]
```

Example:

Calculate the x-average of the current row and each row before and after it.

```SQL
select x, property,
    avg(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving average'
from int_t
where property in ('odd','even');
```

```plaintext
+----+----------+----------------+
| x  | property | moving average |
+----+----------+----------------+
| 2  | even     | 3              |
| 4  | even     | 4              |
| 6  | even     | 6              |
| 8  | even     | 8              |
| 10 | even     | 9              |
| 1  | odd      | 2              |
| 3  | odd      | 3              |
| 5  | odd      | 5              |
| 7  | odd      | 7              |
| 9  | odd      | 8              |
+----+----------+----------------+
```

### COUNT()

Syntax:

```SQL
COUNT(expr) [OVER (analytic_clause)]
```

Example:

Count the occurrence of x from the current row to the first row.

```SQL
select x, property,
    count(x)
        over (
            partition by property
            order by x
            rows between unbounded preceding and current row
        ) as 'cumulative total'
from int_t where property in ('odd','even');
```

```plaintext
+----+----------+------------------+
| x  | property | cumulative count |
+----+----------+------------------+
| 2  | even     | 1                |
| 4  | even     | 2                |
| 6  | even     | 3                |
| 8  | even     | 4                |
| 10 | even     | 5                |
| 1  | odd      | 1                |
| 3  | odd      | 2                |
| 5  | odd      | 3                |
| 7  | odd      | 4                |
| 9  | odd      | 5                |
+----+----------+------------------+
```

### CUME_DIST()

The CUME_DIST() function calculates the cumulative distribution of a value within a partition, indicating its relative position as a percentage of values less than or equal to the value in the current row. With a range of 0 to 1, it's useful for percentile calculations and data distribution analysis.

Syntax:

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

**This function should be used with ORDER BY to sort partition rows into the desired order. Without ORDER BY, all rows are peers and have value N/N = 1, where N is the partition size.**

CUME_DIST() contains NULL values and treats them as the lowest values.

The following example shows the cumulative distribution of column y within each group of column x.

```SQL
SELECT x, y,
    CUME_DIST()
        OVER (
            PARTITION BY x
            ORDER BY y
        ) AS `cume_dist`
FROM int_t;
```

```plaintext
+---+---+--------------------+
| x | y | cume_dist          |
+---+---+--------------------+
| 1 | 1 | 0.3333333333333333 |
| 1 | 2 |                  1 |
| 1 | 2 |                  1 |
| 2 | 1 | 0.3333333333333333 |
| 2 | 2 | 0.6666666666666667 |
| 2 | 3 |                  1 |
| 3 | 1 | 0.6666666666666667 |
| 3 | 1 | 0.6666666666666667 |
| 3 | 2 |                  1 |
+---+---+--------------------+
```

### DENSE_RANK()

The DENSE_RANK() function is used to represent rankings. Unlike RANK(), DENSE_RANK()**does not have vacant** numbers. For example, if there are two tied 1s, the third number of DENSE_RANK() is still 2, whereas the third number of RANK() is 3.

Syntax:

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

The following example shows the ranking of column x according to the property column grouping.

```SQL
select x, y,
    dense_rank()
        over (
            partition by x
            order by y
        ) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 2    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 1    |
| 3 | 2 | 2    |
+---+---+------+
```

### NTILE()

NTILE() function divides the sorted rows in a partition by the specified number of `num_buckets` as equally as possible, stores the divided rows in the respective buckets, starting from 1 `[1, 2, ..., num_buckets]`, and returns the bucket number that each row is in.

About the size of the bucket:

* If the row counts can be divided by the specified number of `num_buckets` exactly, all the buckets will be of the same size.
* If the row counts cannot be divided by the specified number of `num_buckets` exactly, there will be buckets of two different sizes. The difference between sizes is 1. The buckets with more rows will be listed ahead of the one with fewer rows.

Syntax:

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`: Number of the buckets to be created. The value must be a constant positive integer whose maximum is `2^63 - 1`.

Window clause is not allowed in NTILE() function

NTILE() function returns BIGINT type of data.

Example:

The following example divides all rows in the partition into 2 buckets.

```sql
select id, x, y,
    ntile(2)
        over (
            partition by x
            order by y
        ) as bucket_id
from t1;
```

```plaintext
+------+------+------+-----------+
| id   | x    | y    | bucket_id |
+------+------+------+-----------+
|    1 |    1 |   11 |         1 |
|    2 |    1 |   11 |         1 |
|    3 |    1 |   22 |         1 |
|    4 |    1 |   33 |         2 |
|    5 |    1 |   44 |         2 |
|    6 |    1 |   55 |         2 |
|    7 |    2 |   66 |         1 |
|    8 |    2 |   77 |         1 |
|    9 |    2 |   88 |         2 |
|   10 |    3 |   99 |         1 |
+------+------+------+-----------+
```

As the above example shown, when `num_buckets` is `2`:

* Rows of No.1 to No.6 were classified into the first partition; rows of No.1 to No.3 were stored in the first bucket, and rows of No.4 to No.6 were stored in the second one.
* Rows of No.7 to No.9 were classified into the second partition; rows of No.7 and No.8 were stored in the first bucket, and row No.9 was stored in the second one.
* Row No.10 was classified into the third partition and stored in the first bucket.

<br/>

### FIRST_VALUE()

FIRST_VALUE() returns the **first** value of the window range.

Syntax:

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` is supported from v2.5.0. It is used to determine whether NULL values of `expr` are eliminated from the calculation. By default, NULL values are included, which means NULL is returned if the first value in the filtered result is NULL. If you specify IGNORE NULLS, the first non-null value in the filtered result is returned. If all the values are NULL, NULL is returned even if you specify IGNORE NULLS.

Example:

We have the following data:

```SQL
 select name, country, greeting
 from mail_merge;
 ```

```plaintext
+---------+---------+--------------+
| name    | country | greeting     |
+---------+---------+--------------+
| Pete    | USA     | Hello        |
| John    | USA     | Hi           |
| Boris   | Germany | Guten tag    |
| Michael | Germany | Guten morgen |
| Bjorn   | Sweden  | Hej          |
| Mats    | Sweden  | Tja          |
+---------+---------+--------------+
```

Use FIRST_VALUE() to return the first greeting value in each grouping, based on the country grouping.

```SQL
select country, name,
    first_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
```

```plaintext
+---------+---------+-----------+
| country | name    | greeting  |
+---------+---------+-----------+
| Germany | Boris   | Guten tag |
| Germany | Michael | Guten tag |
| Sweden  | Bjorn   | Hej       |
| Sweden  | Mats    | Hej       |
| USA     | John    | Hi        |
| USA     | Pete    | Hi        |
+---------+---------+-----------+
```

### LAG()

Returns the value of the row that lags the current row by `offset` rows. This function is often used to compare values between rows and filter data.

`LAG()` can be used to query data of the following types:

* Numeric: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
* String: CHAR, VARCHAR
* Date: DATE, DATETIME
* BITMAP and HLL are supported from StarRocks v2.5.

Syntax:

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

Parameters:

* `expr`: the field you want to compute.
* `offset`: the offset. It must be a **positive integer**. If this parameter is not specified, 1 is the default.
* `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`.
* `IGNORE NULLS` is supported from v3.0. It is used to determine whether NULL values of `expr` are included in the result. By default, NULL values are included when `offset` rows are counted, which means NULL is returned if the value of the destination row is NULL. See Example 1. If you specify IGNORE NULLS, NULL values are ignored when `offset` rows are counted and the system continues to search for `offset` non-null values. If `offset` non-null values cannot be found, NULL or `default` (if specified) is returned. See Example 2.

Example 1: IGNORE NULLS is not specified

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

Example 2: IGNORE NULLS is specified

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

### LAST_VALUE()

LAST_VALUE() returns the **last** value of the window range. It is the opposite of FIRST_VALUE().

Syntax:

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS` is supported from v2.5.0. It is used to determine whether NULL values of `expr` are eliminated from the calculation. By default, NULL values are included, which means NULL is returned if the last value in the filtered result is NULL. If you specify IGNORE NULLS, the last non-null value in the filtered result is returned. If all the values are NULL, NULL is returned even if you specify IGNORE NULLS.

Use the data from the example:

```SQL
select country, name,
    last_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
```

```plaintext
+---------+---------+--------------+
| country | name    | greeting     |
+---------+---------+--------------+
| Germany | Boris   | Guten morgen |
| Germany | Michael | Guten morgen |
| Sweden  | Bjorn   | Tja          |
| Sweden  | Mats    | Tja          |
| USA     | John    | Hello        |
| USA     | Pete    | Hello        |
+---------+---------+--------------+
```

### LEAD()

Returns the value of the row that leads the current row by `offset` rows. This function is often used to compare values between rows and filter data.

Data types that can be queried by `lead()` are the same as those supported by [lag()](#lag).

Syntax

```sql
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

Parameters:

* `expr`: the field you want to compute.
* `offset`: the offset. It must be a positive integer. If this parameter is not specified, 1 is the default.
* `default`: the default value returned if no matching row is found. If this parameter is not specified, NULL is the default. `default` supports any expression whose type is compatible with `expr`.
* `IGNORE NULLS` is supported from v3.0. It is used to determine whether NULL values of `expr` are included in the result. By default, NULL values are included when `offset` rows are counted, which means NULL is returned if the value of the destination row is NULL. See Example 1. If you specify IGNORE NULLS, NULL values are ignored when `offset` rows are counted and the system continues to search for `offset` non-null values. If `offset` non-null values cannot be found, NULL or `default` (if specified) is returned. See Example 2.

Example 1: IGNORE NULLS is not specified

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

Example 2: IGNORE NULLS is specified

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

Syntax

```SQL
MAX(expr) [OVER (analytic_clause)]
```

Example:

Calculate the maximum value of rows from the first row to the row after the current row.

```SQL
select x, property,
    max(x)
        over (
            order by property, x
            rows between unbounded preceding and 1 following
        ) as 'local maximum'
from int_t
where property in ('prime','square');
```

```plaintext
+---+----------+---------------+
| x | property | local maximum |
+---+----------+---------------+
| 2 | prime    | 3             |
| 3 | prime    | 5             |
| 5 | prime    | 7             |
| 7 | prime    | 7             |
| 1 | square   | 7             |
| 4 | square   | 9             |
| 9 | square   | 9             |
+---+----------+---------------+
```

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

```sql
select x, property,
    max(x)
        over (
            order by property, x
            rows between 3 preceding and 2 following) as 'local maximum'
from int_t
where property in ('prime','square');
```

### MIN()

Returns the minimum value of the specified rows in the current window.

Syntax:

```SQL
MIN(expr) [OVER (analytic_clause)]
```

Example:

Calculate the minimum value of rows from the first row to the row after the current row.

```SQL
select x, property,
    min(x)
        over (
            order by property, x desc
            rows between unbounded preceding and 1 following
        ) as 'local minimum'
from int_t
where property in ('prime','square');
```

```plaintext
+---+----------+---------------+
| x | property | local minimum |
+---+----------+---------------+
| 7 | prime    | 5             |
| 5 | prime    | 3             |
| 3 | prime    | 2             |
| 2 | prime    | 2             |
| 9 | square   | 2             |
| 4 | square   | 1             |
| 1 | square   | 1             |
+---+----------+---------------+
```

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

```sql
select x, property,
    min(x)
    over (
          order by property, x desc
          rows between 3 preceding and 2 following) as 'local minimum'
from int_t
where property in ('prime','square');
```

### PERCENT_RANK()

The PERCENT_RANK() function calculates the relative rank of a row within a result set as a percentage. It returns the percentage of partition values less than the value in the current row, excluding the highest value. The return values range from 0 to 1. This function is useful for percentile calculations and analyzing data distribution.

The PERCENT_RANK() function is calculated using the following formula, where rank represents the row rank and rows represents the number of partition rows:

```plaintext
(rank - 1) / (rows - 1)
```

Syntax:

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

**This function should be used with ORDER BY to sort partition rows into the desired order. Without ORDER BY, all rows are peers and have value (1 - 1)/(N - 1) = 0, where N is the partition size.**

The following example shows the relative rank of column y within each group of column x.

```SQL
SELECT x, y,
    PERCENT_RANK()
        OVER (
            PARTITION BY x
            ORDER BY y
        ) AS `percent_rank`
FROM int_t;
```

```plaintext
+---+---+--------------+
| x | y | percent_rank |
+---+---+--------------+
| 1 | 1 |            0 |
| 1 | 2 |          0.5 |
| 1 | 2 |          0.5 |
| 2 | 1 |            0 |
| 2 | 2 |          0.5 |
| 2 | 3 |            1 |
| 3 | 1 |            0 |
| 3 | 1 |            0 |
| 3 | 2 |            1 |
+---+---+--------------+
```

### RANK()

The RANK() function is used to represent rankings. Unlike DENSE_RANK(), RANK() will **appear as a vacant** number. For example, if two tied 1s appear, the third number of RANK() will be 3 instead of 2.

Syntax:

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

Example:

Ranking according to column x:

```SQL
select x, y, rank() over(partition by x order by y) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 2    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 1    |
| 3 | 2 | 3    |
+---+---+------+
```

### ROW_NUMBER()

Returns a continuously increasing integer starting from 1 for each row of a Partition. Unlike RANK() and DENSE_RANK(), the value returned by ROW_NUMBER() **does not repeat or have gaps** and is **continuously incremented**.

Syntax:

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

Example:

```SQL
select x, y, row_number() over(partition by x order by y) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 3    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 2    |
| 3 | 2 | 3    |
+---+---+------+
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

The execution order of clauses in a query with QUALIFY is evaluated in the following order:

> 1. From
> 2. Where
> 3. Group by
> 4. Having
> 5. Window
> 6. QUALIFY
> 7. Distinct
> 8. Order by
> 9. Limit

### SUM()

Syntax:

```SQL
SUM(expr) [OVER (analytic_clause)]
```

Example:

Group by property and calculate the sum of the **current, preceding, and following rows** within the group.

```SQL
select x, property,
    sum(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving total'
from int_t where property in ('odd','even');
```

```plaintext
+----+----------+--------------+
| x  | property | moving total |
+----+----------+--------------+
| 2  | even     | 6            |
| 4  | even     | 12           |
| 6  | even     | 18           |
| 8  | even     | 24           |
| 10 | even     | 18           |
| 1  | odd      | 4            |
| 3  | odd      | 9            |
| 5  | odd      | 15           |
| 7  | odd      | 21           |
+----+----------+--------------+
```

### VARIANCE, VAR_POP, VARIANCE_POP

Returns the population variance of an expression. VAR_POP and VARIANCE_POP are aliases of VARIANCE. These functions can be used as window functions since v2.5.10.

**Syntax:**

```SQL
VARIANCE(expr) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> VARIANCE() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the VARIANCE() function.

```plaintext
mysql> select variance(k) over (partition by no) FROM agg;
+-------------------------------------+
| variance(k) OVER (PARTITION BY no ) |
+-------------------------------------+
|                                   0 |
|                             54.6875 |
|                             54.6875 |
|                             54.6875 |
|                             54.6875 |
+-------------------------------------+
```

### VAR_SAMP, VARIANCE_SAMP

Returns the sample variance of an expression. These functions can be used as window functions since v2.5.10.

**Syntax:**

```sql
VAR_SAMP(expr) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> VAR_SAMP() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the VAR_SAMP() window function.

```plaintext
mysql> select VAR_SAMP(k) over (partition by no) FROM agg;
+-------------------------------------+
| var_samp(k) OVER (PARTITION BY no ) |
+-------------------------------------+
|                                   0 |
|                   72.91666666666667 |
|                   72.91666666666667 |
|                   72.91666666666667 |
|                   72.91666666666667 |
+-------------------------------------+
```

### STD, STDDEV, STDDEV_POP

Returns the standard deviation of an expression. These functions can be used as window functions since v2.5.10.

**Syntax:**

```sql
STD(expr) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> STD() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the STD() window function.

```plaintext
mysql> select STD(k) over (partition by no) FROM agg;
+--------------------------------+
| std(k) OVER (PARTITION BY no ) |
+--------------------------------+
|                              0 |
|               7.39509972887452 |
|               7.39509972887452 |
|               7.39509972887452 |
|               7.39509972887452 |
+--------------------------------+
```

### STDDEV_SAMP

Returns the sample standard deviation of an expression. This function can be used as a window function since v2.5.10.

**Syntax:**

```sql
STDDEV_SAMP(expr) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> STDDEV_SAMP() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the STDDEV_SAMP() window function.

```plaintext
mysql> select STDDEV_SAMP(k) over (partition by no) FROM agg;
+----------------------------------------+
| stddev_samp(k) OVER (PARTITION BY no ) |
+----------------------------------------+
|                                      0 |
|                      8.539125638299666 |
|                      8.539125638299666 |
|                      8.539125638299666 |
|                      8.539125638299666 |
+----------------------------------------+
```

### COVAR_SAMP

Returns the sample covariance of two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
COVAR_SAMP(expr1,expr2) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> COVAR_SAMP() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the COVAR_SAMP() window function.

```plaintext
mysql> select COVAR_SAMP(k, v) over (partition by no) FROM agg;
+------------------------------------------+
| covar_samp(k, v) OVER (PARTITION BY no ) |
+------------------------------------------+
|                                     NULL |
|                       119.99999999999999 |
|                       119.99999999999999 |
|                       119.99999999999999 |
|                       119.99999999999999 |
+------------------------------------------+
```

### COVAR_POP

Returns the population covariance of two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
COVAR_POP(expr1, expr2) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> COVAR_POP() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the COVAR_POP() window function.

```plaintext
mysql> select COVAR_POP(k, v) over (partition by no) FROM agg;
+-----------------------------------------+
| covar_pop(k, v) OVER (PARTITION BY no ) |
+-----------------------------------------+
|                                    NULL |
|                       79.99999999999999 |
|                       79.99999999999999 |
|                       79.99999999999999 |
|                       79.99999999999999 |
+-----------------------------------------+
```

### CORR

Returns the Pearson correlation coefficient between two expressions. This function is supported from v2.5.10. It is also an aggregate function.

**Syntax:**

```sql
CORR(expr1, expr2) [OVER (partition_by_clause)]
```

> **NOTE**
>
<<<<<<< HEAD
> CORR() only supports PARTITION BY. It does not support ORDER BY or Window clauses.
=======
> From 2.5.13, 3.0.7, 3.1.4 onwards, this window function supports the ORDER BY and Window clauses.
>>>>>>> 43bbc1e7a7 ([Doc] update version info for window func (#34195))

**Parameters:**

If `expr` is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

**Examples:**

Suppose table `agg` has the following data:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

Use the CORR() window function.

```plaintext
mysql> select CORR(k, v) over (partition by no) FROM agg;
+------------------------------------+
| corr(k, v) OVER (PARTITION BY no ) |
+------------------------------------+
|                               NULL |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
+------------------------------------+
```
