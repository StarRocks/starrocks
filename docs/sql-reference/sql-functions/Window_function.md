# Window function

## Background

The window function is a special class of built-in functions. Similar to the aggregation function, it also does calculations on multiple input rows to get a single data value. The difference is that the window function processes the input data within a specific window, rather than using the “group by” method. The data in each window can be sorted and grouped using the over() clause. The window function **computes a separate value for each row**, rather than computing one value for each group. This flexibility allows users to add additional columns to the select clause and further filter the result set. The window function can only appear in the select list and the outermost position of a clause. It takes effect at the end of the query, that is, after the `join`, `where`, and `group by` operations are performed. The window function is often used to analyze trends, calculate outliers, and perform bucketing analyses on large-scale data.

## Usage

Syntax of the window function：

~~~SQL
function(args) OVER(partition_by_clause order_by_clause [window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
~~~

### Function

Currently supported Functions include:

* MIN(), MAX(), COUNT(), SUM(), AVG()
* FIRST_VALUE(), LAST_VALUE(), LEAD(), LAG()
* ROW_NUMBER(), RANK(), DENSE_RANK()

### PARTITION BY clause

The Partition By clause is similar to Group By. It groups the input rows by one or more specified columns. Rows with the same value are grouped together.

### ORDER BY clause

The `Order By` clause is basically the same as the outer `Order By`. It defines the order of the input rows. If`Partition By` is specified, `Order By` defines the order within each Partition grouping. The only difference is that `Order By n` (n is a positive integer) in the `OVER` clause is equivalent to no operation, whereas the n in the outer `Order By` indicates sorting by the nth column.

Example 1:

This example shows adding an id column to the select list with values of 1, 2, 3, etc., sorted by the `date_and_time` column in the events table.

~~~SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
~~~

### Window Clause

The window clause is used to specify a range of rows for operations ( the previous and later lines based on the current line). It supports the following syntaxes – AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE() and SUM(). For MAX() and MIN(), the window clause can specify the start to `UNBOUNDED PRECEDING`.

Syntax:

~~~SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
~~~

Example 2:

Suppose we have the following stock data, the stock symbol is JDR, and the closing price is the daily closing price.

~~~SQL
create table stock_ticker (
    stock_symbol string,
    closing_price decimal(8,2),
    closing_date timestamp);

-- ...load some data...

select *
from stock_ticker
order by stock_symbol, closing_date
~~~

The raw data was shown as follows:

~~~Plain Text
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
~~~

This query uses the window function to generate the moving_average column whose value is the 3-day (previous day, current day, and next day) average stock price. The first day does not have the value of its previous day, and the last day does not have the value of the day after, so these two rows only calculate the average value of two days. Here `Partition By` does not take effect, because all the data is JDR data. However, if there is other stock information, `Partition By` will ensure that the window function is operated within each Partition.

~~~SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
~~~

The following data is obtained:

~~~Plain Text
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
~~~

## Function Examples

This section describes the window functions supported in StarRocks.

### AVG()

Syntax:

~~~SQL
AVG([DISTINCT | ALL] *expression*) [OVER (*analytic_clause*)]
~~~

Example 3:

Calculate the x-average of the current row and each row before and after it.

~~~SQL
select x, property,
    avg(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving average'
from int_t
where property in ('odd','even');
~~~

~~~Plain Text
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
~~~

### COUNT()

Syntax:

~~~SQL
COUNT([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

Example 4:

Count the occurrence of x from the current row to the first row.

~~~SQL
select x, property,
    count(x)
        over (
            partition by property
            order by x
            rows between unbounded preceding and current row
        ) as 'cumulative total'
from int_t where property in ('odd','even');
~~~

~~~Plain Text
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
~~~

### DENSE_RANK()

The DENSE_RANK() function is used to represent rankings. Unlike RANK(), DENSE_RANK()**does not have vacant** numbers. For example, if there are two tied 1s, the third number of DENSE_RANK() is still 2, whereas the third number of RANK() is 3.

Syntax:

~~~SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
~~~

The following example shows the ranking of column x according to the property column grouping.

~~~SQL
select x, y,
    dense_rank()
        over (
            partition by x
            order by y
        ) as rank
from int_t;
~~~

~~~Plain Text
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
~~~

### NTILE

NTILE() function divides the sorted rows in a partition by the specified number of `num_buckets` as equally as possible, stores the divided rows in the respective buckets, starting from 1 `[1, 2, ..., num_buckets]`, and returns the bucket number that each row is in.

About the size of the bucket:

* If the row counts can be divided by the specified number of `num_buckets` exactly, all the buckets will be of the same size.
* If the row counts cannot be divided by the specified number of `num_buckets` exactly, there will be buckets of two different sizes. The difference between sizes is 1. The buckets with more rows will be listed ahead of the one with fewer rows.

Syntax:

~~~~SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
~~~~

`num_buckets`: Number of the buckets to be created. The value must be a constant positive integer whose maximum is `2^63 - 1`.

Window clause is not allowed in NTILE() function

NTILE() function returns BIGINT type of data.

Example:

The following example divides all rows in the partition into 2 buckets.

~~~~sql
select id, x, y,
    ntile(2)
        over (
            partition by x
            order by y
        ) as bucket_id
from t1;
~~~~

~~~~Plain Text
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
~~~~

As the above example shown, when `num_buckets` is `2`:

* Rows of No.1 to No.6 were classified into the first partition; rows of No.1 to No.3 were stored in the first bucket, and rows of No.4 to No.6 were stored in the second one.
* Rows of No.7 to No.9 were classified into the second partition; rows of No.7 and No.8 were stored in the first bucket, and row No.9 was stored in the second one.
* Row No.10 was classified into the third partition and stored in the first bucket.

<br/>

### FIRST_VALUE()

FIRST_VALUE() returns the **first** value of the window range.

Syntax:

~~~SQL
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

For example 5:

We have the following data:

~~~SQL
 select name, country, greeting
 from mail_merge;
 ~~~

~~~Plain Text
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
~~~

Use FIRST_VALUE() to return the first greeting value in each grouping, based on the country grouping.

~~~SQL
select country, name,
    first_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
~~~

~~~Plain Text
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
~~~

### LAG()

The LAG() method is used to calculate the value of rows ahead of the current row.

Syntax:

~~~SQL
LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)
~~~

Example 6:

Calculate the closing price of the previous day

~~~SQL
select stock_symbol, closing_date, closing_price,
    lag(closing_price,1, 0) over
    (
        partition by stock_symbol
        order by closing_date
    ) as "yesterday closing"
from stock_ticker
order by closing_date;
~~~

~~~Plain Text
+--------------+---------------------+---------------+-------------------+
| stock_symbol | closing_date        | closing_price | yesterday closing |
+--------------+---------------------+---------------+-------------------+
| JDR          | 2014-09-13 00:00:00 | 12.86         | 0                 |
| JDR          | 2014-09-14 00:00:00 | 12.89         | 12.86             |
| JDR          | 2014-09-15 00:00:00 | 12.94         | 12.89             |
| JDR          | 2014-09-16 00:00:00 | 12.55         | 12.94             |
| JDR          | 2014-09-17 00:00:00 | 14.03         | 12.55             |
| JDR          | 2014-09-18 00:00:00 | 14.75         | 14.03             |
| JDR          | 2014-09-19 00:00:00 | 13.98         | 14.75             |
+--------------+---------------------+---------------+-------------------+
~~~

### LAST_VALUE()

LAST_VALUE() returns the **last** value of the window range. It is the opposite of FIRST_VALUE().

Syntax:

~~~SQL
LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

Use the data form the example 6:

~~~SQL
select country, name,
    last_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
~~~

~~~Plain Text
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
~~~

### LEAD()

The LEAD() method is used to calculate the value of rows after the current row.

Syntax:

~~~SQL
LEAD (expr, offset, default]) OVER (partition_by_clause order_by_clause)
~~~

Example 7:

Calculate the next day's closing price and compare it to today's  to see whether it is higher or lower.

~~~SQL
select stock_symbol, closing_date, closing_price,
    case
        (lead(closing_price,1, 0)
            over (partition by stock_symbol
                  order by closing_date)
         - closing_price) > 0
    when true then "higher"
    when false then "flat or lower"
    end as "trending"
from stock_ticker
order by closing_date;
~~~

~~~Plain Text
+--------------+---------------------+---------------+---------------+
| stock_symbol | closing_date        | closing_price | trending      |
+--------------+---------------------+---------------+---------------+
| JDR          | 2014-09-13 00:00:00 | 12.86         | higher        |
| JDR          | 2014-09-14 00:00:00 | 12.89         | higher        |
| JDR          | 2014-09-15 00:00:00 | 12.94         | flat or lower |
| JDR          | 2014-09-16 00:00:00 | 12.55         | higher        |
| JDR          | 2014-09-17 00:00:00 | 14.03         | higher        |
| JDR          | 2014-09-18 00:00:00 | 14.75         | flat or lower |
| JDR          | 2014-09-19 00:00:00 | 13.98         | flat or lower |
+--------------+---------------------+---------------+---------------+
~~~

### MAX()

Returns the maximum value of the specified rows in the current window.

Syntax：

~~~SQL
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

Example 8:

Calculate the maximum value of rows from the first row to the row after the current row.

~~~SQL
select x, property,
    max(x)
        over (
            order by property, x
            rows between unbounded preceding and 1 following
        ) as 'local maximum'
from int_t
where property in ('prime','square');
~~~

~~~Plain Text
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
~~~

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

~~~sql
select x, property,
    max(x)
        over (
            order by property, x
            rows between 3 preceding and 2 following) as 'local maximum'
from int_t
where property in ('prime','square');
~~~

### MIN()

Returns the minimum value of the specified rows in the current window.

Syntax：

~~~SQL
MIN([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

Example 9:

Calculate the minimum value of rows from the first row to the row after the current row.

~~~SQL
select x, property,
    min(x)
        over (
            order by property, x desc
            rows between unbounded preceding and 1 following
        ) as 'local minimum'
from int_t
where property in ('prime','square');
~~~

~~~Plain Text
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
~~~

From StarRocks 2.4 onwards, you can specify the row range as `rows between n preceding and n following`, which means you can capture `n` rows before the current row and `n` rows after the current row.

Example statement:

~~~sql
select x, property,
    min(x)
    over (
          order by property, x desc
          rows between 3 preceding and 2 following) as 'local minimum'
from int_t
where property in ('prime','square');
~~~

### RANK()

The RANK() function is used to represent rankings. Unlike DENSE_RANK(), RANK() will **appear as a vacant** number. For example, if two tied 1s appear, the third number of RANK() will be 3 instead of 2.

Syntax:

~~~SQL
RANK() OVER(partition_by_clause order_by_clause)
~~~

Example 10:

Ranking according to column x:

~~~SQL
select x, y, rank() over(partition by x order by y) as rank
from int_t;
~~~

~~~Plain Text
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
~~~

### ROW_NUMBER()

Returns a continuously increasing integer starting from 1 for each row of a Partition. Unlike RANK() and DENSE_RANK(), the value returned by ROW_NUMBER() **does not repeat or have gaps** and is **continuously incremented**.

Syntax:

~~~SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
~~~

Example 11:

~~~SQL
select x, y, row_number() over(partition by x order by y) as rank
from int_t;
~~~

~~~Plain Text
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
~~~

### SUM()

Syntax：

~~~SQL
SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

Example 12:

Group by property and calculate the sum of the **current, preceding, and following rows** within the group.

~~~SQL
select x, property,
    sum(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving total'
from int_t where property in ('odd','even');
~~~

~~~Plain Text
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
~~~
