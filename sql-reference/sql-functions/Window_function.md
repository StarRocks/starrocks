# 使用窗口函数组织过滤数据

本文介绍如何使用 StarRocks 窗口函数。

窗口函数是 StarRocks 内置的特殊函数。和聚合函数类似，窗口函数通过对多行数据进行计算得到一个数据值。不同的是，窗口函数使用 Over() 子句对**当前窗口**内的数据进行排序和分组，同时**对结果集的每一行**计算出一个单独的值，而不是对每个 Group By 分组计算一个值。这种灵活的方式允许您在 SELECT 子句中增加额外的列，对结果集进行重新组织和过滤。

窗口函数在金融和科学计算领域较为常用，常被用来分析趋势、计算离群值以及对大量数据进行分桶分析等。

当前 StarRocks 支持的窗口函数包括：

* `MIN()`, `MAX()`, `COUNT()`, `SUM()`, `AVG()`
* `FIRST_VALUE()`, `LAST_VALUE()`, `LEAD()`, `LAG()`
* `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
* `NTILE()`

## 窗口函数语法及参数

语法：

~~~SQL
FUNCTION(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
~~~

> 注意：窗口函数只能出现在 SELECT 列表和最外层的 Order By 子句中。在查询过程中，窗口函数会在最后生效，也就是在执行完 Join，Where 和 Group By 等操作之后生效。

参数：

* **partition_by_clause**：Partition By 子句。该子句将输入行按照指定的一列或多列分组，相同值的行会被分到一组。
* **order_by_clause**：Order By 子句。与外层的 Order By 类似，Order By 子句定义了输入行的排列顺序，如果指定了 Partition By，则 Order By 定义了每个 Partition 分组内的顺序。与外层 Order By 的唯一不同在于，OVER() 子句中的 `Order By n`（n是正整数）相当于不做任何操作，而外层的 `Order By n` 表示按照第 `n` 列排序。

    以下示例展示了在 SELECT 列表中增加一个 `id` 列，它的值是 `1`，`2`，`3` 等，顺序按照 `events` 表中的 `date_and_time` 列排序。

    ~~~SQL
    SELECT row_number() OVER (ORDER BY date_and_time) AS id,
        c1, c2, c3, c4
    FROM events;
    ~~~

* **window_clause**：Window 子句，可以用来为窗口函数指定一个运算范围，以当前行为准，前后若干行作为窗口函数运算的对象。Window 子句支持的函数有：`AVG()`、`COUNT()`、`FIRST_VALUE()`、`LAST_VALUE()` 和 `SUM()`。对于 `MAX()` 和 `MIN()`，Window 子句可以通过 UNBOUNDED、PRECEDING 关键词指定开始范围。

    Window 子句语法：

    ~~~SQL
    ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
    ~~~

    > 注意：Window 子句必须在 Order By 子句之内。

## 使用 AVG() 窗口函数

`AVG()` 函数用于计算特定窗口内选中字段的平均值。

语法：

~~~SQL
AVG( expression ) [OVER (*analytic_clause*)]
~~~

以下示例模拟如下的股票数据，股票代码是 `JDR`，`closing price` 代表其每天的收盘价。

~~~SQL
CREATE TABLE stock_ticker (
    stock_symbol  STRING,
    closing_price DECIMAL(8,2),
    closing_date  DATETIME
)
DUPLICATE KEY(stock_symbol)
COMMENT "OLAP"
DISTRIBUTED BY HASH(closing_date) BUCKETS 3;

INSERT INTO stock_ticker VALUES 
    ("JDR", 12.86, "2014-10-02 00:00:00"), 
    ("JDR", 12.89, "2014-10-03 00:00:00"), 
    ("JDR", 12.94, "2014-10-04 00:00:00"), 
    ("JDR", 12.55, "2014-10-05 00:00:00"), 
    ("JDR", 14.03, "2014-10-06 00:00:00"), 
    ("JDR", 14.75, "2014-10-07 00:00:00"), 
    ("JDR", 13.98, "2014-10-08 00:00:00")
;
~~~

以下示例使用 `AVG()` 函数计算了该股票每日与其前后一日的收盘价均值。

~~~SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
~~~

返回：

~~~Plain Text
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
~~~

<br/>

## 使用 COUNT() 窗口函数

`COUNT()` 函数用于返回特定窗口内满足要求的行的数目。

语法：

~~~SQL
COUNT( expression ) [OVER (analytic_clause)]
~~~

以下示例使用 `COUNT()` 计算了从**当前行到第一行**数据 `property` 列数据出现的次数。

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

返回：

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

<br/>

## 使用 DENSE_RANK() 窗口函数

`DENSE_RANK()` 函数用来为特定窗口中的数据排名。当函数中出现相同排名时，下一行的排名为相同排名数加 1。因此，`DENSE_RANK()` 返回的序号**是连续的数字**。而 `RANK()` 返回的序号**有可能是不连续的数字**。

语法：

~~~SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
~~~

以下示例使用 `DENSE_RANK()` 对 `x` 列排名。

~~~SQL
select x, y,
    dense_rank()
        over (
            partition by x
            order by y
        ) as rank
from int_t;
~~~

返回：

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

<br/>

## 使用 FIRST_VALUE() 窗口函数

`FIRST_VALUE()` 函数返回窗口范围内的**第一个**值。

语法：

~~~SQL
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

以下示例使用的数据如下：

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

以下示例使用 `FIRST_VALUE()` 函数，根据 `country` 列分组，返回每个分组中第一个 `greeting` 的值。

~~~SQL
select country, name,
    first_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
~~~

返回：

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

<br/>

## 使用 LAG() 窗口函数

`LAG()` 函数用来计算当前行**向前**数若干行的值。

语法：

~~~SQL
LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)
~~~

以下示例计算 `stock_ticker` 表中每个 `closing_date` 前一天的收盘价。

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

返回：

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

<br/>

## 使用 LAST_VALUE() 窗口函数

`LAST_VALUE()` 返回窗口范围内的**最后一个**值。与 `FIRST_VALUE()` 相反。

语法：

~~~SQL
LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

以下示例使用 `LAST_VALUE()` 函数，根据 `country` 列分组，返回每个分组中最后一个 `greeting` 的值。

~~~SQL
select country, name,
    last_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
~~~

返回：

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

<br/>

## 使用 LEAD() 窗口函数

`LEAD()` 函数用来计算当前行**向后**数若干行的值。

语法：

~~~SQL
LEAD (expr, offset, default) OVER (partition_by_clause order_by_clause)
~~~

以下示例计算第二天的收盘价对比当天收盘价的走势，即第二天收盘价比当天高还是低。

~~~SQL
select stock_symbol, closing_date, closing_price,
    case
        (lead(closing_price, 1, 0)
            over (partition by stock_symbol
                  order by closing_date)
         - closing_price) > 0
    when true then "higher"
    when false then "flat or lower"
    end as "trending"
from stock_ticker
order by closing_date;
~~~

返回：

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

<br/>

## 使用 MAX() 窗口函数

`MAX()` 函数返回当前窗口指定行数内数据的最大值。

语法：

~~~SQL
MAX(expression) [OVER (analytic_clause)]
~~~

以下示例计算**从第一行到当前行之后一行中**的最大值。

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

返回结果：

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

从2.4版本开始，该函数支持设置`rows between n preceding and n following`，即支持计算当前行前n行及后n行中的最大值。比如要计算当前行前3行和后2行中的最大值，语句可写为：

~~~SQL
select x, property,
    max(x)
        over (
            order by property, x
            rows between 3 preceding and 2 following) as 'local maximum'
from int_t
where property in ('prime','square');
~~~

## 使用 MIN() 窗口函数

`MIN()` 函数返回当前窗口指定行数内数据的最小值。

语法：

~~~SQL
MIN(expression) [OVER (analytic_clause)]
~~~

以下示例计算**从第一行到当前行之后一行中**的最小值。

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

返回结果：

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

从2.4版本开始，该函数支持设置`rows between n preceding and n following`，即支持计算当前行前n行以及后`n`行中的最小值。比如要计算当前行前3行和后2行中的最小值，语句可写为：

~~~SQL
select x, property,
    min(x)
    over (
          order by property, x desc
          rows between 3 preceding and 2 following) as 'local minimum'
from int_t
where property in ('prime','square');
~~~

## 使用 NTILE() 窗口函数

`NTILE()` 函数将分区中已排序的数据**尽可能均匀**地分配至指定数量（`num_buckets`）的桶中，并返回每一行所在的桶号。桶的编号从 `1` 开始直至 `num_buckets`。`NTILE()` 的返回类型为 BIGINT。

> 说明
>
> * 如果分区包含的行数无法被 `num_buckets` 整除，那么会存在两个不同的分桶大小，它们的差值为 1。较大的分桶位于较小的分桶之前。
> * 如果分区包含的行数可以被 `num_buckets` 整除，那么所有分桶的大小相同。

语法：

~~~~SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
~~~~

其中，`num_buckets` 是要划分桶的数量，必须是一个常量正整数，最大值为 BIGINT 的最大值，即 `2^63 - 1`。

> 注意
> `NTILE()` 函数不能使用 Window 子句。

以下示例使用 `NTILE()` 函数当前窗口中的数据划分至 `2` 个桶中，划分结果见 `bucket_id` 列。

~~~~sql
select id, x, y,
    ntile(2)
        over (
            partition by x
            order by y
        ) as bucket_id
from t1;
~~~~

返回：

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

如上述例子所示，`num_buckets` 为 `2`，此时：

* 第 1-6 行为一个分区，其中第 1-3 行在第一个分桶中、第 4-6 行在第二个分桶中。
* 第 7-9 行为一个分区，其中第 7-8 行在第一个分桶中、第 9 行在第二个分桶中。
* 第 10 行为一个分区，其在第一个分桶中。

<br/>

## 使用 RANK() 窗口函数

`RANK()` 函数用来对当前窗口内的数据进行排名，返回结果集是对分区内每行的排名，行的排名是相关行之前的排名数加一。与 `DENSE_RANK()` 不同的是， `RANK()` 返回的序号**有可能是不连续的数字**，而 `DENSE_RANK()` 返回的序号**是连续的数字**。

语法：

~~~SQL
RANK() OVER(partition_by_clause order_by_clause)
~~~

以下示例为 `x` 列排名。

~~~SQL
select x, y, 
    rank() over(
        partition by x 
        order by y
    ) as rank
from int_t;
~~~

返回：

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

<br/>

## 使用 ROW_NUMBER() 窗口函数

`ROW_NUMBER()` 函数为每个 Partition 的每一行返回一个从 `1` 开始连续递增的整数。与 `RANK()` 和 `DENSE_RANK()` 不同的是，`ROW_NUMBER()` 返回的值**不会重复也不会出现空缺**，是**连续递增**的。

语法：

~~~SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
~~~

以下示例使用 `ROW_NUMBER()` 为以 `x` 列为分区划分的数据指定 `rank`。

~~~SQL
select x, y, 
    row_number() over(
        partition by x 
        order by y
    ) as rank
from int_t;
~~~

返回：

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

<br/>

## 使用 SUM() 窗口函数

`SUM()` 函数对特定窗口内指定行求和。

语法：

~~~SQL
SUM(expression) [OVER (analytic_clause)]
~~~

以下示例将数据按照 `property` 列进行分组，并在组内计算**当前行以及前后各一行**的 `x` 列数据的和。

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

返回：

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
