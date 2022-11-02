# 窗口函数

## 背景介绍

窗口函数是一类特殊的内置函数。和聚合函数类似，窗口函数也是对于多个输入行做计算得到一个数据值。不同的是，窗口函数是在一个特定的窗口内对输入数据做处理，而不是按照group by来分组计算。每个窗口内的数据可以用over()从句进行排序和分组。窗口函数会**对结果集的每一行**计算出一个单独的值，而不是每个group by分组计算一个值。这种灵活的方式允许用户在select从句中增加额外的列，给用户提供了更多的机会来对结果集进行重新组织和过滤。窗口函数只能出现在select列表和最外层的order by从句中。在查询过程中，窗口函数会在最后生效，就是说，在执行完join，where和group by等操作之后再执行。窗口函数在金融和科学计算领域经常被使用到，用来分析趋势、计算离群值以及对大量数据进行分桶分析等。

<br/>

## 使用方式

窗口函数的语法：

~~~SQL
function(args) OVER(partition_by_clause order_by_clause [window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
~~~

### Function

目前支持的Function包括：

* MIN(), MAX(), COUNT(), SUM(), AVG()
* FIRST_VALUE(), LAST_VALUE(), LEAD(), LAG()
* ROW_NUMBER(), RANK(), DENSE_RANK()

### PARTITION BY从句

Partition By从句和Group By类似。它把输入行按照指定的一列或多列分组，相同值的行会被分到一组。

### ORDER BY从句

Order By从句和外层的Order By基本一致。它定义了输入行的排列顺序，如果指定了Partition By，则Order By定义了每个Partition分组内的顺序。与外层Order By的唯一不同点是：OVER从句中的`Order By n`（n是正整数）相当于不做任何操作，而外层的Order By n表示按照第n列排序。

举例:

这个例子展示了在select列表中增加一个id列，它的值是1，2，3等等，顺序按照events表中的date_and_time列排序。

~~~SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
~~~

### Window Clause

Window从句用来为窗口函数指定一个运算范围，以当前行为准，前后若干行作为窗口函数运算的对象。Window从句支持的方法有：AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE()和SUM()。对于 MAX()和MIN(), window从句可以指定开始范围UNBOUNDED PRECEDING

语法：

~~~SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
~~~

举例：

假设我们有如下的股票数据，股票代码是JDR，closing price是每天的收盘价。

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

得到原始数据如下：

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

这个查询使用窗口函数产生moving_average这一列，它的值是3天的股票均价，即前一天、当前以及后一天三天的均价。第一天没有前一天的值，最后一天没有后一天的值，所以这两行只计算了两天的均值。这里Partition By没有起到作用，因为所有的数据都是JDR的数据，但如果还有其他股票信息，Partition By会保证窗口函数值作用在本Partition之内。

~~~SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
~~~

得到如下数据：

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

<br/>

## 函数举例

本节介绍StarRocks中可以用作窗口函数的方法。

<br/>

### AVG()

语法：

~~~SQL
AVG([DISTINCT | ALL] *expression*) [OVER (*analytic_clause*)]
~~~

举例：

计算当前行和它**前后各一行**数据的x平均值

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

<br/>

### COUNT()

语法：

~~~SQL
COUNT([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

举例：

计算从**当前行到第一行**x出现的次数。

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

<br/>

### DENSE_RANK()

DENSE_RANK()函数用来表示排名，与RANK()不同的是，DENSE_RANK()**不会出现空缺**数字。比如，如果出现了两个并列的1，DENSE_RANK()的第三个数仍然是2，而RANK()的第三个数是3。

语法：

~~~SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
~~~

举例：
下例展示了按照property列分组对x列排名：

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

<br/>

### FIRST_VALUE()

FIRST_VALUE()返回窗口范围内的**第一个**值。

语法：

~~~SQL
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

举例：

我们有如下数据

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

使用FIRST_VALUE()，根据country分组，返回每个分组中第一个greeting的值：

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

<br/>

### LAG()

LAG()方法用来计算当前行**向前**数若干行的值。

语法：

~~~SQL
LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)
~~~

举例：

计算前一天的收盘价

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

<br/>

### LAST_VALUE()

LAST_VALUE()返回窗口范围内的**最后一个**值。与FIRST_VALUE()相反。

语法：

~~~SQL
LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
~~~

使用FIRST_VALUE()举例中的数据：

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

<br/>

### LEAD()

LEAD()方法用来计算当前行**向后**数若干行的值。

语法：

~~~SQL
LEAD (expr, offset, default]) OVER (partition_by_clause order_by_clause)
~~~

举例：

计算第二天的收盘价对比当天收盘价的走势，即第二天收盘价比当天高还是低。

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

<br/>

### MAX()

语法：

~~~SQL
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

举例：

计算**从第一行到当前行之后一行**的最大值

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

<br/>

### MIN()

语法：

~~~SQL
MIN([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

举例：

计算**从第一行到当前行之后一行**的最小值

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

<br/>

### RANK()

RANK()函数用来表示排名，与DENSE_RANK()不同的是，RANK()会**出现空缺**数字。比如，如果出现了两个并列的1，RANK()的第三个数就是3，而不是2。

语法：

~~~SQL
RANK() OVER(partition_by_clause order_by_clause)
~~~

举例：

根据x列进行排名

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

<br/>

### ROW_NUMBER()

为每个Partition的每一行返回一个从1开始连续递增的整数。与RANK()和DENSE_RANK()不同的是，ROW_NUMBER()返回的值**不会重复也不会出现空缺**，是**连续递增**的。

语法：

~~~SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
~~~

举例：

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

<br/>

### SUM()

语法：

~~~SQL
SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]
~~~

举例：

按照property进行分组，在组内计算**当前行以及前后各一行**的x列的和。

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
