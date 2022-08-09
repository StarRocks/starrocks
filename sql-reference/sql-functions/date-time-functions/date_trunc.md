# date_trunc

## 背景

客户在使用时间函数时经常有关于group by hour/week这样的需求，而且希望能够将第一列直接展示成时间的格式，希望有类似 Oracle 的trunc函数这样的方式来对datetime进行高效的截断，从而避免写出类似如下的低效SQL:

```sql
select ADDDATE(DATE_FORMAT(DATE_ADD(from_unixtime(`timestamp`), INTERVAL 8 HOUR),
                           '%Y-%m-%d %H:%i:00'),
               INTERVAL 0 SECOND),
    count()
from xxx group 1 ;
```

## 目的

为了更快实现对时间数据的截断，提供向量化的date_trunc系列函数。

原本已有year/month/day等直接截取部分的时间函数。

date_trunc与此类似，对datetime进行高位截断：

```Plain Text
date_trunc("minute", datetime):
2020-11-04 11:12:19 => 2020-11-04 11:12:00
```

## 函数签名

1. Oracle使用的是`TRUNC(date,[fmt])`的函数格式。

2. PostgreSQL/redshift等使用的是`date_trunc(text,time)`的函数格式。

3. StarRocks采用`date_trunc([fmt], datetime)`的函数格式。

## 函数实现

对于date_trunc的实现，可以简单地分两步做：

1. 把datetime拆解成各个部分(年月日/是分秒)，然后提取所需要的部分。

2. 根据所需要的部分组合为一个新的datetime。

对于week/quarter的情况需要做特殊计算。

需要研究下snowflake的week。

```SQL
-- snowflake 中可以设置一周的第一天

-- 下面两个和默认一样把周一作为一周第一天
alter session set week_start = 0
alter session set week_start = 1

-- 把周三作为一周的第一天
alter session set week_start = 3
```

## 语法

```Haskell
DATETIME date_trunc(VARCHAR fmt, DATETIME datetime)
```

将 datetime 按照 fmt 格式进行截断。

* fmt 支持字符串常量，但是必须为固定的几个值(second，minute，hour，day，month，year，week，quarter)。
输入不对的值，在FE解析直接返回错误信息。

* datetime输入常量的情况会被正确识别到，只处理一次。

|  fmt 格式字符串   |  格式字符串语义   |  对应的例子  |
| --- | --- | --- |
| second |  截断到秒作为有效时间   |  2020-10-25 11:15:32 => 2020-10-25 11:15:32  |
| minute | 截断到分钟作为有效时间 | 2020-11-04 11:12:19 => 2020-11-04 11:12:00 |
| hour | 截断到小时作为有效时间 | 2020-11-04 11:12:13 => 2020-11-04 11:00:00 |
| day | 截断到天作为有效时间 | 2020-11-04 11:12:05 => 2020-11-04 00:00:00 |
| month | 截断到当月第一天作为有效时间 | 2020-11-04 11:12:51 => 2020-11-01 00:00:00 |
| year | 截断到当年第一天作为有效时间 | 2020-11-04 11:12:00 => 2020-01-01 00:00:00 |
| week | 截断到这个星期第一天作为有效时间 | 2020-11-04 11:12:00 => 2020-11-02 00:00:00 |
| quarter | 截断到这个季度第一天作为有效时间 | 2020-06-23 11:12:00 => 2020-04-01 00:00:00 |

## 示例

```Plain Text
MySQL > select date_trunc("hour", "2020-11-04 11:12:13")

2020-11-04 11:00:00
```
