# timestampadd

## description

将整数表达式间隔添加到日期或日期时间表达式`datetime_expr`中。

`interval`的单位由`unit`参数给出，应该是下列值之一:

SECOND，MINUTE，HOUR，DAY，WEEK，MONTH，YEAR。

## 语法

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

将整数表达式间隔添加到日期或日期时间表达式datetime_expr中。

interval的单位由unit参数给出，它应该是下列值之一:

SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, or YEAR。

## example

```plain text

SELECT TIMESTAMPADD(MINUTE,1,'2019-01-02');
+------------------------------------------------+
| timestampadd(MINUTE, 1, '2019-01-02 00:00:00') |
+------------------------------------------------+
| 2019-01-02 00:01:00                            |
+------------------------------------------------+

SELECT TIMESTAMPADD(WEEK,1,'2019-01-02');
+----------------------------------------------+
| timestampadd(WEEK, 1, '2019-01-02 00:00:00') |
+----------------------------------------------+
| 2019-01-09 00:00:00                          |
+----------------------------------------------+
```

## keyword

TIMESTAMPADD
