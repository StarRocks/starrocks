# date_add

## description

### Syntax

```Haskell
DATETIME DATE_ADD(DATETIME date,INTERVAL expr type)
```

向日期添加指定的时间间隔。

* `date`：必须是合法的日期表达式。

* `expr`：需要添加的时间间隔，支持的数据类型为 INT。

* `type`：取值可以是 YEAR，MONTH，DAY，HOUR，MINUTE，或 SECOND。

## 返回值说明

返回 DATETIME 类型的值。如果输入值为空或者不存在，返回 NULL。

## example

```Plain Text
MySQL > select date_add('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_add('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+

select date_add('2010-11-30', INTERVAL 2 DAY);
+----------------------------------------+
| date_add('2010-11-30', INTERVAL 2 DAY) |
+----------------------------------------+
| 2010-12-02 00:00:00                    |
+----------------------------------------+
1 row in set (0.01 sec)

```

## keyword

DATE_ADD,DATE,ADD
