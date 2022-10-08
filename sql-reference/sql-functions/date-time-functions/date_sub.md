# date_sub

## 功能

从日期中减去指定的时间间隔。

## 语法

```Haskell
DATETIME DATE_SUB(DATETIME date,INTERVAL expr type)
```

## 参数说明

* `date` ：合法的日期表达式。
* `expr` ：要减去的时间间隔。
* `type` ：可以是下列值：YEAR, MONTH, DAY, HOUR, MINUTE, SECOND。

## 示例

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+
```
