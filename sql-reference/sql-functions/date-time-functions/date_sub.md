# date_sub

## 功能

从日期中减去指定的时间间隔。

## 语法

```Haskell
DATETIME DATE_SUB(DATETIME date,INTERVAL expr type)
```

## 参数说明

* `date`：合法的日期表达式。
* `expr`：要减去的时间间隔，支持的数据类型为 INT。
* `type`：取值可以是 YEAR，MONTH，DAY，HOUR，MINUTE，或 SECOND。

## 返回值说明

返回 DATETIME 类型的值。如果输入值为空或者不存在，返回 NULL。

## 示例

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+
```
