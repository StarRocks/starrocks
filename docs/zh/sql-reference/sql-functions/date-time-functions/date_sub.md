---
displayed_sidebar: "Chinese"
---

# date_sub

## 功能

从日期中减去指定的时间间隔。

## 语法

```Haskell
DATETIME DATE_SUB(DATETIME|DATE date,INTERVAL expr type)
```

## 参数说明

* `date`：合法的日期表达式。可以是 DATETIME 或 DATE 类型。
* `expr`：要减去的时间间隔，支持的数据类型为 INT。
* `type`：时间间隔的单位，取值可以是 YEAR，MONTH，DAY，HOUR，MINUTE，或 SECOND。

## 返回值说明

返回 DATETIME 类型的值。如果输入值为空或者格式不正确，返回 NULL。

## 示例

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+

select date_sub('2010-11-30', INTERVAL 2 hour);
+-----------------------------------------+
| date_sub('2010-11-30', INTERVAL 2 HOUR) |
+-----------------------------------------+
| 2010-11-29 22:00:00                     |
+-----------------------------------------+
1 row in set (0.01 sec)
```
