---
displayed_sidebar: "Chinese"
---

# dayofweek

## 功能

返回指定日期的工作日索引值，即星期日为 1，星期一为 2，星期六为 7。

参数为 DATE 或 DATETIME 类型，或者为可以 CAST 成 DATE 或 DATETIME 类型的数字。

## 语法

```Haskell
INT dayofweek(DATETIME date)
```

## 示例

```Plain Text
select dayofweek('2019-06-25');
+----------------------------------+
| dayofweek('2019-06-25 00:00:00') |
+----------------------------------+
|                                3 |
+----------------------------------+

select dayofweek(cast(20190625 as date));
+-----------------------------------+
| dayofweek(CAST(20190625 AS DATE)) |
+-----------------------------------+
|                                 3 |
+-----------------------------------+
```
