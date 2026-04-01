---
displayed_sidebar: docs
sidebar_label: "INTERSECT"
---

# INTERSECT

计算多个查询结果的交集，即出现在所有结果集中的结果。该子句仅返回结果集中唯一的行。不支持 ALL 关键字。

## 语法

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **注意**
>
> - INTERSECT 等同于 INTERSECT DISTINCT。
> - 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

## 示例

使用 UNION 中的两个表。

返回两个表中通用的不同 `(id, price)` 组合。以下两个语句是等效的。

```Plaintext
mysql> (select id, price from select1) intersect (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+

mysql> (select id, price from select1) intersect distinct (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+
```
