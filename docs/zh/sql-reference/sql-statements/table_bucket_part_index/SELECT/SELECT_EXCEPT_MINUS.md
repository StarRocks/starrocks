---
displayed_sidebar: docs
sidebar_label: "EXCEPT/MINUS"
---

# EXCEPT/MINUS

返回左侧查询中存在但右侧查询中不存在的不同结果。EXCEPT 等同于 MINUS。

## 语法

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **说明**
>
> - `EXCEPT` 等同于 `EXCEPT DISTINCT`。不支持 `ALL` 关键字。
> - 每个查询语句必须返回相同数量的列，并且这些列必须具有兼容的数据类型。

## 示例

以下示例使用 `UNION` 中的两个表。

返回 `select1` 中找不到的 `(id, price)` 的不同组合。

```Plaintext
mysql> (select id, price from select1) except (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+

mysql> (select id, price from select1) minus (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+
```
