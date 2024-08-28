---
displayed_sidebar: docs
---


# count

## 功能

计算总行数。

该函数有 3 种形式：

- COUNT(*) 返回表中所有的行数。

- COUNT(expr) 返回某列中非 NULL 值的行数。

- COUNT(DISTINCT expr) 返回某列去重后非 NULL 值的行数。

COUNT DISTINCT 用于精确去重，如果需要更好的去重性能，可参考[使用 Bitmap 实现精确去重](../../../using_starrocks/Using_bitmap.md)。

**从 2.4 版本开始，StarRocks 支持在一条查询里使用多个 COUNT(DISTINCT)。**

## 语法

```Haskell
COUNT(expr)
COUNT(DISTINCT expr [,expr,...])
```

## 参数说明

`epxr`: 条件表达式。`expr` 为列名时，列值支持任意类型。

## 返回值说明

返回值为数值类型。如果没有匹配的行，则返回0。 NULL值不参与统计。

## 示例

假设有表 `test`，按照订单 `id` 显示每个订单的国家、商品类别、供应商编号。

```Plain
select * from test order by id;
+------+----------+----------+------------+
| id   | country  | category | supplier   |
+------+----------+----------+------------+
| 1001 | US       | A        | supplier_1 |
| 1002 | Thailand | A        | supplier_2 |
| 1003 | Turkey   | B        | supplier_3 |
| 1004 | US       | A        | supplier_2 |
| 1005 | China    | C        | supplier_4 |
| 1006 | Japan    | D        | supplier_3 |
| 1007 | Japan    | NULL     | supplier_5 |
+------+----------+----------+------------+
```

示例一：查看表中有多少行数据。

```Plain
select count(*) from test;
+----------+
| count(*) |
+----------+
|        7 |
+----------+
```

示例二：查看订单 `id` 数量。

```Plain
select count(id) from test;
+-----------+
| count(id) |
+-----------+
|         7 |
+-----------+
```

示例三：查看 `category` 数量。求和时忽略 NULL 值。

```Plain
```SQL
select count(category) from test;
  +-----------------+
  | count(category) |
  +-----------------+
  |         6       |
  +-----------------+
```

示例四：通过 DISTINCT 去重，查看 `category` 数量。

```Plain
select count(distinct category) from test;
+-------------------------+
| count(DISTINCT category) |
+-------------------------+
|                       4 |
+-------------------------+
```

示例五：查看商品类别（`category`）和供应商（`supplier`）共有多少种不同的组合。

```Plain
select count(distinct category, supplier) from test;
+------------------------------------+
| count(DISTINCT category, supplier) |
+------------------------------------+
|                                  5 |
+------------------------------------+
```

在以上结果中，`id` 为1004的组合与 `id` 为1002的组合重复，只统计一次；`id` 为1007的组合内有NULL值，不统计。

示例六：一条查询里使用多个 COUNT(DISTINCT)。

```Plain
select count(distinct country, category), count(distinct country,supplier) from test;
+-----------------------------------+-----------------------------------+
| count(DISTINCT country, category) | count(DISTINCT country, supplier) |
+-----------------------------------+-----------------------------------+
|                                 6 |                                 7 |
+-----------------------------------+-----------------------------------+
```
