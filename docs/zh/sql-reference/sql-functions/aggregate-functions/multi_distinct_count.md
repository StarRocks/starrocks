---
displayed_sidebar: docs
---

# multi_distinct_count

## 功能

返回 `expr` 中去除重复值后的行数，功能等同于 COUNT(DISTINCT expr)。

## 语法

```Haskell
multi_distinct_count(expr)
```

## 参数说明

`epxr`: 条件表达式。`expr` 为列名时，列值支持任意类型。

## 返回值说明

返回值为数值类型。如果没有匹配的行，则返回 0。 NULL 值不参与统计。

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

示例一：通过 multi_distinct_count 去重，查看 `category` 数量。

```Plain
select multi_distinct_count(category) from test;
+--------------------------------+
| multi_distinct_count(category) |
+--------------------------------+
|                              4 |
+--------------------------------+
```

示例二：通过 multi_distinct_count 去重，查看供应商 `supplier` 数量。

```Plain
select multi_distinct_count(supplier) from test;
+--------------------------------+
| multi_distinct_count(supplier) |
+--------------------------------+
|                              5 |
+--------------------------------+
```
