# COUNT

## 功能

返回满足条件的行数。

该函数有3种形式：

- COUNT(*) 返回表中所有的行数。

- COUNT(expr) 返回某列中非NULL值的行数。

- COUNT(DISTINCT expr) 返回某列去重后非NULL值的行数。

该函数支持与WHERE，GROUP BY子句搭配使用。

COUNT DISTINCT 用于精确去重，如果需要更好的去重性能，可参考[使用 Bitmap 精确去重](../../../using_starrocks/Using_bitmap.md)。

## 语法

```Haskell
COUNT([DISTINCT] expr)
```

## 参数说明

`epxr`: 条件表达式。`expr`为列名时，列值支持任意类型。

## 返回值说明

返回值为数值类型。如果没有匹配的行，则返回0。 NULL值不参与统计。

## 示例

假设有表`test`, 按照订单`id`显示每个订单的国家、商品类别、供应商编号。

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

示例二：查看订单`id`数量。

```Plain
select count(id) from test;
+-----------+
| count(id) |
+-----------+
|         7 |
+-----------+
```

示例三：查看`category`数量。求和时忽略 NULL 值。

```Plain
```SQL
select count(category) from test;
  +-----------------+
  | count(category) |
  +-----------------+
  |         6       |
  +-----------------+
```

示例四：通过DISTINCT去重，查看`category`数量。

```Plain
select count(distinct category) from test;
+-------------------------+
| count(DISTINCT category) |
+-------------------------+
|                       4 |
+-------------------------+
```
