---
displayed_sidebar: docs
---


# grouping_id

## 功能

用于区分相同分组标准的分组统计结果。

## 语法

```Haskell
GROUPING_ID(expr)
```

## 参数说明

`epxr`: 条件表达式，表达式的结果需要是 BIGINT 类型。

## 返回值说明

返回 BIGINT 类型的值。

## 示例

```Plain
MySQL > SELECT COL1,GROUPING_ID(COL2) AS 'GroupingID' FROM tbl GROUP BY ROLLUP (COL1, COL2);
+------+------------+
| COL1 | GroupingID |
+------+------------+
| NULL |          1 |
| 2.20 |          1 |
| 2.20 |          0 |
| 1.10 |          1 |
| 1.10 |          0 |
+------+------------+
```
