---
displayed_sidebar: docs
---

# grouping_id

## Description

grouping_id は、同じグループ化基準のグループ化統計結果を区別するために使用されます。

## Syntax

```Haskell
GROUPING_ID(expr)
```

## Examples

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