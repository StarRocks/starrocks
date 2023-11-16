---
displayed_sidebar: "English"
---


# GROUPING_ID

## Description

GROUPING_ID is used to distinguish the grouping statistics results of the same grouping standard.

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
