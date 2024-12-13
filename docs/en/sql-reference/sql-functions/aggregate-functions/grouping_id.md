---
displayed_sidebar: docs
---


# grouping_id

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

grouping_id is used to distinguish the grouping statistics results of the same grouping standard.

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
