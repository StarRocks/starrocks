---
displayed_sidebar: "English"
---


# HLL_UNION

## Description

Returns the concatenation of a set of HLL values.

## Syntax

```Haskell
hll_union(hll)
```

## Examples

```Plain
mysql> select k1, hll_cardinality(hll_union(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_union(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```
