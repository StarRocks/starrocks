---
displayed_sidebar: docs
---

# hll_union

## Description

HLL 値のセットを連結して返します。

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