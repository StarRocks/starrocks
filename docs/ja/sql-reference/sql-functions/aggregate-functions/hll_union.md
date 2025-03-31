---
displayed_sidebar: docs
---

# hll_union

HLL 値のセットを連結して返します。

## 構文

```Haskell
hll_union(hll)
```

## 例

```Plain
mysql> select k1, hll_cardinality(hll_union(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_union(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```