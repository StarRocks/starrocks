---
displayed_sidebar: docs
---

# hll_cardinality

単一の HLL 型の値の基数を計算します。

## Syntax

```Haskell
HLL_CARDINALITY(hll)
```

## Examples

```plain text
MySQL > select HLL_CARDINALITY(uv_set) from test_uv;
+---------------------------+
| hll_cardinality(`uv_set`) |
+---------------------------+
|                         3 |
+---------------------------+
```

## keyword

HLL,HLL_CARDINALITY