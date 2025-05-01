---
displayed_sidebar: docs
---

# hll_cardinality

## 説明

単一の HLL 型の値の基数を計算します。

## 構文

```Haskell
HLL_CARDINALITY(hll)
```

## 例

```plain text
MySQL > select HLL_CARDINALITY(uv_set) from test_uv;
+---------------------------+
| hll_cardinality(`uv_set`) |
+---------------------------+
|                         3 |
+---------------------------+
```

## キーワード

HLL,HLL_CARDINALITY