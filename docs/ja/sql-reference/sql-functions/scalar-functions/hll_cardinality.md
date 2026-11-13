---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "単一のHLL型の値の基数を計算します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hll_cardinality

単一の HLL 型の値の基数を計算します。

## Syntax

```plaintext
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