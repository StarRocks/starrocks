---
displayed_sidebar: "English"
---

# HLL_CARDINALITY

## Description

HLL_CARDINALITY is used to calculate the cardinality of a single HLL type value.

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