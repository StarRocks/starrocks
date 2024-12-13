---
displayed_sidebar: docs
---

# hll_cardinality

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Calculates the cardinality of a single HLL type value.

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
