---
displayed_sidebar: docs
---

# hll_cardinality



Calculates the cardinality of a single HLL type value.

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
