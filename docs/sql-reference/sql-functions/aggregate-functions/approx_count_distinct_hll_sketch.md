# APPROX_COUNT_DISTINCT_HLL_SKETCH

## Description

Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col). Like APPROX_COUNT_DISTINCT(expr).

It is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is used for columns of high cardinality.

It is slower than APPROX_COUNT_DISTINCT(expr) but with higher precision. Which takes advantages of Apache Datasketches.

## Syntax

```Haskell
APPROX_COUNT_DISTINCT_HLL_SKETCH(expr)
```

## Examples

```plain text
MySQL > select APPROX_COUNT_DISTINCT_HLL_SKETCH(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct_hll_sketch(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```

## keyword

APPROX_COUNT_DISTINCT_HLL_SKETCH,APPROX_COUNT_DISTINCT
