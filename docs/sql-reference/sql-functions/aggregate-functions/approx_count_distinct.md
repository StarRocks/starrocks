# APPROX_COUNT_DISTINCT

## description

### Syntax

```Haskell
APPROX_COUNT_DISTINCT(expr)
```

It returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col).

It is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is used for columns of high cardinality.

## example

```plain text
MySQL > select approx_count_distinct(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```

## keyword

APPROX_COUNT_DISTINCT
