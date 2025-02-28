# DS_HLL



Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col). Like APPROX_COUNT_DISTINCT(expr).

It is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is used for columns of high cardinality.

It is slower than APPROX_COUNT_DISTINCT(expr) but with higher precision. Which takes advantages of Apache Datasketches.

## Syntax

```Haskell
DS_HLL(expr)
```

## Examples

```plain text
MySQL > select DS_HLL(query_id) from log_statis group by datetime;
+-----------------------------------+
| DS_HLL(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```
