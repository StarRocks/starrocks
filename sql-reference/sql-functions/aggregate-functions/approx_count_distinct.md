# APPROX_COUNT_DISTINCT

## description

### Syntax

```Haskell
APPROX_COUNT_DISTINCT(expr)
```

返回类似于 `COUNT(DISTINCT col)` 结果的近似值聚合函数。

它比 COUNT 和 DISTINCT 组合的速度更快，并使用固定大小的内存，因此对于高基数的列可以使用更少的内存。

## example

```plain text
MySQL > select approx_count_distsinct(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```

## keyword

APPROX_COUNT_DISTINCT
