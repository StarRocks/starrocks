---
displayed_sidebar: docs
---

# approx_count_distinct

集計関数の近似値を返します。これは COUNT(DISTINCT col) の結果に似ています。

COUNT と DISTINCT の組み合わせよりも高速で、固定サイズのメモリを使用するため、高いカーディナリティを持つ列に対してメモリ使用量が少なくなります。

## Syntax

```Haskell
APPROX_COUNT_DISTINCT(expr)
```

## Examples

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