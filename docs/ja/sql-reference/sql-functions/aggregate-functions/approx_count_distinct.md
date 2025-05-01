---
displayed_sidebar: docs
---

# approx_count_distinct

## 説明

COUNT(DISTINCT col) の結果に似た集計関数の近似値を返します。

これは COUNT と DISTINCT の組み合わせよりも高速で、固定サイズのメモリを使用するため、高いカーディナリティの列に対して使用するメモリが少なくなります。

## 構文

```Haskell
APPROX_COUNT_DISTINCT(expr)
```

## 例

```plain text
MySQL > select approx_count_distinct(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```

## キーワード

APPROX_COUNT_DISTINCT