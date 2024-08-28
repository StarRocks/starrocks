---
displayed_sidebar: docs
---

# ngram_search

## 功能

计算两个字符串的 ngram 相似度。

:::info
- 目前字符编码仅支持 ASCII 编码，不支持 UTF-8 编码。
- 函数 `ngram_search` 区分大小写。另一个函数 `ngram_search_case_insensitive` 不区分大小写。除此之外，这两个函数是相同的。
:::

## 语法

```SQL
DOUBLE ngram_search(VARCHAR haystack, VARCHAR needle, INT gram_num)
```

## 参数说明

- `haystack`: 必填项，需要比较的第一个字符串。必须是 VARCHAR 值，可以是列名或常量值。如果 `haystack` 为列名，并且表中已经基于该列创建 N-Gram bloom filter 索引，则该索引可以加速 `ngram_search` 函数的运算速度。
- `needle`: 必填项，需要比较的第二个字符串。必须是 VARCHAR 值，且只能是常量值。

    :::tip
  
    - `needle` 值的长度不能大于 2^15，否则会抛出错误。
    - 如果 `haystack` 值的长度大于 2^15，则此函数将返回 0。
    - 如果 `haystack` 或 `needle` 值的长度小于 `gram_num`，则此函数将返回 0。
 
    :::

- `gram_num`: 必填项，用于指定 gram 的数量。推荐值为 `4`。

## 返回值说明

返回一个描述这两个字符串相似程度的值。返回值的范围在 0 和 1 之间。该值越大，两个字符串越相似。

## 示例

```SQL
-- haystack 和 needle 都输常量值
mysql> select ngram_search("chinese","china",4);
+----------------------------------+
| ngram_search('chinese', 'china') |
+----------------------------------+
|                              0.5 |
+----------------------------------+

-- haystack 是列名，needle 是常量值
mysql> select rowkey,ngram_search(rowkey,"31dc496b-760d-6f1a-4521-050073a70000",4) as string_similarity from string_table order by string_similarity desc limit 5;
+--------------------------------------+-------------------+
| rowkey                               | string_similarity |
+--------------------------------------+-------------------+
| 31dc496b-760d-6f1a-4521-050073a70000 |                 1 |
| 31dc496b-760d-6f1a-4521-050073a40000 |         0.8787879 |
| 31dc496b-760d-6f1a-4521-05007fa70000 |         0.8787879 |
| 31dc496b-760d-6f1a-4521-050073a30000 |         0.8787879 |
| 31dc496b-760d-6f1a-4521-0500c3a70000 |         0.8787879 |
+--------------------------------------+-------------------+
```
