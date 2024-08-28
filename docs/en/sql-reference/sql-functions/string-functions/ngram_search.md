---
displayed_sidebar: docs
---

# ngram_search

## Description

Calculate the ngram similarity of the two strings.

:::info
- Currently, the character encoding only supports ASCII encoding and does not support UTF-8 encoding.
- The function `ngram_search` is case-sensitive. Another function `ngram_search_case_insensitive` is case-insensitive. Other than that, these two functions are the same.
:::

## Syntax

```sql
DOUBLE ngram_search(VARCHAR haystack, VARCHAR needle, INT gram_num)
```

## Parameters

- `haystack`: required, the first string to compare. It must be a VARCHAR value. It can be a column name or a const value. If `haystack` is a column name, and an N-Gram bloom filter index is created for that column in the table, the index can accelerate the computation speed of the `ngram_search` function.
- `needle`: required, the second string to compare. It must be a VARCHAR value. It can only be a const value.

  :::tip

  - The length of the `needle` value can not be larger than 2^15. Otherwise an error will be thrown.
  - If the length of the `haystack` value is larger than 2^15, this function will return 0.
  - If the length of the `haystack` or `needle` value is smaller than `gram_num`, this function will return 0.
  
  :::

- `gram_num`: required, used for specifying the number of grams. The recommended value is `4`.

## Return value

Returns a value describing how similar these two strings are. The range of returned value is between 0 and 1. The larger this value, the more similar the two strings are.

## Examples

```SQL
-- The values of haystack and needle are const values.
mysql> select ngram_search('English', 'England',4);
+---------------------------------------+
| ngram_search('English', 'England', 4) |
+---------------------------------------+
|                                  0.25 |
+---------------------------------------+

-- The value of haystack is a column name and the value of needle is a const value.
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
