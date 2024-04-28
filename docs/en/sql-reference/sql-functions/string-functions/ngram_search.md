---
displayed_sidebar: "English"
---

# ngram_search

## Description

Calculate ngram similarity of the two string.

## Syntax

```sql
DOUBLE ngram_search(VARCHAR haystack, VARCHAR needle, INT gram_num)
```

## Parameters

- `haystack`: required, the first string to compare. It must be a VARCHAR value, can be a column or a const value.
- `needle`: required, the second string to compare. It must be a VARCHAR value, can only be const value.

  :::tip

  - The needle's size can not be larger than 2^15, otherwise error will be thrown.
  - if haystack's size is larger than 2^15, this function will return 0.
  - If haystack or needle's size is smaller than gram_num, then this fucntion will return 0.
  
  :::

- `gram_num`: required, used for specify the number of gram. The recommended value is `4`.

## Return value

Returns a value describing how similar these two strings are. The range of return value is between 0 and 1, and the larger the value, the closer the two are.

## Examples

```SQL
-- haystack and needle are const value
mysql> select ngram_search("chinese","china",4);
+----------------------------------+
| ngram_search('chinese', 'china') |
+----------------------------------+
|                              0.5 |
+----------------------------------+

-- haystack is a column and needle are const value
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

## Other note

- Currently we only support Ascii encoding.

- The function `ngram_search` is case-sensitive, while the other function, `ngram_search_case_insensitive`, is case-insensitive. Other than that, these two functions are identical.
