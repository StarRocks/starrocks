---
displayed_sidebar: docs
---

# regexp_split



Split string `str` by regexp expression `pattern`, return maximum `max_split` elements in `ARRAY<VARCHAR>` type.

## Syntax

```Haskell
regexp_split(str, pattern[, max_split])
```

## Parameters

`str`: required, the string to split, which must evaluate to a `VARCHAR` value.

`pattern`: required, the regexp expression pattern used to split, which must evaluate to a `VARCHAR` value.

`max_split`: optional, the number of maximum elements in the output value, which must evaluate to an `INT` value.

## Return value

Returns an `ARRAY<VARCHAR>` value.

## Examples

```Plain Text
mysql> select regexp_split('StarRocks', '');
+---------------------------------------+
| regexp_split('StarRocks', '')         |
+---------------------------------------+
| ["S","t","a","r","R","o","c","k","s"] |
+---------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]');
+-----------------------------------+
| regexp_split('StarRocks', '[SR]') |
+-----------------------------------+
| ["","tar","ocks"]                 |
+-----------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 1);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["StarRocks"]                        |
+--------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 2);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["","tarRocks"]                      |
+--------------------------------------+
```
