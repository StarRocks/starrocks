---
displayed_sidebar: docs
---

# cardinality

## Description

Returns the number of elements in an array. The result type is INT. If the input parameter is NULL, the result is also NULL. Null elements are counted in the length.

It is the alias of [array_length()](array_length.md).

This function is supported from v3.0 onwards.

## Syntax

```Haskell
INT cardinality(any_array)
```

## Parameters

`any_array`: the ARRAY value from which you want to retrieve the number of elements.

## Return value

Returns an INT value.

## Examples

```plain text
mysql> select cardinality([1,2,3]);
+-----------------------+
|  cardinality([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select cardinality([1,2,3,null]);
+------------------------------+
| cardinality([1, 2, 3, NULL]) |
+------------------------------+
|                            4 |
+------------------------------+

mysql> select cardinality([[1,2], [3,4]]);
+-----------------------------+
|  cardinality([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keyword

CARDINALITY, ARRAY_LENGTH, ARRAY
