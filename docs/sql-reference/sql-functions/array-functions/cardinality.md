# cardinality

## Description

Returns the number of elements in the array. The result type is INT. if the parameter is NULL, the result is also NULL.

It is the alias of [array_length()](array_length.md).

## Syntax

```Haskell
cardinality(any_array)
```

## Examples

```plain text
mysql> select cardinality([1,2,3]);
+-----------------------+
|  cardinality([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)
mysql> select cardinality([[1,2], [3,4]]);
+-----------------------------+
|  cardinality([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keyword

CARDINALITY,ARRAY_LENGTH,ARRAY