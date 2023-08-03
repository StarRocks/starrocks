# array_length

## Description

Returns the number of elements in an array. The result type is INT. If the input parameter is NULL, the result is also NULL. Null elements are counted in the length.

It has an alias [cardinality()](cardinality.md).

## Syntax

```Haskell
INT array_length(any_array)
```

## Parameters

`any_array`: the ARRAY value from which you want to retrieve the number of elements.

## Return value

Returns an INT value.

## Examples

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+

mysql> select array_length([1,2,3,null]);
+-------------------------------+
| array_length([1, 2, 3, NULL]) |
+-------------------------------+
|                             4 |
+-------------------------------+

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
```

## keywords

ARRAY_LENGTH,ARRAY,CARDINALITY
