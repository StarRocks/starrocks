# array_length

## Description

Returns the number of elements in the array. The result type is INT. if the parameter is NULL, the result is also NULL.

## Syntax

```Haskell
array_length(any_array)
```

## Examples

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```

## keyword

ARRAY_LENGTH,ARRAY
