# array_contains_seq

## Description

Checks whether all the elements of array2 appear in array1 in the same exact order. Therefore, the function will return 1, if and only if array1 = prefix + array2 + suffix.

## Syntax

~~~Haskell
BOOLEAN array_contains_all(arr1, arr2)
~~~

## Parameters

`arr`: the two arrays to compare. This syntax checks whether `arr2` is a subset of `arr1` and in the same exact order.

The data types of elements in the two arrays must be the same. For the data types of array elements supported by StarRocks, see [ARRAY](../../../sql-reference/sql-statements/data-types/Array.md).

## Return value

Returns a value of the BOOLEAN type.

1 is returned if `arr2` is a subset of `arr1`. Otherwise, 0 is returned.

## Examples

Returns a value of the BOOLEAN type.

```Plaintext
MySQL [(none)]> select array_contains_seq([1,2,3,4], [1,2,3]);
+---------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [1, 2, 3]) |
+---------------------------------------------+
|                                           1 |
+---------------------------------------------+
```

```Plaintext
MySQL [(none)]> select array_contains_seq([1,2,3,4], [3,2]);
+------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [3, 2]) |
+------------------------------------------+
|                                        0 |
+------------------------------------------+
1 row in set (0.18 sec)
```
