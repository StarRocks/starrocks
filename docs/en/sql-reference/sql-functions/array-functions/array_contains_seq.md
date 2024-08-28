---
displayed_sidebar: docs
---

# array_contains_seq

## Description

Checks whether all the elements of `arr2` appear in `arr1` in the same exact order, that is, whether `arr2` is a subset of `arr1` and the elements in `arr2` observe the same exact order as elements in `arr1`. If yes, this function will return 1.

For example:

- `select array_contains_seq([1,2,3,4], [1,2,3]);` returns 1.
- `select array_contains_seq([1,2,3,4], [4,3]);` returns 0.

This function is supported from v3.3 onwards.

## Syntax

~~~Haskell
BOOLEAN array_contains_all(arr1, arr2)
~~~

## Parameters

`arr`: the two arrays to compare. This syntax checks whether `arr2` is a subset of `arr1` and in the same exact order.

The data types of elements in the two arrays must be the same. For the data types of array elements supported by StarRocks, see [ARRAY](../../../sql-reference/data-types/semi_structured/Array.md).

## Return value

Returns a value of the BOOLEAN type.

- `1` is returned if `arr2` is a subset of `arr1` and the elements in `arr2` observe the same order as those in `arr1`. Otherwise, `0` is returned.
- An empty array is a subset of any array. Therefore, `1` is returned if `arr2` is empty but `arr1` is a valid array.
- NULL is returned if any input array is NULL.
- Nulls in arrays are processed as normal values. For example, `SELECT array_contains_seq([1, 2, NULL, 3, 4], [2,3])` will return 0. However, `SELECT array_contains_seq([1, 2, NULL, 3, 4], [2,NULL,3])` will return 1.

## Examples

```Plaintext

MySQL > select array_contains_seq([1,2,3,4], [1,2,3]);
+---------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [1, 2, 3]) |
+---------------------------------------------+
|                                           1 |
+---------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4], [3,2]);
+------------------------------------------+
| array_contains_seq([1, 2, 3, 4], [3, 2]) |
+------------------------------------------+
|                                        0 |
+------------------------------------------+

MySQL > select array_contains_seq([1, 2, NULL, 3, 4], ['a']);
+-----------------------------------------------+
| array_contains_all([1, 2, NULL, 3, 4], ['a']) |
+-----------------------------------------------+
|                                             0 |
+-----------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4,null], null);
+------------------------------------------+
| array_contains([1, 2, 3, 4, NULL], NULL) |
+------------------------------------------+
|                                     NULL |
+------------------------------------------+

MySQL > select array_contains_seq([1,2,3,4], []);
+--------------------------------------+
| array_contains_seq([1, 2, 3, 4], []) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```
