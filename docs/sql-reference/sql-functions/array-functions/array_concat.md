# array_concat

## Description

Concatenates multiple arrays into one array that contains all the elements in the arrays.

We recommend that elements in the arrays to concatenate are of the same type. StarRocks also supports concatenating two arrays of different types. However, we did not fully test all the combinations of data types.

Nulls are processed as normal values.

## Syntax

```SQL
array_concat(input0, input1, ...)
```

## Parameters

`input`: one or more arrays that you want to concatenate. Specify arrays in the `(input0, input1, ...)` format.

## Return value

Returns an array that contains all the elements held in the arrays that are specified by the `input` parameter. The elements of the returned array are of the same data type as the elements of the specified arrays. Additionally, the elements of the returned array follow the order of the specified arrays and their elements.

## Examples

Example 1: Concatenate arrays that contain numeric elements.

```Plain%20Text
select array_concat([57.73,97.32,128.55,null,324.2], [3], [5]) as res;

+-------------------------------------+

| res                                 |

+-------------------------------------+

| [57.73,97.32,128.55,null,324.2,3,5] |

+-------------------------------------+
```

Example 2: Concatenate arrays that contain string elements.

```Plain%20Text
select array_concat(["sql","storage","execute"], ["Query"], ["Vectorized", "cbo"]);

+----------------------------------------------------------------------------+

| array_concat(['sql','storage','execute'], ['Query'], ['Vectorized','cbo']) |

+----------------------------------------------------------------------------+

| ["sql","storage","execute","Query","Vectorized","cbo"]                     |

+----------------------------------------------------------------------------+
```

Example 3: Concatenate two arrays of different types.

```Plain%20Text
select array_concat([57,65], ["pear","apple"]);
+-------------------------------------------+
| array_concat([57, 65], ['pear', 'apple']) |
+-------------------------------------------+
| ["57","65","pear","apple"]                |
+-------------------------------------------+
```

Example 4: Process nulls as normal values.

```Plain%20Text
select array_concat(["sql",null], [null], ["Vectorized", null]);

+---------------------------------------------------------+

| array_concat(['sql',NULL], [NULL], ['Vectorized',NULL]) |

+---------------------------------------------------------+

| ["sql",null,null,"Vectorized",null]                     |

+---------------------------------------------------------+
```
