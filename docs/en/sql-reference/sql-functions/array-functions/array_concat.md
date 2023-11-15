# array_concat

## Description

Concatenates multiple arrays into one array.

## Syntax

```SQL
array_concat(input0, input1, ...)
```

## Parameters

`input`: one or more arrays that you want to concatenate. Specify arrays in the `(input0, input1, ...)` format and make sure that the elements of the arrays are of the same data type.

## Return value

Returns an array. The array that is returned consists of all elements held in the arrays that are specified by the `input` parameter. The elements of the returned array are of the same data type as the elements of the specified arrays. Additionally, the elements of the returned array follow the order of the specified arrays and their elements.

## Examples

Example 1:

```Plain_Text
mysql> select array_concat([57.73,97.32,128.55,null,324.2], [3], [5]) as res;

+-------------------------------------+

| res                                 |

+-------------------------------------+

| [57.73,97.32,128.55,null,324.2,3,5] |

+-------------------------------------+
```

Example 2:

```Plain_Text
mysql> select array_concat(["sql","storage","execute"], ["Query"], ["Vectorized", "cbo"]);

+----------------------------------------------------------------------------+

| array_concat(['sql','storage','execute'], ['Query'], ['Vectorized','cbo']) |

+----------------------------------------------------------------------------+

| ["sql","storage","execute","Query","Vectorized","cbo"]                     |

+----------------------------------------------------------------------------+
```

Example 3:

```Plain_Text
mysql> select array_concat(["sql",null], [null], ["Vectorized", null]);

+---------------------------------------------------------+

| array_concat(['sql',NULL], [NULL], ['Vectorized',NULL]) |

+---------------------------------------------------------+

| ["sql",null,null,"Vectorized",null]                     |

+---------------------------------------------------------+
```
