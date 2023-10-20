# reverse

## Description

Reverses the order in which the elements of a string or array are arranged.

## Syntax

```Haskell
reverse(param)
```

## Parameters

`param`: the string or array that you want to reverse. The supported data types are VARCHAR, CHAR, and ARRAY. If you specify an array, the array must be a one-dimensional array whose elements are not of the DECIMAL data type.

## Return value

Returns a value of the same data type as the object that you specify in the `param` parameter.

## Examples

Example 1: Reverse a string.

```Plain_Text
MySQL > SELECT reverse('hello');

+------------------+

| REVERSE('hello') |

+------------------+

| olleh            |

+------------------+

```

Example 2: Reverse an array.

```Plain_Text
MYSQL> SELECT REVERSE([4,1,5,8]);

+--------------------+

| REVERSE([4,1,5,8]) |

+--------------------+

| [8,5,1,4]          |

+--------------------+
```
