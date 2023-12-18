---
displayed_sidebar: "English"
---

# array_difference

## Description

Calculates the difference between every two neighboring elements of an array by subtracting each element from its following element and returns an array that consists of the differences.

## Syntax

```SQL
array_difference(input)
```

## Parameters

`input`: the array for which you want to calculate the differences between every two neighboring elements.

## Return value

Returns an array of the same data type and length as the array that you specify in the `input` parameter.

## Examples

Example 1:

```Plain
mysql> SELECT array_difference([342, 32423, 213, 23432]);
+-----------------------------------------+
| array_difference([342,32423,213,23432]) |
+-----------------------------------------+
| [0,32081,-32210,23219]                  |
+-----------------------------------------+
```

Example 2:

```Plain
mysql> SELECT array_difference([342, 32423, 213, null, 23432]);
+----------------------------------------------+
| array_difference([342,32423,213,NULL,23432]) |
+----------------------------------------------+
| [0,32081,-32210,null,null]                   |
+----------------------------------------------+
```

Example 3:

```Plain
mysql> SELECT array_difference([1.2, 2.3, 3.2, 4324242.55]);
+--------------------------------------------+
| array_difference([1.2,2.3,3.2,4324242.55]) |
+--------------------------------------------+
| [0,1.1,0.9,4324239.35]                     |
+--------------------------------------------+
```

Example 4:

```Plain
mysql> SELECT array_difference([false, true, false]);
+----------------------------------------+
| array_difference([FALSE, TRUE, FALSE]) |
+----------------------------------------+
| [0,1,-1]                               |
+----------------------------------------+
```
