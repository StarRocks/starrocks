---
displayed_sidebar: "English"
---

# reverse

## Description

Reverses a string or array. Returns a string or array with the characters in the string or array elements in reverse order.

## Syntax

```Haskell
reverse(param)
```

## Parameters

`param`: the string or array to reverse. It can be of the VARCHAR, CHAR, or ARRAY type.

Currently, this function supports only one-dimensional arrays and the array elements cannot be of the DECIMAL type. This function supports the following types of array elements: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, DECIMALV2, DATETIME, DATE, and JSON. **JSON is supported from 2.5.**

## Return value

The return type is the same as `param`.

## Examples

Example 1: Reverse a string.

```Plain Text
MySQL > SELECT REVERSE('hello');
+------------------+
| REVERSE('hello') |
+------------------+
| olleh            |
+------------------+
1 row in set (0.00 sec)
```

Example 2: Reverse an array.

```Plain Text
MYSQL> SELECT REVERSE([4,1,5,8]);
+--------------------+
| REVERSE([4,1,5,8]) |
+--------------------+
| [8,5,1,4]          |
+--------------------+
```
