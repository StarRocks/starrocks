---
displayed_sidebar: "English"
---

# log

## Description

Calculates the logarithm of a number to a specified base (or radix). If the base is not specified, this function is equivalent to [ln](../math-functions/ln.md).

## Syntax

```SQL
log([base,] arg)
```

## Parameters

- `base`: Optional. The base. Only DOUBLE data type is supported. If this parameter is not specified, this function is equivalent to [ln](../math-functions/ln.md).

> **NOTE**
>
> StarRocks returns NULL if `base` is specified as a negative, 0, or 1.

- `arg`: The value whose logarithm you want to calculate. Only DOUBLE data type is supported.

> **NOTE**
>
> StarRocks returns NULL if `arg` is specified as a negative or 0.

## Return value

Returns a value of the DOUBLE data type.

## Example

Example 1: Calculate the logarithm of 8 to the base 2.

```Plain
mysql> select log(2,8);
+-----------+
| log(2, 8) |
+-----------+
|         3 |
+-----------+
1 row in set (0.01 sec)
```

Example 2: Calculate the logarithm of 10 to the base *e* (with the base not specified).

```Plain
mysql> select log(10);
+-------------------+
| log(10)           |
+-------------------+
| 2.302585092994046 |
+-------------------+
1 row in set (0.09 sec)
```
