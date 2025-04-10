---
displayed_sidebar: docs
---

# log2

## Description

Calculates the base 2 logarithm of a number.

## Syntax

```SQL
log2(arg)
```

## Parameters

- `arg`: The value whose logarithm you want to calculate. Only DOUBLE data type is supported.

> **NOTE**
>
> StarRocks returns NULL if `arg` is specified as a negative or 0.

## Return value

Returns a value of the DOUBLE data type.

## Example

Example 1: Calculate the base 2 logarithm of 8.

```Plain
mysql> select log2(8);
+---------+
| log2(8) |
+---------+
|       3 |
+---------+
1 row in set (0.00 sec)
```
