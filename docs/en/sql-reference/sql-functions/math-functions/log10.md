---
displayed_sidebar: docs
---

# log10, dlog10

## Description

Calculates the base 10 logarithm of a number.

## Syntax

```SQL
log10(arg)
```

## Parameters

- `arg`: The value whose logarithm you want to calculate. Only DOUBLE data type is supported.

> **NOTE**
>
> StarRocks returns NULL if `arg` is specified as a negative or 0.

## Return value

Returns a value of the DOUBLE data type.

## Example

Example 1: Calculate the base 10 logarithm of 100.

```Plain
select log10(100);
+------------+
| log10(100) |
+------------+
|          2 |
+------------+
1 row in set (0.02 sec)
```
