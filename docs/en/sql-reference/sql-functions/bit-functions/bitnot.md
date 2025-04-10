---
displayed_sidebar: docs
---

# bitnot

## Description

Returns the bitwise negation of a numeric expression.

## Syntax

```Haskell
BITNOT(x);
```

## Parameters

`x`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

## Return value

The return value has the same type as `x`. If any value is NULL, the result is NULL.

## Examples

```Plain Text
mysql> select bitnot(3);
+-----------+
| bitnot(3) |
+-----------+
|        -4 |
+-----------+
```
