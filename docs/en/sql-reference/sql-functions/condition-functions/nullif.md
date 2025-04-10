---
displayed_sidebar: docs
---

# nullif

## Description

Returns NULL if `expr1` is equal to `expr2`. Otherwise, returns `expr1`.

## Syntax

```Haskell
nullif(expr1,expr2);
```

## Parameters

`expr1` and `expr2` must be compatible in data type.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+

mysql> select nullif(1,1);
+--------------+
| nullif(1, 1) |
+--------------+
|         NULL |
+--------------+
```
