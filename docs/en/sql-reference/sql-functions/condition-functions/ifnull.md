---
displayed_sidebar: "English"
---

# ifnull

## Description

If `expr1` is NULL, returns expr2. If `expr1` is not NULL, returns `expr1`.

## Syntax

```Haskell
ifnull(expr1,expr2);
```

## Parameters

`expr1` and `expr2` must be compatible in data type.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select ifnull(2,4);
+--------------+
| ifnull(2, 4) |
+--------------+
|            2 |
+--------------+

mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.01 sec)
```
