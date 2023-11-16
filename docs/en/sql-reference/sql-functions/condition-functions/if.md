---
displayed_sidebar: "English"
---

# if

## Description

If `expr1` evaluates to TRUE, returns `expr2`. Otherwise, returns `expr3`.

## Syntax

```Haskell
if(expr1,expr2,expr3);
```

## Parameters

`expr1`: the condition. It must be a BOOLEAN value.

`expr2` and `expr3` must be compatible in data type.

## Return value

The return value has the same type as `expr2`.

## Examples

```Plain Text
mysql> select if(true,1,2);
+----------------+
| if(TRUE, 1, 2) |
+----------------+
|              1 |
+----------------+

mysql> select if(false,2.14,2);
+--------------------+
| if(FALSE, 2.14, 2) |
+--------------------+
|               2.00 |
+--------------------+
```
