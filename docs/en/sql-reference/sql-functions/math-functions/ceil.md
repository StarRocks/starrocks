---
displayed_sidebar: docs
---

# ceil, dceil

## Description

Returns values from the input `arg` rounded to the nearest equal or larger integer.

## Syntax

```Shell
ceil(arg)
```

## Parameter

`arg` supports the DOUBLE data type.

## Return value

Returns a value of the BIGINT data type.

## Examples

```Plain
mysql> select ceil(3.14);
+------------+
| ceil(3.14) |
+------------+
|          4 |
+------------+
1 row in set (0.15 sec)
```
