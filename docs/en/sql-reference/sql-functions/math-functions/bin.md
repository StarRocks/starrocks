---
displayed_sidebar: docs
---

# bin

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Transforms the input `arg` into a binary.

## Syntax

```Shell
bin(arg)
```

## Parameter

`arg`: the input you want to transform into a binary. It supports the BIGINT data type.

## Return value

Returns a value of the VARCHAR data type.

## Examples

```Plain
mysql> select bin(3);
+--------+
| bin(3) |
+--------+
| 11     |
+--------+
1 row in set (0.02 sec)
```
