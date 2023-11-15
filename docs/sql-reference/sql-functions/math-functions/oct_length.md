# oct

## Description

Transforms the input `arg` into a octal value.

## Syntax

```Shell
oct(arg)
```

## Parameter

`arg`: the input you want to transform into a octal value. It supports the BIGINT data type.

## Return value

Returns a value of the VARCHAR data type.

## Examples

```Plain
mysql> select oct(11);
+--------+
| oct(11) |
+--------+
| 13     |
+--------+
1 row in set (0.02 sec)
```
