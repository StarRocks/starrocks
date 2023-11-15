# oct_length

## Description

Return the length of the octal value for the input `arg`.

## Syntax

```Shell
oct_length(arg)
```

## Parameter

`arg`: the input you want to transform into a octal value. It supports the BIGINT data type.

## Return value

Returns a value of the INT data type.

## Examples

```Plain
mysql> select oct_length(11);
+----------------+
| oct_length(11) |
+----------------+
| 2              |
+----------------+
1 row in set (0.02 sec)
```
