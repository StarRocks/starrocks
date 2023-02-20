# exp,dexp

## Description

Returns the value of e raised to the power of `x`. This function is called natural logarithms function.

## Syntax

```SQL
EXP(x);
```

## Parameters

`x`: the power number. DOUBLE is supported.

## Return value

Returns a value of the DOUBLE data type.

## Examples

Return e raised to the power of 3.14:

```Plaintext
mysql> select exp(3.14);
+--------------------+
| exp(3.14)          |
+--------------------+
| 23.103866858722185 |
+--------------------+
1 row in set (0.01 sec)
```
