# fmod

## Description

Returns the floating point remainder of the division ( `dividend`/`divisor` ). It is a modulo function.

## Syntax

```SQL
fmod(dividend,devisor);
```

## Parameters

- `dividend`:  DOUBLE or FLOAT is supported.

- `devisor`: DOUBLE or FLOAT is supported.

> **Note**
>
> The data type of `devisor` needs to be the same as the data type of `dividend`. Otherwise, StarRocks performs implicit conversion to convert the data type.

## Return value

The data type and sign of output need to be the same as the data type and sign of `dividend`. If `divisor` is `0`, `NULL` is returned.

## Examples

```Plaintext
mysql> select fmod(3.14,3.14);
+------------------+
| fmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+

mysql> select fmod(11.5,3);
+---------------+
| fmod(11.5, 3) |
+---------------+
|           2.5 |
+---------------+

mysql> select fmod(3,6);
+------------+
| fmod(3, 6) |
+------------+
|          3 |
+------------+

mysql> select fmod(3,0);
+------------+
| fmod(3, 0) |
+------------+
|       NULL |
+------------+
```
