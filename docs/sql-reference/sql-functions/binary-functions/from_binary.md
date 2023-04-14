# from_binary

## Description

Translate a binary type to a varchar type according to the input's binary type, the binary type only supports `hex`, `encode64` and `utf8`; if no `binary_type` is set, `hex` is the  default.

## Syntax

```Haskell
from_bianry(binary, [binary_type])
```

## Parameters

`binary`: the input binary to be converted.

`binary_type`:

- `hex`(default type): `from_binary` will assume the input binary will use the `hex` method to encode the input binary to `varchar`.
- `encode64`: `from_binary` will use the `base64` method to encode the input binary to `varchar`.
- `utf8`:  `from_binary` will convert the input binary as a varchar type without any transformations.

## Return value

Returns a varchar value of the converted input binary with the defined varchar type.

## Examples

Use this function to shift numeric values.

```Plain
mysql> select from_binary(to_binary('ABAB', 'hex'), 'hex');
+----------------------------------------------+
| from_binary(to_binary('ABAB', 'hex'), 'hex') |
+----------------------------------------------+
| ABAB                                         |
+----------------------------------------------+
1 row in set (0.02 sec)

mysql> select from_base64(from_binary(to_binary('U1RBUlJPQ0tT', 'encode64'), 'encode64'));
+-----------------------------------------------------------------------------+
| from_base64(from_binary(to_binary('U1RBUlJPQ0tT', 'encode64'), 'encode64')) |
+-----------------------------------------------------------------------------+
| STARROCKS                                                                   |
+-----------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select from_binary(to_binary('STARROCKS', 'utf8'), 'utf8');
+-----------------------------------------------------+
| from_binary(to_binary('STARROCKS', 'utf8'), 'utf8') |
+-----------------------------------------------------+
| STARROCKS                                           |
+-----------------------------------------------------+
1 row in set (0.01 sec)

```

## References

- [to_binary](to_binary.md)
