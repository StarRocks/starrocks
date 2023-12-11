---
displayed_sidebar: "English"
---

# from_binary

## Description

Converts a binary value to a VARCHAR string based on the specified binary format (`binary_type`). The following binary formats are supported: `hex`, `encode64`, and `utf8`. If no `binary_type` is specified, `hex` is the default.

## Syntax

```Haskell
from_binary(binary[, binary_type])
```

## Parameters

- `binary`: the input binary to convert, required.

- `binary_type`: the binary format for conversion, optional.

  - `hex` (default): `from_binary` uses the `hex` method to encode the input binary to a VARCHAR string.
  - `encode64`: `from_binary` uses the `base64` method to encode the input binary to a VARCHAR string.
  - `utf8`: `from_binary` converts the input binary to a VARCHAR string without any transformation.

## Return value

Returns a VARCHAR string.

## Examples

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
- [BINARY/VARBINARY data type](../../sql-statements/data-types/BINARY.md)
