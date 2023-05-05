# to_binary

## Description

Converts a VARCHAR string to a binary value based on the specified binary format (`binary_type`) of the input string. The following binary formats are supported: `hex`, `encode64`, and `utf8`. If no `binary_type` is specified, `hex` is the default.

## Syntax

```Haskell
to_binary(str[, binary_type])
```

## Parameters

- `str`: the string to convert, required. It must be a VARCHAR string.
- `binary_type`: the binary format for conversion, optional.

  - `hex`(default): `to_binary` assumes the input string is a hex string in which all characters are in '0123456789abcdef'. If the input string is not valid, an empty binary is returned (exceptions will not be thrown). `to_binary` will convert the input string into binary directly. For example, `"abab"` will be converted to `x'abab'`. The input string is not case-sensitive.
  - `encode64`: `to_binary` assumes the input string is a base64-encoded string . If the input string is not valid, an empty binary is returned (exceptions will not be thrown). `to_binary` will decode the base64-encoded string as the binary result. For example, `"YWJhYg=="` will be converted to `x'abab'`.
  - `utf8`: `to_binary` converts the input string as a binary value without any transformation.

## Return value

Returns a VARCHAR value.

## Examples

The following examples assume that the `--binary-as-hex` option is enabled when you access StarRocks from your MySQL client. This way, binary data is displayed using hexadecimal notation.

```Plain
mysql> select to_binary('ABAB', 'hex');
+----------------------------------------------------+
| to_binary('ABAB', 'hex')                           |
+----------------------------------------------------+
| 0xABAB                                             |
+----------------------------------------------------+
1 row in set (0.01 sec)

mysql> select to_binary('U1RBUlJPQ0tT', 'encode64');
+------------------------------------------------------------------------------+
| to_binary('U1RBUlJPQ0tT', 'encode64')                                        |
+------------------------------------------------------------------------------+
| 0x53544152524F434B53                                                         |
+------------------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select to_binary('STARROCKS', 'utf8');
+----------------------------------------------------------------+
| to_binary('STARROCKS', 'utf8')                                 |
+----------------------------------------------------------------+
| 0x53544152524F434B53                                           |
+----------------------------------------------------------------+
1 row in set (0.00 sec)

-- The input string does not match the binary format and an empty binary is returned.

mysql> select to_binary('U1RBUlJPQ0tT', 'hex');
+--------------------------------------------------------------------+
| to_binary('U1RBUlJPQ0tT', 'hex')                                   |
+--------------------------------------------------------------------+
| 0x                                                                 |
+--------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## References

- [from_binary](from_binary.md)
- [BINARY/VARBINARY data type](../../sql-statements/data-types/BINARY.md)
