# to_binary

## Description

Translate a varchar column to binary type according to the input's binary type, the binary type only supports `hex`, `encode64` and `utf8`; if no `binary_type` is set, `hex` is the  default.

## Syntax

```Haskell
to_bianry(str, [binary_type])
```

## Parameters

`str`: the input string(varchar type) to be converted.
`binary_type`:

- `hex`(default type): `to_binary` will assume the input string is a hex string in which all chars must be in '0123456789abcdef', otherwise empty is appended(exceptions will not be thrown) if the input string is not valid. `to_binary` will convert the input string into binary directly, eg "abab" string will be converted to x'abab', the input string is case-insensitive.
- `encode64`: `to_binary` will assume the input string is a base64-encoded string, otherwise empty is appended (exceptions will be not thrown). `to_binary` will decode the base64-encoded string as the binary result, eg "YWJhYg==" string will be converted to x'abab' binary.
- `utf8`:  `to_binary` will convert the input as a binary type without any transformations.

## Return value

Returns a binary value of the converted input string with the defined binary type.

## Examples

Use this function to shift numeric values.

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
```

## References

- [from_binary](from_binary.md)
