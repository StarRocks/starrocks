# hex_decode_binary

## Description

This function decodes a hex encoded string to a binary


## Syntax

```Haskell
hex_decode_binary(str);
```

## Parameters

`str`: the string to convert. The supported data type is VARCHAR. An empty binary is returned if any of the following situations occur:

- The length of the string is 0 or the number of characters in the string is an odd number.
- The string contains characters other than `[0-9]`, `[a-z]`, and `[A-Z]`.

## Return value

Returns a value of the BINARY type.

## Examples

```Plain Text
mysql> select hex(hex_decode_binary(hex("Hello StarRocks")));
+------------------------------------------------+
| hex(hex_decode_binary(hex('Hello StarRocks'))) |
+------------------------------------------------+
| 48656C6C6F2053746172526F636B73                 |
+------------------------------------------------+

mysql> select hex_decode_binary(NULL);
+--------------------------------------------------+
| hex_decode_binary(NULL)                          |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
```

## Keywords

HEX_DECODE_BINARY
