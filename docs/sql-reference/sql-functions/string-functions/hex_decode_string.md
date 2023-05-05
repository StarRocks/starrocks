# hex_decode_string

## Description

This function performs the opposite operation of [hex()](hex.md).

It interprets each pair of hexadecimal digits in the input string as a number and converts it to the byte represented by the number. The return value is a binary string.

This function is supported from v3.0.

## Syntax

```Haskell
hex_decode_string(str);
```

## Parameters

`str`: the string to convert. The supported data type is VARCHAR. An empty string is returned if any of the following situations occur:

- The length of the string is 0 or the number of characters in the string is an odd number.
- The string contains characters other than `[0-9]`, `[a-z]`, and `[A-Z]`.

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select hex_decode_string(hex("Hello StarRocks"));
+-------------------------------------------+
| hex_decode_string(hex('Hello StarRocks')) |
+-------------------------------------------+
| Hello StarRocks                           |
+-------------------------------------------+
```

## Keywords

HEX_DECODE_STRING
