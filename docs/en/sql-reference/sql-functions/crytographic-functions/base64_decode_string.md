---
displayed_sidebar: "English"
---

# base64_decode_string

## Description

This function is the same as [from_base64](from_base64.md).
Decodes a Base64-encoded string. and an inverse of [to_base64](to_base64.md).

This function is supported from v3.0.

## Syntax

```Haskell
base64_decode_string(str);
```

## Parameters

`str`: the string to decode. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL or an invalid Base64 string, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text

mysql> select base64_decode_string(to_base64("Hello StarRocks"));
+----------------------------------------------------+
| base64_decode_string(to_base64('Hello StarRocks')) |
+----------------------------------------------------+
| Hello StarRocks                                    |
+----------------------------------------------------+
```
