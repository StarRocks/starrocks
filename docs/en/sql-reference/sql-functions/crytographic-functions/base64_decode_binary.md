---
displayed_sidebar: docs
---

# base64_decode_binary

## Description

Decodes a Base64-encoded string and return a BINARY.

This function is supported from v3.0.

## Syntax

```Haskell
base64_decode_binary(str);
```

## Parameters

`str`: the string to decode. It must be of the VARCHAR type.

## Return value

Returns a value of the VARBINARY type. If the input is NULL or an invalid Base64 string, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select hex(base64_decode_binary(to_base64("Hello StarRocks")));
+---------------------------------------------------------+
| hex(base64_decode_binary(to_base64('Hello StarRocks'))) |
+---------------------------------------------------------+
| 48656C6C6F2053746172526F636B73                          |
+---------------------------------------------------------+

mysql> select base64_decode_binary(NULL);
+--------------------------------------------------------+
| base64_decode_binary(NULL)                             |
+--------------------------------------------------------+
| NULL                                                   |
+--------------------------------------------------------+
```
