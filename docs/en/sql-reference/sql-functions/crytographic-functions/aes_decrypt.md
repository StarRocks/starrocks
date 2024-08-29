---
displayed_sidebar: docs
---

# aes_decrypt

## Description

Uses the AES_128_ECB algorithm to decrypt a string and returns a binary string.

AES is short for advanced encryption standard and ECB is short for electronic code book. The key used to encrypt the string is a 128-bit string.

## Syntax

```Haskell
aes_decrypt(str,key_str);
```

## Parameters

`str`: the string to decrypt. It must be of the VARCHAR type.

`key_str`: the key used to encrypt `str`. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is invalid, NULL is returned.

## Examples

Decode a Base64 string and use this function to decrypt the decoded string into the original string.

```Plain Text
mysql> select AES_DECRYPT(from_base64('uv/Lhzm74syo8JlfWarwKA==  '),'F3229A0B371ED2D9441B830D21A390C3');
+--------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('uv/Lhzm74syo8JlfWarwKA==  '), 'F3229A0B371ED2D9441B830D21A390C3') |
+--------------------------------------------------------------------------------------------+
| starrocks                                                                                  |
+--------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
