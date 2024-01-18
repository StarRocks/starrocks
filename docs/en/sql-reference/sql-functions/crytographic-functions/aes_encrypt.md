---
displayed_sidebar: "English"
---

# aes_encrypt

## Description

Uses the AES_128_ECB algorithm to encrypt a string and returns a binary string.

AES is short for advanced encryption standard and ECB is short for electronic code book. The key used to encrypt the string is a 128-bit string.

## Syntax

```Haskell
aes_encrypt(str,key_str);
```

## Parameters

`str`: the string to encrypt. It must be of the VARCHAR type.

`key_str`: the key used to encrypt `str`. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned.

## Examples

Use this function to AES encrypt `starrocks` and convert the encrypted string into a Base64-encoded string.

```Plain Text
mysql> select to_base64(AES_ENCRYPT('starrocks','F3229A0B371ED2D9441B830D21A390C3'));
+-------------------------------------------------------------------------+
| to_base64(aes_encrypt('starrocks', 'F3229A0B371ED2D9441B830D21A390C3')) |
+-------------------------------------------------------------------------+
| uv/Lhzm74syo8JlfWarwKA==                                                |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
