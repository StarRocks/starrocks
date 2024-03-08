---
displayed_sidebar: "English"
---

# fpe_encrypt

## Description

Encrypts the integer and decimal parts of the string `str` using the Format-Preserving Encryption (FPE) FF1 method, combining the encrypted results into a new encrypted string. The encryption process is as follows:

1. Encrypt the integer part of `str` with `fpe_ff1_encrypt`, prefixing the result with a placeholder "1".
2. Multiply the decimal part of `str` by 100000000000000, encrypt it with `fpe_ff1_encrypt`, and append a "1" as a placeholder at the end.

This process ensures the format of each part is preserved during encryption, clearly differentiating between the integer and decimal parts in the final encrypted string.

the reverse function of [fpe_decrypt](fpe_decrypt.md).


## Syntax

```Haskell
fpe_encrypt(str, key);
```

## Parameters

`str`: The string is decimal in string format to encrypt, like "-123.456". It must be of the VARCHAR type.

`key`: The key string used to encrypt str, with a length limit of 16, 24, or 32,. It must be of the VARCHAR type.

## Return value

The return data type is VARCHAR. Returns NULL if the input is NULL. Errors are returned for empty inputs.

This function only accepts 2 parameters; more inputs will result in an error.

## Examples

```Plain Text
mysql> select fpe_encrypt('9237923.347343', 'abcdefghijk12345abcdefghijk12345');
+-----------------------------------------------------------------------+
| fpe_encrypt('9237923.347343', 'abcdefghijk12345abcdefghijk12345') |
+-----------------------------------------------------------------------+
| 17705785.238108909558021                                              |
+-----------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select fpe_encrypt('9302923.04832', 'abcdefghijk12345abcdefghijk12345');
+----------------------------------------------------------------------+
| fpe_encrypt('9302923.04832', 'abcdefghijk12345abcdefghijk12345') |
+----------------------------------------------------------------------+
| 14788209.02880443556771                                              |
+----------------------------------------------------------------------+
1 row in set (0.00 sec)
```