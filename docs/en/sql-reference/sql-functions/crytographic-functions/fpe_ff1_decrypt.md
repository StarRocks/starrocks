---
displayed_sidebar: "English"
---

# fpe_ff1_decrypt

## Description

Decrypts a string `str` using the Format-Preserving Encryption (FPE) FF1 method. The reverse function is [fpe_ff1_encrypt](fpe_ff1_encrypt.md).


## Syntax

```Haskell
fpe_ff1_decrypt(str, key, radix);
```

## Parameters

`str`: The string to be decrypted, supports VARCHAR data type.
`key`: The key string used for decryption, length limited to 16, 24, or 32, supports VARCHAR data type.
`radix`: The numeric base for decryption. Supports up to a base of 36. The character set for base 10 is "0123456789", for base 16 is "0123456789abcdef", and for base 36 is "0123456789abcdefghijklmnopqrstuvwxyz". Supports INT data type.

## Return value

The return data type is VARCHAR. Returns NULL if input is NULL. Returns an error if input is empty.

This function only accepts 3 parameters; inputting more will result in an error.

## Examples

```Plain Text
mysql> select fpe_ff1_decrypt('697512', 'abcdefghijk12345abcdefghijk12345', 10);
+-------------------------------------------------------------------+
| fpe_ff1_encrypt('697512', 'abcdefghijk12345abcdefghijk12345', 10) |
+-------------------------------------------------------------------+
| 893892                                                            |
+-------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select fpe_ff1_decrypt(30094485160, 'abcdefghijk12345abcdefghijk12345', 10);
+----------------------------------------------------------------------+
| fpe_ff1_encrypt(30094485160, 'abcdefghijk12345abcdefghijk12345', 10) |
+----------------------------------------------------------------------+
| 42302920232                                                          |
+----------------------------------------------------------------------+
1 row in set (0.00 sec)
```
