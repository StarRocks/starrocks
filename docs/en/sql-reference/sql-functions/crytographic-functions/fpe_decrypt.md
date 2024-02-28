---
displayed_sidebar: "English"
---

# fpe_decrypt

## Description

Decrypts strings encrypted by `fpe_encrypt`, the reverse function of [fpe_encrypt](fpe_encrypt.md).


## Syntax

```Haskell
fpe_decrypt(str, key);
```

## Parameters

`str`: The string to decrypt. It must be of the VARCHAR type.

`key`: The key string used to decrypt str, with a length limit of 16, 24, or 32,. It must be of the VARCHAR type.

## Return value

he data type of the return value is VARCHAR. Returns NULL if the input is NULL. Returns an error if the input is empty.

This function only accepts 2 parameters; providing more will result in an error.

## Examples

```Plain Text
mysql> select fpe_decrypt('17705785.238108909558021', 'abcdefghijk12345abcdefghijk12345');
+---------------------------------------------------------------------------------+
| fpe_decrypt('17705785.238108909558021', 'abcdefghijk12345abcdefghijk12345') |
+---------------------------------------------------------------------------------+
| 9237923.347343                                                                  |
+---------------------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select fpe_decrypt('14788209.02880443556771', 'abcdefghijk12345abcdefghijk12345');
+--------------------------------------------------------------------------------+
| fpe_decrypt('14788209.02880443556771', 'abcdefghijk12345abcdefghijk12345') |
+--------------------------------------------------------------------------------+
| 9302923.04832                                                                  |
+--------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
