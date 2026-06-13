---
displayed_sidebar: docs
---

# blake3

Uses the BLAKE3 digest algorithm to hash a string into a 256-bit hexadecimal string. Every 32 bits are separated by a space.

BLAKE3 is a modern cryptographic hash function that is significantly faster than MD5, SHA-1, SHA-2, and SHA-3, while providing strong security guarantees. It produces a 256-bit (32-byte) digest.

## Syntax

```Haskell
BLAKE3(str);
```

## Parameters

`str`: the string to hash. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select blake3('l');
+-------------------------------------------------------------------------+
| blake3('l')                                                             |
+-------------------------------------------------------------------------+
| b9f63c83 975dadee 9b4dc3fa eeb669dd ef34bbe1 2e3a0626 18316698 2681d81c |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
