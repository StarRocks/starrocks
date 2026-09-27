---
displayed_sidebar: docs
description: "Uses the BLAKE3 digest algorithm to hash a string into a 256-bit hexadecimal string."
---

# blake3

Uses the BLAKE3 digest algorithm to hash a string into a 256-bit hexadecimal string.

BLAKE3 is a modern cryptographic hash function that is significantly faster than MD5, SHA-1, SHA-2, and SHA-3, while providing strong security guarantees. It produces a 256-bit (32-byte) digest.

## Syntax

```Haskell
BLAKE3(str);
```

## Parameters

`str`: the string to hash. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned. An empty input string returns the BLAKE3 digest of the zero-length message (`af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262`).

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select blake3('l');
+------------------------------------------------------------------+
| blake3('l')                                                      |
+------------------------------------------------------------------+
| b9f63c83975dadee9b4dc3faeeb669ddef34bbe12e3a0626183166982681d81c |
+------------------------------------------------------------------+
1 row in set (0.01 sec)
```
