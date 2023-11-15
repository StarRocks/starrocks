# sm3

## Description

Uses the SM3 digest algorithm to encrypt a string into a 256-bit hexadecimal string. Every 32 bits are separated by a space.

Digest algorithms have a broad range of use cases in digital signature, message authentication, and data integrity check. The SM3 algorithm is an enhancement of SHA-256.

## Syntax

```Haskell
SM3(str);
```

## Parameters

`str`: the string to encrypt. It must be of the VARCHAR type.

## Return value

Returns a value of the VARCHAR type. If the input is NULL, NULL is returned. If the input is empty, an error is returned.

This function accepts only one string. More than one input string causes an error.

## Examples

```Plain Text
mysql> select sm3('l');
+-------------------------------------------------------------------------+
| sm3('l')                                                                |
+-------------------------------------------------------------------------+
| 1dec1a7a 94236240 49db411e 2c32c62d c0c93856 8208ac3a 09d395eb 2468b445 |
+-------------------------------------------------------------------------+
1 row in set (0.01 sec)
```
