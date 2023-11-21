---
displayed_sidebar: "English"
---

# sha2

Calculates the SHA-224 hash value, SHA-256 hash value, SHA-384 hash value, or SHA-512 hash value of a string.

## Syntax

```Haskell
VARCHAR sha2(VARCHAR expr, INT hash_length)
```

## Parameters

- `Expr`: the string whose value you want to calculate.
- `hash_length`: the length of a hash value. The value of this parameter can be 224, 256, 384, 512, or 0. The value 0 is equivalent to 256. If you set this parameter to any other value, this function returns `NULL`.

## Return value

Returns a hash value of the VARCHAR type. If any of the two input parameters is `Null`, `Null` is returned.

## Examples

```Plain Text
mysql> select sha2('abc',224);

+----------------------------------------------------------+

| sha2('abc', 224)                                         |

+----------------------------------------------------------+

| 23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7 |

+----------------------------------------------------------+

1 row in set (0.01 sec)



mysql> select sha2('abc', 384);

+--------------------------------------------------------------------------------------------------+

| sha2('abc', 384)                                                                                 |

+--------------------------------------------------------------------------------------------------+

| cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7 |

+--------------------------------------------------------------------------------------------------+

1 row in set (0.00 sec)



mysql> select sha2('abc', 1),sha2('abc', null), sha2(null, 384);

+----------------+-------------------+-----------------+

| sha2('abc', 1) | sha2('abc', NULL) | sha2(NULL, 384) |

+----------------+-------------------+-----------------+

| NULL           | NULL              | NULL            |

+----------------+-------------------+-----------------+

1 row in set (0.01 sec)
```

## Keywords

SHA2, ENCRYPTION
