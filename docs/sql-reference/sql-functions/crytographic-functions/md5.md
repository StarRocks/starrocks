# md5

Calculate the 128-bit checksum of a string.

## Syntax

```Apache
VARCHAR md5(VARCHAR expr)
```

## Parameters

`Expr`: the string to be calculated.

## Return value

Returns a checksum, which is a string of 32 hexadecimal numbers.

## Examples

```Lua
mysql> select md5('abc');

+----------------------------------+

| md5('abc')                       |

+----------------------------------+

| 900150983cd24fb0d6963f7d28e17f72 |

+----------------------------------+

1 row in set (0.01 sec)
```

## Keywords

MD5, ENCRYPTION
