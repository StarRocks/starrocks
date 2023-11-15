# xx_hash3_64

## Description

Returns the 64-bit xxhash3 hash value of the input string.

## Syntax

```Haskell
BIGINT XX_HASH3_64(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select xx_hash3_64(null);
+-------------------+
| xx_hash3_64(NULL) |
+-------------------+
|              NULL |
+-------------------+

MySQL > select xx_hash3_64("hello");
+----------------------+
| xx_hash3_64('hello') |
+----------------------+
| -7685981735718036227 |
+----------------------+

MySQL > select xx_hash3_64("hello", "world");
+-------------------------------+
| xx_hash3_64('hello', 'world') |
+-------------------------------+
|           7001965798170371843 |
+-------------------------------+
```

## keyword

XX_HASH3_64,HASH
