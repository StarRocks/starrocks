---
displayed_sidebar: "English"
---

# xx_hash3_64

## Description

Returns the 64-bit xxhash3 hash value of the input string. xx_hash3_64 has better performance than [murmur_hash3_32](./murmur_hash3_32.md) by using AVX2 instruction and has state-of-art hash quality which is broadly integrated with many software.

This function is supported from v3.2.0.

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

XX_HASH3_64,HASH,xxHash3
