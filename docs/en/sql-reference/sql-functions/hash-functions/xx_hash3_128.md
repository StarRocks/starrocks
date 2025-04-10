---
displayed_sidebar: docs
---

# xx_hash3_128

## Description

Returns the 128-bit xxhash3 hash value of the input string. 

## Syntax

```Haskell
LARGEINT XX_HASH3_128(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select xx_hash3_128(null);
+-------------------+
| xx_hash3_128(NULL) |
+-------------------+
|              NULL |
+-------------------+

MySQL > select xx_hash3_128("hello");
+-----------------------------------------+
| xx_hash3_128('hello')                   |
+-----------------------------------------+
| -98478366302105124680504504609445627880 |
+-----------------------------------------+

MySQL > select xx_hash3_128("hello", "world");
+-----------------------------------------+
| xx_hash3_128('hello', 'world')          |
+-----------------------------------------+
| -45235302294609689438180196264627189905 |
+-----------------------------------------+
```

## keyword

XX_HASH3_128,HASH,xxHash3
