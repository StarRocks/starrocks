---
displayed_sidebar: docs
description: "Returns the 32-bit XXH32 hash value of the input string."
---

# xx_hash32

Returns the 32-bit XXH32 hash value of the input string.

## Syntax

```Haskell
INT XX_HASH32(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select xx_hash32(null);
+-----------------+
| xx_hash32(NULL) |
+-----------------+
|            NULL |
+-----------------+
```

```Plain Text
MySQL > select xx_hash32("hello");
+--------------------+
| xx_hash32('hello') |
+--------------------+
|          -83855367 |
+--------------------+
```

```Plain Text
MySQL > select xx_hash32("hello", "world");
+-----------------------------+
| xx_hash32('hello', 'world') |
+-----------------------------+
|                  -920844969 |
+-----------------------------+
```

## Related functions

- [`xx_hash64`](./xx_hash64.md)
- [`xx_hash3_32`](./xx_hash3_32.md)

## keyword

XX_HASH32,HASH,XXH32,xxHash
