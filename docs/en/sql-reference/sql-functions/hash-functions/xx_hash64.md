---
displayed_sidebar: docs
description: "Returns the 64-bit XXH64 hash value of the input string."
---

# xx_hash64

Returns the 64-bit XXH64 hash value of the input string.

## Syntax

```Haskell
BIGINT XX_HASH64(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select xx_hash64(null);
+-----------------+
| xx_hash64(NULL) |
+-----------------+
|            NULL |
+-----------------+
```

```Plain Text
MySQL > select xx_hash64("hello");
+---------------------+
| xx_hash64('hello')  |
+---------------------+
| 2794345569481354659 |
+---------------------+
```

```Plain Text
MySQL > select xx_hash64("hello", "world");
+-----------------------------+
| xx_hash64('hello', 'world') |
+-----------------------------+
|         8004569595807101537 |
+-----------------------------+
```

## Related functions

- [`xx_hash32`](./xx_hash32.md)
- [`xx_hash3_64`](./xx_hash3_64.md)

## keyword

XX_HASH64,HASH,XXH64,xxHash
