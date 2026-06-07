---
displayed_sidebar: docs
description: "Returns a signed 32-bit value derived from the XXH3 hash of the input string."
---

# xx_hash3_32

Returns a signed 32-bit value derived from the XXH3 hash of the input string. This function uses `XXH3_64bits_withSeed` and returns the lower 32 bits as an `INT`. For multiple inputs, the 32-bit result from each input is used as the seed for the next input.

## Syntax

```Haskell
INT XX_HASH3_32(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select xx_hash3_32(null);
+-------------------+
| xx_hash3_32(NULL) |
+-------------------+
|              NULL |
+-------------------+
```

```Plain Text
MySQL > select xx_hash3_32("hello");
+----------------------+
| xx_hash3_32('hello') |
+----------------------+
|           1549982973 |
+----------------------+
```

```Plain Text
MySQL > select xx_hash3_32("hello", "world");
+-------------------------------+
| xx_hash3_32('hello', 'world') |
+-------------------------------+
|                   -1872669326 |
+-------------------------------+
```

## Related functions

- [`xx_hash3_64`](./xx_hash3_64.md)
- [`xx_hash32`](./xx_hash32.md)

## keyword

XX_HASH3_32,HASH,xxHash3
