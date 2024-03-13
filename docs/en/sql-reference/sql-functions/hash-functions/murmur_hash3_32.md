---
displayed_sidebar: "English"
---

# murmur_hash3_32

## Description

Returns the 32-bit murmur3 hash value of the input string.

## Syntax

```Haskell
INT MURMUR_HASH3_32(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select murmur_hash3_32(null);
+-----------------------+
| murmur_hash3_32(NULL) |
+-----------------------+
|                  NULL |
+-----------------------+

MySQL > select murmur_hash3_32("hello");
+--------------------------+
| murmur_hash3_32('hello') |
+--------------------------+
|               1321743225 |
+--------------------------+

MySQL > select murmur_hash3_32("hello", "world");
+-----------------------------------+
| murmur_hash3_32('hello', 'world') |
+-----------------------------------+
|                         984713481 |
+-----------------------------------+
```

## keyword

MURMUR_HASH3_32,HASH
