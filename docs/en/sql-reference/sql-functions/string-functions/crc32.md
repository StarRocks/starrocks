---
displayed_sidebar: "English"
---

# crc32

## Description

This function returns the CRC32 checksum of a string

## Syntax

```Haskell
BIGINT crc32(VARCHAR str)
```

## Examples

```Plain Text
MySQL [(none)]> select crc32("starrocks");
+--------------------+
| crc32('starrocks') |
+--------------------+
|         2312449062 |
+--------------------+
```

## keyword

CRC32
