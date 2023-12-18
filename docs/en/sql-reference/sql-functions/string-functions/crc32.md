---
displayed_sidebar: "English"
---

# crc32

## Description

This function returns the CRC32 checksum of a element

## Syntax

```Haskell
BIGINT crc32(ANY_TYPE, ...)
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
```Plain Text
MySQL [(none)]> select crc32(null);
+-------------+
| crc32(NULL) |
+-------------+
|   558161692 |
+-------------+
```
```Plain Text
MySQL [(none)]> select crc32(null, 1, "string");
+--------------------------+
| crc32(NULL, 1, 'string') |
+--------------------------+
|               3800869202 |
+--------------------------+
```

## keyword

CRC32
