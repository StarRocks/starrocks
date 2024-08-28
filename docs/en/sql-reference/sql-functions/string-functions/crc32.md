---
displayed_sidebar: docs
---

# crc32

## Description

Returns the 32-bit cyclic redundancy check (CRC) value of a string. If the input is NULL, NULL is returned.

This function is used for error detection. It uses the CRC32 algorithm to detect changes between source and target data.

This function is supported from v3.3 onwards.

## Syntax

```Haskell
BIGINT crc32(VARCHAR str)
```

## Parameters

`str`: the string for which you want to calculate the CRC32 value.

## Examples

```Plain Text
mysql > select crc32("starrocks");
+--------------------+
| crc32('starrocks') |
+--------------------+
|         2312449062 |
+--------------------+

mysql > select crc32(null);
+-------------+
| crc32(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.18 sec)
```

## Keywords

CRC32
