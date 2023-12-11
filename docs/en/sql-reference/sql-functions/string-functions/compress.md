---
displayed_sidebar: "English"
---

# compress

## Description

In some scenario, the data is too big to insert into StarRocks, so we need a function to compress the data during ingestion, and a function to uncompress the data during query.

in which, the input is a string value to be compressed, and the method is the compression method. By now, StarRocks support the following compress method:

- SNAPPY
- ZLIB
- ZSTD
- LZ4

the parameter should be case-insensitive. The default value of method is ZLIB.

## Syntax

```Haskell
BINARY compress(VARCHAR input[, VARCHAR method])
```

## Parameters

`input`: required, the string to compress, which must evaluate to a VARCHAR value.

`method`: optional, support SNAPPY、ZLIB、ZSTD and LZ4, the default value of method is ZLIB.

## Examples

```Plain Text
mysql> SELECT compress("text");
+----------------------------------------------------+
| compress('text', 'ZLIB')                           |
+----------------------------------------------------+
| 0x789C2B49AD280100046701C6                         |
+----------------------------------------------------+

mysql> SELECT compress("123456");
+--------------------------------------------------------+
| compress('123456', 'ZLIB')                             |
+--------------------------------------------------------+
| 0x789C3334323631350300042E0136                         |
+--------------------------------------------------------+

mysql> SELECT compress("text", "SNAPPY");
+--------------------------------------------------------+
| compress('text', 'SNAPPY')                             |
+--------------------------------------------------------+
| 0x040C74657874                                         |
+--------------------------------------------------------+

mysql> SELECT compress("text", "LZ4");
+--------------------------------------------------+
| compress('text', 'LZ4')                          |
+--------------------------------------------------+
| 0x4074657874                                     |
+--------------------------------------------------+
```

## keyword

COMPRESS
