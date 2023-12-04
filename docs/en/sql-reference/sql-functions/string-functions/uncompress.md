---
displayed_sidebar: "English"
---

# uncompress

## Description

In some scenario, the data is too big to insert into StarRocks, so we need a function to compress the data during ingestion, and a function to uncompress the data during query.

in which, the input is a binary value to be decompressed, and the method is the compression method. By now, StarRocks support the following compress method:

- SNAPPY
- ZLIB
- ZSTD
- LZ4

the parameter should be case-insensitive. The default value of method is ZLIB.

## Syntax

```Haskell
VARCHAR uncompress(BINARY input[, VARCHAR method])
```

## Parameters

`input`: required, the data to uncompress, which must evaluate to a BINARY value.

`method`: optional, support SNAPPY、ZLIB、ZSTD and LZ4, the default value of method is ZLIB.

## Examples

```Plain Text
mysql> SELECT uncompress(to_binary('789C2B49AD280100046701C6'));
+-----------------------------------------------------------+
| uncompress(to_binary('789C2B49AD280100046701C6'), 'ZLIB') |
+-----------------------------------------------------------+
| text                                                      |
+-----------------------------------------------------------+

mysql> select uncompress(compress("text"));
+----------------------------------------------+
| uncompress(compress('text', 'ZLIB'), 'ZLIB') |
+----------------------------------------------+
| text                                         |
+----------------------------------------------+

mysql> SELECT uncompress(compress("text", "SNAPPY"), 'snappy');
+--------------------------------------------------+
| uncompress(compress('text', 'SNAPPY'), 'snappy') |
+--------------------------------------------------+
| text                                             |
+--------------------------------------------------+

mysql> SELECT uncompress(compress("text", "LZ4"), 'lz4');
+--------------------------------------------+
| uncompress(compress('text', 'LZ4'), 'lz4') |
+--------------------------------------------+
| text                                       |
+--------------------------------------------+
```

## keyword

UNCOMPRESS
