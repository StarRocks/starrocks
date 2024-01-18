---
displayed_sidebar: "English"
---

# Data compression

StarRocks supports data compression for table and index storage. Data compression not only helps save storage space, but also improves performance of I/O intensive tasks because StarRocks can read fewer pages from disk for each request. Note that extra CPU resource is required to compress and decompress the data.

## Choose a data compression algorithm

StarRocks supports four data compression algorithms: LZ4, Zstandard (or zstd), zlib, and Snappy. These data compression algorithms differ in compression ratio and compression/decompression performance. Generally, the compression ratios of these algorithms rank as follows: zlib > Zstandard > LZ4 > Snappy. Among them, zlib has shown relatively high compression ratios. As a result of the highly compressed data, the loading and query performances on tables with the zlib compression algorithm are also affected. LZ4 and Zstandard, especially, have well-balanced compression ratios and decompression performances. You can choose among these compression algorithms to cater to your business needs for less storage space or better performance. We recommend LZ4 or Zstandard if you do not have specific demands for less storage space.

> **NOTE**
>
> Different data types can affect the compression ratio.

## Specify a data compression algorithm for a table

You can specify a data compression algorithm for a table only when creating the table, and you cannot change it afterwards.

The following example creates a table `data_compression` with the Zstandard algorithm. For detailed instructions, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

```SQL
CREATE TABLE `data_compression` (
  `id`      INT(11)     NOT NULL     COMMENT "",
  `name`    CHAR(200)   NULL         COMMENT ""
)
ENGINE=OLAP 
UNIQUE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 7
PROPERTIES (
"compression" = "ZSTD"
);
```

> **NOTE**
>
> If no data compression algorithm is specified, StarRocks uses LZ4 by default.

You can view the compression algorithm of a table using [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md).
