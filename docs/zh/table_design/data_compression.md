# 数据压缩

StarRocks 支持对表和索引数据进行压缩 (compression)。数据压缩不仅有助于节省存储空间，还可以提高 I/O 密集型任务的性能。请注意，压缩和解压缩数据需要额外的 CPU 资源。

## 选择数据压缩算法

StarRocks 支持四种数据压缩算法：LZ4、Zstandard（或 zstd）、zlib 和 Snappy。 这些数据压缩算法在压缩率和压缩/解压缩性能上有所不同。 通常来说，这些算法的压缩率排名如下：zlib > Zstandard > LZ4 > Snappy。其中，zlib 拥有较高的压缩比。但由于数据高度压缩，使用 zlib 算法的表，其导入和查询性能会受到一定程度的影响。而 LZ4 和 Zstandard 算法具有较为均衡的压缩比和解压缩性能。您可以根据自身业务场景在这些压缩算法中进行选择，以满足您对存储或性能的需求。如果您对存储空间占用没有特殊需求，建议您使用 LZ4 或 Zstandard 算法。

> **说明**
>
> 不同的数据类型也会影响算法的压缩率。

## 设置数据压缩算法

您只能在创建表时为其设置数据压缩算法，且后续无法修改。

以下示例基于 Zstandard 算法创建表 `data_compression`。 详细说明请参阅 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。

```SQL
CREATE TABLE `data_compression` (
  `id`      INT(11)     NOT NULL     COMMENT "",
  `name`    CHAR(200)   NULL         COMMENT ""
)
ENGINE=OLAP 
UNIQUE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"compression" = "ZSTD"
);
```

> **说明**
>
> 如不指定数据压缩算法，StarRocks 默认使用 LZ4。

您可以通过 [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md) 命令查看指定表采用的压缩算法。
