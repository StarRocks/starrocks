---
displayed_sidebar: "Chinese"
---

# 使用 INSERT INTO FILES 导出数据

本文描述了如何使用 INSERT INTO FILES 将 StarRocks 数据导出至远程存储。

从 v3.2 版本开始，StarRocks 支持使用表函数 FILES() 在远程存储中定义可写（Writable）文件。您可以通过将 FILES() 与 INSERT 语句结合，将数据从 StarRocks 导出到远程存储。

与 StarRocks 支持的其他数据导出方法相比，使用 INSERT INTO FILES 导出提供了一个更统一、易于使用的接口。您可以使用与导入数据相同的语法直接将数据导出到远程存储。此外，该方法支持通过提取指定列的值将数据文件存储在不同的存储路径中，允许您将导出的数据进一步分区。

> **说明**
>
> - 使用 INSERT INTO FILES 导出数据不支持将数据导出至本地文件系统。
> - 当前，INSERT INTO FILES 仅支持以 Parquet 文件格式导出数据。

## 准备工作

以下示例创建了一个名为 `unload` 的数据库和一个名为 `sales_records` 的表，作为以下教程中使用的数据对象。您也可以使用自己的数据作为替代。

```SQL
CREATE DATABASE unload;
USE unload;
CREATE TABLE sales_records(
    record_id     BIGINT,
    seller        STRING,
    store_id      INT,
    sales_time    DATETIME,
    sales_amt     DOUBLE
)
DUPLICATE KEY(record_id)
PARTITION BY date_trunc('day', sales_time)
DISTRIBUTED BY HASH(record_id);

INSERT INTO sales_records
VALUES
    (220313001,"Amy",1,"2022-03-13 12:00:00",8573.25),
    (220314002,"Bob",2,"2022-03-14 12:00:00",6948.99),
    (220314003,"Amy",1,"2022-03-14 12:00:00",4319.01),
    (220315004,"Carl",3,"2022-03-15 12:00:00",8734.26),
    (220316005,"Carl",3,"2022-03-16 12:00:00",4212.69),
    (220317006,"Bob",2,"2022-03-17 12:00:00",9515.88);
```

表 `sales_records` 包含了每个交易的交易 ID `record_id`、销售人员 `seller`、商店 ID `store_id`、时间 `sales_time` 和销售额 `sales_amt`。该表根据 `sales_time` 以日为单位分区。

此外，您还需要准备一个具有写权限的远程存储系统。以下示例使用启用了简单验证的 HDFS 集群。有关支持的远程存储系统和认证方法，请参阅 [SQL参考 - FILES()](../sql-reference/sql-functions/table-functions/files.md)。

## 导出数据

INSERT INTO FILES 支持将数据导出到单个文件或多个文件。您可以通过为这些文件指定不同的存储路径来进一步分区。

在使用 INSERT INTO FILES 导出数据时，您必须通过设置 `compression` 属性手动设置压缩算法。有关 StarRocks 支持的数据压缩算法，请参阅[数据压缩](../table_design/data_compression.md)。

### 导出数据到多个文件

默认情况下，INSERT INTO FILES 会将数据导出到多个数据文件中，每个文件的大小为 1 GB。您可以使用`max_file_size` 属性配置文件大小。

以下示例将 `sales_records` 中的所有数据行导出为多个以 `data1` 为前缀的 Parquet 文件。每个文件的大小为 1 KB。

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data1",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "max_file_size" = "1KB"
)
SELECT * FROM sales_records;
```

### 导出数据到不同路径下的多个文件

您还可以使用 `partition_by` 属性提取指定列的值，从而将数据文件分别存储到不同路径中。

以下示例将 `sales_records` 中的所有数据行导出为多个 Parquet 文件，存储在 HDFS 集群的路径 **/unload/partitioned/** 下。这些文件存储在不同的子路径中，这些子路径根据列 `sales_time` 中的值来区分。

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "partition_by" = "sales_time"
)
SELECT * FROM sales_records;
```

### 导出数据到单个文件

要将数据导出到单个数据文件，您必须将 `single` 属性设置为 `true`。

以下示例将 `sales_records` 中的所有数据行导出为以 `data2` 为前缀的单个 Parquet 文件。

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data2",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "single" = "true"
)
SELECT * FROM sales_records;
```

## 另请参阅

- 有关使用 INSERT 的更多说明，请参阅 [SQL 参考 - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)。
- 有关使用 FILES() 的更多说明，请参阅 [SQL 参考 - FILES()](../sql-reference/sql-functions/table-functions/files.md)。
