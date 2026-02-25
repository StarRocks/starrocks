---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'ddl', '创建表', '修改表', '创建数据库', '删除表', '创建视图', '修改视图']
---

# Iceberg DDL 操作

本文档介绍 StarRocks 中 Iceberg catalog 的数据定义语言（DDL）操作，包括创建和管理数据库、表和视图。

您必须具有适当的权限才能执行 DDL 操作。有关权限的更多信息，请参阅[权限](../../../administration/user_privs/authorization/privilege_item.md)。

---

## CREATE DATABASE

在 Iceberg catalog 中创建数据库。此功能从 v3.1 开始支持。

### 语法

```SQL
CREATE DATABASE [IF NOT EXISTS] <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

### 参数

- `location`: 指定创建数据库的文件路径。支持 HDFS 和云存储。如果未指定，数据库将创建在 Iceberg catalog 的默认文件路径中。

`prefix` 根据存储系统而异：
- HDFS: `hdfs`
- Google GCS: `gs`
- Azure Blob Storage (HTTP): `wasb`
- Azure Blob Storage (HTTPS): `wasbs`
- Azure Data Lake Storage Gen1: `adl`
- Azure Data Lake Storage Gen2 (HTTP): `abfs`
- Azure Data Lake Storage Gen2 (HTTPS): `abfss`
- AWS S3 或 S3 兼容存储: `s3`

### 示例

```SQL
CREATE DATABASE iceberg_db
PROPERTIES ("location" = "s3://my_bucket/iceberg_db/");
```

---

## DROP DATABASE

从 Iceberg catalog 中删除空数据库。此功能从 v3.1 开始支持。

:::note
只能删除空数据库。删除数据库时，存储上的文件路径不会被删除。
:::

### 语法

```SQL
DROP DATABASE [IF EXISTS] <database_name>
```

### 示例

```SQL
DROP DATABASE iceberg_db;
```

---

## CREATE TABLE

在 Iceberg 数据库中创建表。此功能从 v3.1 开始支持。

### 语法

```SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name
(
    column_definition1[, column_definition2, ...],
    partition_column_definition1, partition_column_definition2, ...
)
[partition_desc]
[ORDER BY sort_desc]
[PROPERTIES ("key" = "value", ...)]
[AS SELECT query]
```

### 参数

#### column_definition

```SQL
col_name col_type [COMMENT 'comment']
```

:::note
所有非分区列必须使用 `NULL` 作为默认值。分区列必须在非分区列之后定义，且不能使用 `NULL` 作为默认值。
:::

#### partition_desc

```SQL
PARTITION BY (partition_expr[, partition_expr...])
```

每个 `partition_expr` 可以是：
- `column_name`（标识转换）
- `transform_expr(column_name)`
- `transform_expr(column_name, parameter)`

StarRocks 支持 Apache Iceberg 规范中定义的分区转换表达式。

:::note
分区列支持除 FLOAT、DOUBLE、DECIMAL 和 DATETIME 之外的所有数据类型。
:::

#### ORDER BY (v4.0+)

为 Iceberg 表指定排序键：

```SQL
ORDER BY (column_name [ASC | DESC] [NULLS FIRST | NULLS LAST], ...)
```

#### PROPERTIES

关键表属性：

- `location`: 表的文件路径。在使用 AWS Glue 且未指定数据库级别位置时是必需的。
- `file_format`: 文件格式。仅支持 `parquet`（默认）。
- `compression_codec`: 压缩算法。选项：SNAPPY、GZIP、ZSTD、LZ4（默认：`zstd`）。

### 示例

**创建非分区表：**

```SQL
CREATE TABLE unpartition_tbl
(
    id int,
    score double
);
```

**创建分区表：**

```SQL
CREATE TABLE partition_tbl
(
    action varchar(20),
    id int,
    dt date
)
PARTITION BY (id, dt);
```

**创建具有隐藏分区的表：**

```SQL
CREATE TABLE hidden_partition_tbl
(
    action VARCHAR(20),
    id INT,
    dt DATE
)
PARTITION BY bucket(id, 10), year(dt);
```

**通过查询创建表：**

```SQL
CREATE TABLE new_tbl
PARTITION BY (id, dt)
AS SELECT * FROM existing_tbl;
```

---

## ALTER TABLE（演进分区规范）

通过添加或删除分区列来修改 Iceberg 表的分区规范。

### 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
ADD PARTITION COLUMN partition_expr [, partition_expr ...];

ALTER TABLE [catalog.][database.]table_name
DROP PARTITION COLUMN partition_expr [, partition_expr ...];
```

支持的 `partition_expr` 格式：
- 列名（标识转换）
- 转换表达式：`year()`、`month()`、`day()`、`hour()`、`truncate()`、`bucket()`

### 示例

**添加分区列：**

```SQL
ALTER TABLE sales_data
ADD PARTITION COLUMN month(sale_date), bucket(customer_id, 10);
```

**删除分区列：**

```SQL
ALTER TABLE sales_data
DROP PARTITION COLUMN day(sale_date);
```

---

## DROP TABLE

删除 Iceberg 表。此功能从 v3.1 开始支持。

删除表时，默认情况下不会删除存储上的文件路径和数据。

### 语法

```SQL
DROP TABLE [IF EXISTS] <table_name> [FORCE]
```

### 参数

- `FORCE`: 指定时，删除表在存储上的数据，同时保留文件路径。

### 示例

```SQL
DROP TABLE iceberg_db.sales_data;

-- 强制删除并删除数据
DROP TABLE iceberg_db.temp_data FORCE;
```

---

## CREATE VIEW

创建 Iceberg 视图。此功能从 v3.5 开始支持。

### 语法

```SQL
CREATE VIEW [IF NOT EXISTS]
[<catalog>.<database>.]<view_name>
(
    <column_name> [COMMENT 'column comment']
    [, <column_name> [COMMENT 'column comment'], ...]
)
[COMMENT 'view comment']
[PROPERTIES ("key" = "value", ...)]
AS <query_statement>
```

### 示例

```SQL
CREATE VIEW IF NOT EXISTS iceberg_db.sales_summary AS
SELECT region, SUM(amount) as total_sales
FROM iceberg_db.sales
GROUP BY region;
```

**使用属性（v4.0.3+）：**

```SQL
CREATE VIEW IF NOT EXISTS iceberg_db.sales_summary
PROPERTIES (
  "key1" = "value1"
)
AS
SELECT region, SUM(amount) as total_sales
FROM iceberg_db.sales
GROUP BY region;
```

---

## ALTER VIEW

为现有的 Iceberg 视图添加或修改 StarRocks 方言。此功能从 v3.5 开始支持。

:::note
您只能为每个 Iceberg 视图定义一个 StarRocks 方言。
:::

### 语法

```SQL
ALTER VIEW [<catalog>.<database>.]<view_name>
(
    <column_name> [, <column_name>]
)
{ ADD | MODIFY } DIALECT
<query_statement>
```

### 示例

**添加 StarRocks 方言：**

```SQL
ALTER VIEW iceberg_db.spark_view ADD DIALECT
SELECT k1, k2 FROM iceberg_db.source_table;
```

**修改 StarRocks 方言：**

```SQL
ALTER VIEW iceberg_db.spark_view MODIFY DIALECT
SELECT k1, k2, k3 FROM iceberg_db.source_table;
```
