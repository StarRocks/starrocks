---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'ddl', 'create table', 'alter table', 'create database', 'drop table', 'create view', 'alter view']
---

# Iceberg DDL operations

This document describes Data Definition Language (DDL) operations for Iceberg catalogs in StarRocks, including creating and managing databases, tables, and views.

You must have the appropriate privileges to perform DDL operations. For more information about privileges, see [Privileges](../../../administration/user_privs/authorization/privilege_item.md).

---

## CREATE DATABASE

Creates a database in an Iceberg catalog. This feature is supported from v3.1 onwards.

### Syntax

```SQL
CREATE DATABASE [IF NOT EXISTS] <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

### Parameters

- `location`: Specifies the file path where the database will be created. Both HDFS and cloud storage are supported. If not specified, the database is created in the default file path of the Iceberg catalog.

The `prefix` varies based on the storage system:
- HDFS: `hdfs`
- Google GCS: `gs`
- Azure Blob Storage (HTTP): `wasb`
- Azure Blob Storage (HTTPS): `wasbs`
- Azure Data Lake Storage Gen1: `adl`
- Azure Data Lake Storage Gen2 (HTTP): `abfs`
- Azure Data Lake Storage Gen2 (HTTPS): `abfss`
- AWS S3 or S3-compatible storage: `s3`

### Example

```SQL
CREATE DATABASE iceberg_db
PROPERTIES ("location" = "s3://my_bucket/iceberg_db/");
```

---

## DROP DATABASE

Drops an empty database from an Iceberg catalog. This feature is supported from v3.1 onwards.

:::note
Only empty databases can be dropped. When you drop a database, the file path on storage is not deleted.
:::

### Syntax

```SQL
DROP DATABASE [IF EXISTS] <database_name>
```

### Example

```SQL
DROP DATABASE iceberg_db;
```

---

## CREATE TABLE

Creates a table in an Iceberg database. This feature is supported from v3.1 onwards.

### Syntax

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

### Parameters

#### column_definition

```SQL
col_name col_type [COMMENT 'comment']
```

:::note
All non-partition columns must use `NULL` as the default value. Partition columns must be defined after non-partition columns and cannot use `NULL` as the default value.
:::

#### partition_desc

```SQL
PARTITION BY (partition_expr[, partition_expr...])
```

Each `partition_expr` can be:
- `column_name` (identity transform)
- `transform_expr(column_name)`
- `transform_expr(column_name, parameter)`

StarRocks supports partition transformation expressions defined in the Apache Iceberg specification.

:::note
Partition columns support all data types except FLOAT, DOUBLE, DECIMAL, and DATETIME.
:::

#### ORDER BY (v4.0+)

Specifies sort keys for the Iceberg table:

```SQL
ORDER BY (column_name [ASC | DESC] [NULLS FIRST | NULLS LAST], ...)
```

#### PROPERTIES

Key table properties:

- `location`: File path for the table. Required when using AWS Glue without database-level location.
- `file_format`: File format. Only `parquet` is supported (default).
- `compression_codec`: Compression algorithm. Options: SNAPPY, GZIP, ZSTD, LZ4 (default: `zstd`).

### Examples

**Create a non-partitioned table:**

```SQL
CREATE TABLE unpartition_tbl
(
    id int,
    score double
);
```

**Create a partitioned table:**

```SQL
CREATE TABLE partition_tbl
(
    action varchar(20),
    id int,
    dt date
)
PARTITION BY (id, dt);
```

**Create a table with hidden partitions:**

```SQL
CREATE TABLE hidden_partition_tbl
(
    action VARCHAR(20),
    id INT,
    dt DATE
)
PARTITION BY bucket(id, 10), year(dt);
```

**Create table as select:**

```SQL
CREATE TABLE new_tbl
PARTITION BY (id, dt)
AS SELECT * FROM existing_tbl;
```

---

## ALTER TABLE (Evolve partition spec)

Modifies an Iceberg table's partition spec by adding or dropping partition columns.

### Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
ADD PARTITION COLUMN partition_expr [, partition_expr ...];

ALTER TABLE [catalog.][database.]table_name
DROP PARTITION COLUMN partition_expr [, partition_expr ...];
```

Supported `partition_expr` formats:
- Column name (identity transform)
- Transform expressions: `year()`, `month()`, `day()`, `hour()`, `truncate()`, `bucket()`

### Examples

**Add partition columns:**

```SQL
ALTER TABLE sales_data
ADD PARTITION COLUMN month(sale_date), bucket(customer_id, 10);
```

**Drop partition column:**

```SQL
ALTER TABLE sales_data
DROP PARTITION COLUMN day(sale_date);
```

---

## DROP TABLE

Drops an Iceberg table. This feature is supported from v3.1 onwards.

When you drop a table, the file path and data on storage are not deleted by default.

### Syntax

```SQL
DROP TABLE [IF EXISTS] <table_name> [FORCE]
```

### Parameters

- `FORCE`: When specified, deletes the table's data on storage while retaining the file path.

### Example

```SQL
DROP TABLE iceberg_db.sales_data;

-- Force drop with data deletion
DROP TABLE iceberg_db.temp_data FORCE;
```

---

## CREATE VIEW

Creates an Iceberg view. This feature is supported from v3.5 onwards.

### Syntax

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

### Example

```SQL
CREATE VIEW IF NOT EXISTS iceberg_db.sales_summary AS
SELECT region, SUM(amount) as total_sales
FROM iceberg_db.sales
GROUP BY region;
```

**With properties (v4.0.3+):**

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

Adds or modifies StarRocks dialect for an existing Iceberg view. This feature is supported from v3.5 onwards.

:::note
You can define only one StarRocks dialect for each Iceberg view.
:::

### Syntax

```SQL
ALTER VIEW [<catalog>.<database>.]<view_name>
(
    <column_name> [, <column_name>]
)
{ ADD | MODIFY } DIALECT
<query_statement>
```

### Examples

**Add StarRocks dialect:**

```SQL
ALTER VIEW iceberg_db.spark_view ADD DIALECT
SELECT k1, k2 FROM iceberg_db.source_table;
```

**Modify StarRocks dialect:**

```SQL
ALTER VIEW iceberg_db.spark_view MODIFY DIALECT
SELECT k1, k2, k3 FROM iceberg_db.source_table;
```
