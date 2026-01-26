---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'ddl', 'create table', 'alter table', 'create database', 'drop table', 'create view', 'alter view']
---

# Iceberg DDL 操作

このドキュメントでは、StarRocksにおけるIcebergカタログのデータ定義言語（DDL）操作について説明します。これには、データベース、テーブル、およびビューの作成と管理が含まれます。

DDL操作を実行するには、適切な権限が必要です。権限の詳細については、[権限](../../../administration/user_privs/authorization/privilege_item.md)を参照してください。

---

## データベースの作成

Icebergカタログにデータベースを作成します。この機能はv3.1以降でサポートされています。

### 構文

```SQL
CREATE DATABASE [IF NOT EXISTS] <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

### パラメータ

- `location`: データベースが作成されるファイルパスを指定します。HDFSとクラウドストレージの両方がサポートされています。指定しない場合、データベースはIcebergカタログのデフォルトファイルパスに作成されます。

`prefix`は使用するストレージシステムに基づいて異なります：
- HDFS: `hdfs`
- Google GCS: `gs`
- Azure Blob Storage (HTTP): `wasb`
- Azure Blob Storage (HTTPS): `wasbs`
- Azure Data Lake Storage Gen1: `adl`
- Azure Data Lake Storage Gen2 (HTTP): `abfs`
- Azure Data Lake Storage Gen2 (HTTPS): `abfss`
- AWS S3またはS3互換ストレージ: `s3`

### 例

```SQL
CREATE DATABASE iceberg_db
PROPERTIES ("location" = "s3://my_bucket/iceberg_db/");
```

---

## データベースの削除

Icebergカタログから空のデータベースを削除します。この機能はv3.1以降でサポートされています。

:::note
空のデータベースのみを削除できます。データベースを削除しても、ストレージ上のファイルパスは削除されません。
:::

### 構文

```SQL
DROP DATABASE [IF EXISTS] <database_name>
```

### 例

```SQL
DROP DATABASE iceberg_db;
```

---

## テーブルの作成

Icebergデータベースにテーブルを作成します。この機能はv3.1以降でサポートされています。

### 構文

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

### パラメータ

#### column_definition

```SQL
col_name col_type [COMMENT 'comment']
```

:::note
非パーティション列にはすべて `NULL` をデフォルト値として使用する必要があります。パーティション列は非パーティション列の後に定義する必要があり、`NULL` をデフォルト値として使用することはできません。
:::

#### partition_desc

```SQL
PARTITION BY (partition_expr[, partition_expr...])
```

各 `partition_expr` は以下のいずれかです：
- `column_name`（識別変換）
- `transform_expr(column_name)`
- `transform_expr(column_name, parameter)`

StarRocksは、Apache Iceberg仕様で定義されたパーティション変換式をサポートしています。

:::note
パーティション列は、FLOAT、DOUBLE、DECIMAL、およびDATETIMEを除くすべてのデータ型をサポートしています。
:::

#### ORDER BY (v4.0+)

Icebergテーブルのソートキーを指定します：

```SQL
ORDER BY (column_name [ASC | DESC] [NULLS FIRST | NULLS LAST], ...)
```

#### PROPERTIES

主要なテーブルプロパティ：

- `location`: テーブルのファイルパス。データベースレベルの場所を指定せずにAWS Glueを使用する場合に必要です。
- `file_format`: ファイル形式。`parquet` のみがサポートされています（デフォルト）。
- `compression_codec`: 圧縮アルゴリズム。オプション：SNAPPY、GZIP、ZSTD、LZ4（デフォルト：`zstd`）。

### 例

**非パーティションテーブルを作成する：**

```SQL
CREATE TABLE unpartition_tbl
(
    id int,
    score double
);
```

**パーティションテーブルを作成する：**

```SQL
CREATE TABLE partition_tbl
(
    action varchar(20),
    id int,
    dt date
)
PARTITION BY (id, dt);
```

**隠しパーティションを持つテーブルを作成する：**

```SQL
CREATE TABLE hidden_partition_tbl
(
    action VARCHAR(20),
    id INT,
    dt DATE
)
PARTITION BY bucket(id, 10), year(dt);
```

**SELECTを使用してテーブルを作成する：**

```SQL
CREATE TABLE new_tbl
PARTITION BY (id, dt)
AS SELECT * FROM existing_tbl;
```

---

## テーブルの変更（パーティション仕様の変更）

パーティション列を追加または削除して、Icebergテーブルのパーティション仕様を変更します。

### 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
ADD PARTITION COLUMN partition_expr [, partition_expr ...];

ALTER TABLE [catalog.][database.]table_name
DROP PARTITION COLUMN partition_expr [, partition_expr ...];
```

サポートされている `partition_expr` 形式：
- 列名（識別変換）
- 変換式：`year()`、`month()`、`day()`、`hour()`、`truncate()`、`bucket()`

### 例

**パーティション列を追加する：**

```SQL
ALTER TABLE sales_data
ADD PARTITION COLUMN month(sale_date), bucket(customer_id, 10);
```

**パーティション列を削除する：**

```SQL
ALTER TABLE sales_data
DROP PARTITION COLUMN day(sale_date);
```

---

## テーブルの削除

Icebergテーブルを削除します。この機能はv3.1以降でサポートされています。

テーブルを削除しても、デフォルトではストレージ上のファイルパスとデータは削除されません。

### 構文

```SQL
DROP TABLE [IF EXISTS] <table_name> [FORCE]
```

### パラメータ

- `FORCE`: 指定すると、ファイルパスを保持したまま、ストレージ上のテーブルのデータを削除します。

### 例

```SQL
DROP TABLE iceberg_db.sales_data;

-- データ削除を伴う強制削除
DROP TABLE iceberg_db.temp_data FORCE;
```

---

## ビューの作成

Icebergビューを作成します。この機能はv3.5以降でサポートされています。

### 構文

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

### 例

```SQL
CREATE VIEW IF NOT EXISTS iceberg_db.sales_summary AS
SELECT region, SUM(amount) as total_sales
FROM iceberg_db.sales
GROUP BY region;
```

**プロパティを使用する場合（v4.0.3以降）：**

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

## ビューの変更

既存のIcebergビューにStarRocks方言を追加または変更します。この機能はv3.5以降でサポートされています。

:::note
各Icebergビューに対して1つのStarRocks方言のみを定義できます。
:::

### 構文

```SQL
ALTER VIEW [<catalog>.<database>.]<view_name>
(
    <column_name> [, <column_name>]
)
{ ADD | MODIFY } DIALECT
<query_statement>
```

### 例

**StarRocks方言を追加する：**

```SQL
ALTER VIEW iceberg_db.spark_view ADD DIALECT
SELECT k1, k2 FROM iceberg_db.source_table;
```

**StarRocks方言を変更する：**

```SQL
ALTER VIEW iceberg_db.spark_view MODIFY DIALECT
SELECT k1, k2, k3 FROM iceberg_db.source_table;
```
