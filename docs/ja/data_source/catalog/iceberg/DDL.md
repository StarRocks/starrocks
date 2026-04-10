---
displayed_sidebar: docs
keywords: ['iceberg', 'ddl', 'create table', 'alter table', 'create database', 'drop table', 'create view', 'alter view']
---

# Iceberg DDL 操作

StarRocks Iceberg Catalog は、データベース、テーブル、ビューの作成と管理を含む、さまざまなデータ定義言語 (DDL) 操作をサポートしています。

DDL 操作を実行するには、適切な権限が必要です。権限の詳細については、以下を参照してください。[権限](../../../administration/user_privs/authorization/privilege_item.md)。

## CREATE DATABASE

Iceberg カタログにデータベースを作成します。この機能は v3.1 以降でサポートされています。

### 構文

```SQL
CREATE DATABASE [IF NOT EXISTS] <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

### パラメータ

`location`: データベースが作成されるファイルパスを指定します。HDFS とクラウドストレージの両方がサポートされています。指定しない場合、データベースは Iceberg カタログのデフォルトのファイルパスに作成されます。

`prefix` はストレージシステムによって異なります。

- HDFS: `hdfs`
- Google GCS: `gs`
- Azure Blob Storage (HTTP): `wasb`
- Azure Blob Storage (HTTPS): `wasbs`
- Azure Data Lake Storage Gen1: `adl`
- Azure Data Lake Storage Gen2 (HTTP): `abfs`
- Azure Data Lake Storage Gen2 (HTTPS): `abfss`
- AWS S3 または S3 互換ストレージ: `s3`

### 例

```SQL
CREATE DATABASE iceberg_db
PROPERTIES ("location" = "s3://my_bucket/iceberg_db/");
```

## DROP DATABASE

Iceberg カタログから空のデータベースを削除します。この機能は v3.1 以降でサポートされています。

:::note
空のデータベースのみを削除できます。データベースを削除しても、リモートストレージ内のファイルパスは削除されません。
:::

### 構文

```SQL
DROP DATABASE [IF EXISTS] <database_name>
```

### 例

```SQL
DROP DATABASE iceberg_db;
```

## CREATE TABLE

Iceberg データベースにテーブルを作成します。この機能は v3.1 以降でサポートされています。

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

#### `column_definition`

```SQL
col_name col_type [COMMENT 'comment'] [DEFAULT default_value]
```

:::note
すべての非パーティション列は、デフォルト値として `NULL` を使用する必要があります。パーティション列は非パーティション列の後に定義する必要があり、デフォルト値として `NULL` を使用することはできません。
:::

##### デフォルト値

v4.1 以降、StarRocks は Iceberg テーブルの列にデフォルト値を設定することをサポートしています。この機能には Iceberg フォーマットバージョン 3 (`"format-version" = "3"`) が必要です。

**使用法:**

- **書き込み時の埋め込み**: INSERT ステートメントを実行する際、列に値が指定されていない場合、システムは自動的にその列のデフォルト値を使用します。
- **スキーマ進化時の埋め込み**: 既存のテーブルに新しい列が追加された場合、古いデータファイル（新しい列を含まない）を読み取ると、新しい列のデフォルト値が使用されます。

**構文:**

```SQL
col_name col_type DEFAULT default_value
```

**要件:**

- テーブルは Iceberg フォーマットバージョン 3 (`"format-version" = "3"`) を使用する必要があります。
- 数値型 (INT, BIGINT, FLOAT, DOUBLE)、BOOLEAN、STRING、および DATE/TIMESTAMP 型のデフォルト値は引用符で囲む必要があります。例: `DEFAULT "18"`、`DEFAULT "100.0"`、`DEFAULT "true"`。

**例:**

- **デフォルト値を持つテーブルを作成する:**

```SQL
CREATE TABLE user_info (
    id INT,
    name STRING,
    age INT DEFAULT "18",
    score DOUBLE DEFAULT "100.0",
    status STRING DEFAULT 'active',
    is_active BOOLEAN DEFAULT "true"
) PROPERTIES ("format-version" = "3");
```

- **デフォルト値を持つ列を追加する:**

```SQL
ALTER TABLE user_info ADD COLUMN bonus DOUBLE DEFAULT "50.5";
```

- **列のデフォルト値を変更する:**

```SQL
ALTER TABLE user_info MODIFY COLUMN status STRING DEFAULT "inactive";
```

#### `partition_desc`

```SQL
PARTITION BY (partition_expr[, partition_expr...])
```

各 `partition_expr` は次のいずれかです:

- `column_name` (同一性変換)
- `transform_expr(column_name)`
- `transform_expr(column_name, parameter)`

StarRocks は、Apache Iceberg 仕様で定義されているパーティション変換式をサポートしています。

:::note
パーティション列は、FLOAT、DOUBLE、DECIMAL、DATETIME を除くすべてのデータ型をサポートしています。
:::

#### `ORDER BY`

Iceberg テーブルのソートキーを指定します。この機能は v4.0 以降でサポートされています。

```SQL
ORDER BY (column_name [ASC | DESC] [NULLS FIRST | NULLS LAST], ...)
```

#### `PROPERTIES`

主要なテーブルプロパティ:

- `location`: テーブルのファイルパス。データベースレベルのロケーションなしで AWS Glue を使用する場合に必須です。
- `file_format`: ファイル形式。`parquet` (デフォルト) のみがサポートされています。
- `compression_codec`: 圧縮アルゴリズム。オプション: SNAPPY, GZIP, ZSTD, LZ4 (デフォルト: `zstd`)。

### 例

- **非パーティションテーブルを作成する:**

```SQL
CREATE TABLE unpartition_tbl
(
    id int,
    score double
);
```

- **パーティションテーブルを作成する:**

```SQL
CREATE TABLE partition_tbl
(
    action varchar(20),
    id int,
    dt date
)
PARTITION BY (id, dt);
```

- **隠しパーティションを持つテーブルを作成する:**

```SQL
CREATE TABLE hidden_partition_tbl
(
    action VARCHAR(20),
    id INT,
    dt DATE
)
PARTITION BY bucket(id, 10), year(dt);
```

- **CREATE TABLE AS SELECT:**

```SQL
CREATE TABLE new_tbl
PARTITION BY (id, dt)
AS SELECT * FROM existing_tbl;
```

## パーティション仕様を進化させるための ALTER TABLE

パーティション列を追加または削除して、Iceberg テーブルのパーティション仕様を変更します。

### 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
ADD PARTITION COLUMN partition_expr [, partition_expr ...];

ALTER TABLE [catalog.][database.]table_name
DROP PARTITION COLUMN partition_expr [, partition_expr ...];

ALTER TABLE [catalog.][database.]table_name
REPLACE PARTITION COLUMN old_partition_expr WITH new_partition_expr;
```

サポートされている `partition_expr` 形式:

- 列名 (同一性変換)
- 変換式: `year()`、`month()`、`day()`、`hour()`、`truncate()`、`bucket()`

### 例

- **パーティション列を追加する:**

```SQL
ALTER TABLE sales_data
ADD PARTITION COLUMN month(sale_date), bucket(customer_id, 10);
```

- **パーティション列を削除する:**

```SQL
ALTER TABLE sales_data
DROP PARTITION COLUMN day(sale_date);
```

- **パーティション列を置き換える:**

```SQL
ALTER TABLE sales_data
REPLACE PARTITION COLUMN day(sale_date) WITH month(sale_date);
```

## DROP TABLE

Iceberg テーブルを削除します。この機能は v3.1 以降でサポートされています。

テーブルを削除しても、リモートストレージ内のファイルパスとデータはデフォルトでは削除されません。

### 構文

```SQL
DROP TABLE [IF EXISTS] <table_name> [FORCE]
```

### パラメータ

- `FORCE`: 指定すると、リモートストレージ内のテーブルデータは削除されますが、ファイルパスは保持されます。

### 例

```SQL
DROP TABLE iceberg_db.sales_data;

-- テーブルとそのデータを強制的に削除
DROP TABLE iceberg_db.temp_data FORCE;
```

## CREATE VIEW

Iceberg ビューを作成します。この機能は v3.5 以降でサポートされています。PROPERTIES を使用した Iceberg ビューの作成は v4.0.3 以降でサポートされています。

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

- **通常のIcebergビューを作成します:**

```SQL
CREATE VIEW IF NOT EXISTS iceberg_db.sales_summary AS
SELECT region, SUM(amount) as total_sales
FROM iceberg_db.sales
GROUP BY region;
```

- **プロパティを持つIcebergビューを作成します:**

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

## ALTER VIEWでStarRocks方言を更新

既存のIcebergビューにStarRocks方言を追加または変更します。この機能はv3.5以降でサポートされています。

:::note
各Icebergビューに対して定義できるStarRocks方言は1つだけです。
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

- **StarRocks方言を追加:**

```SQL
ALTER VIEW iceberg_db.spark_view ADD DIALECT
SELECT k1, k2 FROM iceberg_db.source_table;
```

- **StarRocks方言を変更:**

```SQL
ALTER VIEW iceberg_db.spark_view MODIFY DIALECT
SELECT k1, k2, k3 FROM iceberg_db.source_table;
```
