---
displayed_sidebar: docs
---

# INSERT INTO FILES を使用したデータのアンロード

このトピックでは、INSERT INTO FILES を使用して StarRocks からリモートストレージにデータをアンロードする方法について説明します。

バージョン 3.2 以降、StarRocks はテーブル関数 FILES() を使用してリモートストレージに書き込み可能なファイルを定義することをサポートしています。その後、FILES() を INSERT 文と組み合わせて、StarRocks からリモートストレージにデータをアンロードできます。

StarRocks がサポートする他のデータエクスポート方法と比較して、INSERT INTO FILES を使用したデータのアンロードは、より統一された使いやすいインターフェースを提供します。データをロードする際に使用したのと同じ構文を使用して、リモートストレージに直接データをアンロードできます。さらに、この方法は、指定された列の値を抽出することによって、異なるストレージパスにデータファイルを保存することをサポートしており、エクスポートされたデータをパーティション化されたレイアウトで管理することができます。

> **NOTE**
>
> INSERT INTO FILES を使用したデータのアンロードは、ローカルファイルシステムへの直接エクスポートをサポートしていません。ただし、NFS を使用してローカルファイルにデータをエクスポートすることができます。詳細は [Unload to local files using NFS](#unload-to-local-files-using-nfs) を参照してください。

## 準備

次の例では、データベース `unload` とテーブル `sales_records` を作成し、以下のチュートリアルで使用できるデータオブジェクトとして使用します。ご自身のデータを使用することもできます。

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

テーブル `sales_records` には、各トランザクションのトランザクション ID `record_id`、販売員 `seller`、店舗 ID `store_id`、時間 `sales_time`、販売額 `sales_amt` が含まれています。これは `sales_time` に基づいて日次でパーティション化されています。

また、書き込みアクセス権を持つリモートストレージシステムを準備する必要があります。以下の例では、次のリモートストレージにデータをエクスポートします。

- シンプル認証方式が有効な HDFS クラスター。
- IAM ユーザー資格情報を使用した AWS S3 バケット。

FILES() がサポートするリモートストレージシステムと資格情報方法の詳細については、[SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md) を参照してください。

## データのアンロード

INSERT INTO FILES は、単一ファイルまたは複数ファイルへのデータのアンロードをサポートしています。これらのデータファイルを別々のストレージパスに指定することで、さらにパーティション化することができます。

INSERT INTO FILES を使用してデータをアンロードする際には、プロパティ `compression` を使用して圧縮アルゴリズムを手動で設定する必要があります。FILES がサポートするデータ圧縮アルゴリズムの詳細については、[unload_data_param](../sql-reference/sql-functions/table-functions/files.md#unload_data_param) を参照してください。

### 複数ファイルへのデータのアンロード

デフォルトでは、INSERT INTO FILES はデータを複数のデータファイルにアンロードし、各ファイルのサイズは 1 GB です。プロパティ `target_max_file_size` を使用してファイルサイズを設定できます（単位: バイト）。

次の例では、`sales_records` のすべてのデータ行を `data1` というプレフィックスの付いた複数の Parquet ファイルとしてアンロードします。各ファイルのサイズは 1 KB です。

:::note

ここで `target_max_file_size` を 1 KB に設定しているのは、小さなデータセットで複数ファイルにアンロードすることを示すためです。本番環境では、この値を数百 MB から数 GB の範囲内に設定することを強くお勧めします。

:::

- **S3 へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/data1",
    "format" = "parquet",
    "compression" = "uncompressed",
    "target_max_file_size" = "1024", -- 1KB
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **HDFS へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data1",
    "format" = "parquet",
    "compression" = "uncompressed",
    "target_max_file_size" = "1024", -- 1KB
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### 異なるパスの下に複数ファイルへのデータのアンロード

プロパティ `partition_by` を使用して、指定された列の値を抽出することで、異なるストレージパスにデータファイルをパーティション化することもできます。

次の例では、`sales_records` のすべてのデータ行を **/unload/partitioned/** パスの下にある複数の Parquet ファイルとしてアンロードします。これらのファイルは、列 `sales_time` の値によって区別される異なるサブパスに保存されます。

- **S3 へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/partitioned/",
    "format" = "parquet",
    "compression" = "lz4",
    "partition_by" = "sales_time",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **HDFS へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "compression" = "lz4",
    "partition_by" = "sales_time",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### 単一ファイルへのデータのアンロード

データを単一のデータファイルにアンロードするには、プロパティ `single` を `true` に指定する必要があります。

次の例では、`sales_records` のすべてのデータ行を `data2` というプレフィックスの付いた単一の Parquet ファイルとしてアンロードします。

- **S3 へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/data2",
    "format" = "parquet",
    "compression" = "lz4",
    "single" = "true",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **HDFS へのアンロード**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data2",
    "format" = "parquet",
    "compression" = "lz4",
    "single" = "true",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### MinIO へのアンロード

MinIO 用のパラメータは、AWS S3 用のパラメータとは異なります。

例:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://huditest/unload/data3",
    "format" = "parquet",
    "compression" = "zstd",
    "single" = "true",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000"
)
SELECT * FROM sales_records;
```

### NFS を使用したローカルファイルへのアンロード

`file://` プロトコルを介して NFS 内のファイルにアクセスするには、各 BE または CN ノードの同じディレクトリの下に NAS デバイスを NFS としてマウントする必要があります。

例:

```SQL
-- CSV ファイルへのデータのアンロード。
INSERT INTO FILES(
  'path' = 'file:///home/ubuntu/csvfile/', 
  'format' = 'csv', 
  'csv.column_separator' = ',', 
  'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Parquet ファイルへのデータのアンロード。
INSERT INTO FILES(
  'path' = 'file:///home/ubuntu/parquetfile/',
   'format' = 'parquet'
)
SELECT * FROM sales_records;
```

## 参照

- INSERT の使用方法の詳細については、[SQL reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md) を参照してください。
- FILES() の使用方法の詳細については、[SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md) を参照してください。