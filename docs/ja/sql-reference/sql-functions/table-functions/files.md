---
displayed_sidebar: docs
---

# FILES

## 説明

リモートストレージ内のデータファイルを定義します。

バージョン v3.1.0 以降、StarRocks はリモートストレージ内の読み取り専用ファイルをテーブル関数 FILES() を使用して定義することをサポートしています。ファイルのパス関連プロパティを使用してリモートストレージにアクセスし、ファイル内のデータのテーブルスキーマを推測し、データ行を返します。[SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) を使用してデータ行を直接クエリしたり、[INSERT](../../sql-statements/loading_unloading/INSERT.md) を使用して既存のテーブルにデータ行をロードしたり、[CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) を使用して新しいテーブルを作成し、データ行をロードすることができます。

バージョン v3.2.0 以降、FILES() はリモートストレージ内のファイルにデータを書き込むことをサポートしています。INSERT INTO FILES() を使用して StarRocks からリモートストレージにデータをアンロードできます。

現在、FILES() 関数は以下のデータソースとファイル形式をサポートしています：

- **データソース:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - その他の S3 互換ストレージシステム
  - Microsoft Azure Blob Storage
- **ファイル形式:**
  - Parquet
  - ORC (データアンロードには現在サポートされていません)

## 構文

- **データロード**:

  ```SQL
  FILES( data_location , data_format [, StorageCredentialParams ] [, columns_from_path ] )
  ```

- **データアンロード**:

  ```SQL
  FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
  ```

## パラメータ

すべてのパラメータは `"key" = "value"` のペアで指定します。

### data_location

ファイルにアクセスするための URI です。

パスまたはファイルを指定できます。たとえば、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` と指定して、HDFS サーバーのパス `/user/data/tablename` から `20210411` という名前のデータファイルをロードできます。

ワイルドカード `?`, `*`, `[]`, `{}`, または `^` を使用して複数のデータファイルの保存パスとしてこのパラメータを指定することもできます。たとえば、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` または `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` と指定して、HDFS サーバーのパス `/user/data/tablename` 内のすべてのパーティションまたは `202104` パーティションのみからデータファイルをロードできます。

:::note

ワイルドカードは中間パスを指定するためにも使用できます。

:::

- HDFS にアクセスするには、このパラメータを次のように指定する必要があります:

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- 例: "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- AWS S3 にアクセスするには:

  - S3 プロトコルを使用する場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "s3://<s3_path>"
    -- 例: "path" = "s3://path/file.parquet"
    ```

  - S3A プロトコルを使用する場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "s3a://<s3_path>"
    -- 例: "path" = "s3a://path/file.parquet"
    ```

- Google Cloud Storage にアクセスするには、このパラメータを次のように指定する必要があります:

  ```SQL
  "path" = "s3a://<gcs_path>"
  -- 例: "path" = "s3a://path/file.parquet"
  ```

- Azure Blob Storage にアクセスするには:

  - ストレージアカウントが HTTP 経由でのアクセスを許可している場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 例: "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```
  
  - ストレージアカウントが HTTPS 経由でのアクセスを許可している場合、このパラメータを次のように指定する必要があります:

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 例: "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

### data_format

データファイルの形式です。有効な値は `parquet` と `orc` です。

### schema_detect

バージョン v3.2 以降、FILES() は同じバッチのデータファイルの自動スキーマ検出と統合をサポートしています。StarRocks はまず、バッチ内のランダムなデータファイルの特定のデータ行をサンプリングすることでデータのスキーマを検出します。その後、StarRocks はバッチ内のすべてのデータファイルから列を統合します。

サンプリングルールは次のパラメータを使用して設定できます:

- `auto_detect_sample_files`: 各バッチでサンプリングするランダムなデータファイルの数。範囲: [0, + ∞]。デフォルト: `1`。
- `auto_detect_sample_rows`: 各サンプリングされたデータファイルでスキャンするデータ行の数。範囲: [0, + ∞]。デフォルト: `500`。

サンプリング後、StarRocks は次のルールに従ってすべてのデータファイルから列を統合します:

- 異なる列名またはインデックスを持つ列は、それぞれ個別の列として識別され、最終的にすべての個別の列の統合が返されます。
- 同じ列名を持つが異なるデータ型を持つ列は、同じ列として識別されますが、相対的に細かい粒度レベルで一般的なデータ型を持ちます。たとえば、ファイル A の列 `col1` が INT で、ファイル B では DECIMAL の場合、返される列では DOUBLE が使用されます。
  - すべての整数列は、全体的に粗い粒度レベルで整数型として統合されます。
  - 整数列と FLOAT 型の列は、DECIMAL 型として統合されます。
  - 他の型を統合するために文字列型が使用されます。
- 一般的に、STRING 型はすべてのデータ型を統合するために使用できます。

[Example 6](#example-6) を参照してください。

StarRocks がすべての列を統合できない場合、エラー情報とすべてのファイルスキーマを含むスキーマエラーレポートを生成します。

> **注意**
>
> 単一バッチ内のすべてのデータファイルは同じファイル形式である必要があります。

### StorageCredentialParams

StarRocks がストレージシステムにアクセスするために使用する認証情報です。

StarRocks は現在、HDFS へのシンプル認証、AWS S3 および GCS への IAM ユーザー認証、Azure Blob Storage への共有キー認証をサポートしています。

- シンプル認証を使用して HDFS にアクセスする:

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **キー**                        | **必須** | **説明**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | hadoop.security.authentication | いいえ           | 認証方法。有効な値: `simple` (デフォルト)。`simple` はシンプル認証を表し、認証を行わないことを意味します。 |
  | username                       | はい          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのユーザー名。 |
  | password                       | はい          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのパスワード。 |

- IAM ユーザー認証を使用して AWS S3 にアクセスする:

  ```SQL
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **キー**           | **必須** | **説明**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | はい          | Amazon S3 バケットにアクセスするために使用できるアクセスキー ID。 |
  | aws.s3.secret_key | はい          | Amazon S3 バケットにアクセスするために使用できるシークレットアクセスキー。 |
  | aws.s3.region     | はい          | AWS S3 バケットが存在するリージョン。例: `us-west-2`。 |

- IAM ユーザー認証を使用して GCS にアクセスする:

  ```SQL
  "fs.s3a.access.key" = "AAAAAAAAAAAAAAAAAAAA",
  "fs.s3a.secret.key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
  "fs.s3a.endpoint" = "<gcs_endpoint>"
  ```

  | **キー**           | **必須** | **説明**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | fs.s3a.access.key | はい          | GCS バケットにアクセスするために使用できるアクセスキー ID。 |
  | fs.s3a.secret.key | はい          | GCS バケットにアクセスするために使用できるシークレットアクセスキー。|
  | fs.s3a.endpoint   | はい          | GCS バケットにアクセスするために使用できるエンドポイント。例: `storage.googleapis.com`。エンドポイントアドレスに `https` を指定しないでください。 |

- 共有キーを使用して Azure Blob Storage にアクセスする:

  ```SQL
  "azure.blob.storage_account" = "<storage_account>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **キー**                    | **必須** | **説明**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account | はい          | Azure Blob Storage アカウントの名前。                  |
  | azure.blob.shared_key      | はい          | Azure Blob Storage アカウントにアクセスするために使用できる共有キー。 |

### columns_from_path

バージョン v3.2 以降、StarRocks はファイルパスからキー/値ペアの値を抽出し、列の値として使用できます。

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

データファイル **file1** が `/geo/country=US/city=LA/` という形式のパスに保存されているとします。`columns_from_path` パラメータを `"columns_from_path" = "country, city"` と指定して、ファイルパス内の地理情報を返される列の値として抽出できます。詳細な手順については、Example 4 を参照してください。

### unload_data_param

バージョン v3.2 以降、FILES() はデータアンロード用にリモートストレージ内の書き込み可能なファイルを定義することをサポートしています。

```sql
-- バージョン v3.2 以降でサポートされています。
unload_data_param::=
    "compression" = "<compression_method>",
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **キー**          | **必須** | **説明**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | はい          | データをアンロードする際に使用する圧縮方法。有効な値:<ul><li>`uncompressed`: 圧縮アルゴリズムを使用しません。</li><li>`gzip`: gzip 圧縮アルゴリズムを使用します。</li><li>`snappy`: SNAPPY 圧縮アルゴリズムを使用します。</li><li>`zstd`: Zstd 圧縮アルゴリズムを使用します。</li><li>`lz4`: LZ4 圧縮アルゴリズムを使用します。</li></ul>                  |
| partition_by     | いいえ           | データファイルを異なるストレージパスにパーティション分割するために使用される列のリスト。複数の列はカンマ (,) で区切られます。FILES() は指定された列のキー/値情報を抽出し、抽出されたキー/値ペアを特徴とするストレージパスにデータファイルを保存します。詳細な手順については、Example 5 を参照してください。 |
| single           | いいえ           | データを単一のファイルにアンロードするかどうか。有効な値:<ul><li>`true`: データは単一のデータファイルに保存されます。</li><li>`false` (デフォルト): アンロードされたデータの量が 512 MB を超える場合、データは複数のファイルに保存されます。</li></ul>                  |
| target_max_file_size | いいえ           | アンロードされるバッチ内の各ファイルの最大サイズを最善の努力で指定します。単位: バイト。デフォルト値: 1073741824 (1 GB)。アンロードされるデータのサイズがこの値を超える場合、データは複数のファイルに分割され、各ファイルのサイズはこの値を大幅に超えないようにします。バージョン v3.2.7 で導入されました。 |

## 戻り値

SELECT と共に使用すると、FILES() はファイル内のデータをテーブルとして返します。

- Parquet または ORC ファイルをクエリする場合、SELECT 文で目的の列名を直接指定するか、`*` を指定してすべての列からデータを取得できます。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

## 使用上の注意

バージョン v3.2 以降、FILES() は基本データ型に加えて、ARRAY、JSON、MAP、STRUCT などの複雑なデータ型もサポートしています。

## 例

#### 例 1

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/par-dup.parquet** からデータをクエリします:

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
     "aws.s3.region" = "us-west-2"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

#### 例 2

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します:

```Plain
MySQL > INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

#### 例 3

テーブル `ctas_wiki_edit` を作成し、AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブルにデータ行を挿入します:

```Plain
MySQL > CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```

#### 例 4

Parquet ファイル **/geo/country=US/city=LA/file1.parquet** (2 列 - `id` と `user` のみを含む) からデータをクエリし、そのパス内のキー/値情報を返される列として抽出します。

```Plain
SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/geo/country=US/city=LA/file1.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "columns_from_path" = "country, city"
);
+------+---------+---------+------+
| id   | user    | country | city |
+------+---------+---------+------+
|    1 | richard | US      | LA   |
|    2 | amber   | US      | LA   |
+------+---------+---------+------+
2 rows in set (3.84 sec)
```

#### 例 5

`sales_records` 内のすべてのデータ行を HDFS クラスターのパス **/unload/partitioned/** に複数の Parquet ファイルとしてアンロードします。これらのファイルは、列 `sales_time` の値によって区別される異なるサブパスに保存されます。

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

#### 例 6

自動スキーマ検出と統合。

次の例は、S3 バケット内の 2 つの Parquet ファイルに基づいています:

- ファイル 1 には 3 つの列があります - INT 列 `c1`、FLOAT 列 `c2`、DATE 列 `c3`。

```Plain
c1,c2,c3
1,0.71173,2017-11-20
2,0.16145,2017-11-21
3,0.80524,2017-11-22
4,0.91852,2017-11-23
5,0.37766,2017-11-24
6,0.34413,2017-11-25
7,0.40055,2017-11-26
8,0.42437,2017-11-27
9,0.67935,2017-11-27
10,0.22783,2017-11-29
```

- ファイル 2 には 3 つの列があります - INT 列 `c1`、INT 列 `c2`、DATETIME 列 `c3`。

```Plain
c1,c2,c3
101,9,2018-05-15T18:30:00
102,3,2018-05-15T18:30:00
103,2,2018-05-15T18:30:00
104,3,2018-05-15T18:30:00
105,6,2018-05-15T18:30:00
106,1,2018-05-15T18:30:00
107,8,2018-05-15T18:30:00
108,5,2018-05-15T18:30:00
109,6,2018-05-15T18:30:00
110,8,2018-05-15T18:30:00
```

CTAS 文を使用して `test_ctas_parquet` という名前のテーブルを作成し、2 つの Parquet ファイルからデータ行をテーブルに挿入します:

```SQL
CREATE TABLE test_ctas_parquet AS
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/*",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

`test_ctas_parquet` のテーブルスキーマを表示します:

```SQL
SHOW CREATE TABLE test_ctas_parquet\G
```

```Plain
*************************** 1. row ***************************
       Table: test_ctas_parquet
Create Table: CREATE TABLE `test_ctas_parquet` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
```

結果は、FLOAT と INT データを含む `c2` 列が DECIMAL 列として統合され、DATE と DATETIME データを含む `c3` 列が VARCHAR 列として統合されていることを示しています。