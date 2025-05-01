---
displayed_sidebar: docs
---

# FILES

## 説明

リモートストレージ内のデータファイルを定義します。以下の用途に使用できます:

- [リモートストレージシステムからデータをロードまたはクエリする](#files-for-loading)
- [リモートストレージシステムにデータをアンロードする](#files-for-unloading)

現在、FILES() 関数は以下のデータソースとファイル形式をサポートしています:

- **データソース:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - その他の S3 互換ストレージシステム
  - Microsoft Azure Blob Storage
  - NFS(NAS)
- **ファイル形式:**
  - Parquet
  - ORC
  - CSV

バージョン 3.2 以降、FILES() は基本データ型に加えて、ARRAY、JSON、MAP、STRUCT などの複雑なデータ型もサポートしています。

## ロード用の FILES()

バージョン 3.1.0 以降、StarRocks はテーブル関数 FILES() を使用してリモートストレージ内の読み取り専用ファイルを定義することをサポートしています。ファイルのパス関連プロパティを使用してリモートストレージにアクセスし、ファイル内のデータのテーブルスキーマを推測し、データ行を返します。[SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) を使用してデータ行を直接クエリしたり、[INSERT](../../sql-statements/loading_unloading/INSERT.md) を使用して既存のテーブルにデータ行をロードしたり、[CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) を使用して新しいテーブルを作成し、データ行をロードすることができます。バージョン 3.3.4 以降、[DESC](../../sql-statements/table_bucket_part_index/DESCRIBE.md) を使用して FILES() でデータファイルのスキーマを表示することもできます。

### 構文

```SQL
FILES( data_location , data_format [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] )
```

### パラメータ

すべてのパラメータは `"key" = "value"` のペアで指定します。

#### data_location

ファイルにアクセスするための URI。

パスまたはファイルを指定できます。例えば、HDFS サーバー上のパス `/user/data/tablename` から `20210411` という名前のデータファイルをロードするために、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` と指定できます。

また、ワイルドカード `?`, `*`, `[]`, `{}`, `^` を使用して複数のデータファイルの保存パスを指定することもできます。例えば、HDFS サーバー上のパス `/user/data/tablename` のすべてのパーティションまたは `202104` パーティションのみからデータファイルをロードするために、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` または `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` と指定できます。

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

- NFS(NAS) にアクセスするには:

  ```SQL
  "path" = "file:///<absolute_path>"
  -- 例: "path" = "file:///home/ubuntu/parquetfile/file.parquet"
  ```

  :::note

  `file://` プロトコルを介して NFS 内のファイルにアクセスするには、NAS デバイスを各 BE または CN ノードの同じディレクトリに NFS としてマウントする必要があります。

  :::

#### data_format

データファイルの形式。 有効な値: `parquet`, `orc`, `csv`.

特定のデータファイル形式に対して詳細なオプションを設定する必要があります。

##### CSV

CSV 形式の例:

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

###### csv.column_separator

データファイルが CSV 形式の場合に使用されるカラムセパレータを指定します。このパラメータを指定しない場合、デフォルトで `\\t` (タブ) が使用されます。このパラメータで指定するカラムセパレータは、データファイルで実際に使用されているカラムセパレータと一致している必要があります。そうでない場合、データ品質が不十分なためロードジョブが失敗します。

Files() を使用するタスクは MySQL プロトコルに従って送信されます。StarRocks と MySQL はどちらもロードリクエスト内の文字をエスケープします。したがって、カラムセパレータがタブのような不可視文字の場合、カラムセパレータの前にバックスラッシュ (`\`) を追加する必要があります。例えば、カラムセパレータが `\t` の場合は `\\t` を入力し、カラムセパレータが `\n` の場合は `\\n` を入力する必要があります。Apache Hive™ ファイルは `\x01` をカラムセパレータとして使用するため、データファイルが Hive からのものである場合は `\\x01` を入力する必要があります。

> **NOTE**
>
> - CSV データの場合、カンマ (,) やタブ、パイプ (|) などの UTF-8 文字列をテキストデリミタとして使用できますが、その長さは 50 バイトを超えてはなりません。
> - Null 値は `\N` を使用して示されます。例えば、データファイルが 3 つのカラムで構成されており、そのデータファイルのレコードが最初と最後のカラムにデータを持ち、2 番目のカラムにデータがない場合、この状況では 2 番目のカラムに `\N` を使用して null 値を示す必要があります。つまり、レコードは `a,\N,b` としてコンパイルされる必要があり、`a,,b` ではありません。`a,,b` はレコードの 2 番目のカラムが空の文字列を持っていることを示します。

###### csv.enclose

データファイルが CSV 形式の場合、RFC4180 に従ってフィールド値をラップするために使用される文字を指定します。タイプ: シングルバイト文字。デフォルト値: `NONE`。最も一般的な文字はシングルクォーテーションマーク (`'`) とダブルクォーテーションマーク (`"`) です。

`enclose` で指定された文字でラップされたすべての特殊文字 (行セパレータやカラムセパレータを含む) は通常のシンボルと見なされます。StarRocks は、`enclose` で指定された任意のシングルバイト文字を指定できるため、RFC4180 よりも柔軟です。

フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。例えば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、このフィールド値をデータファイルに `"a ""quoted"" c"` として入力できます。

###### csv.skip_header

CSV 形式のデータでスキップするヘッダ行の数を指定します。タイプ: INTEGER。デフォルト値: `0`。

一部の CSV 形式のデータファイルでは、カラム名やカラムデータ型などのメタデータを定義するためにヘッダ行が使用されます。`skip_header` パラメータを設定することで、StarRocks にこれらのヘッダ行をスキップさせることができます。例えば、このパラメータを `1` に設定すると、StarRocks はデータロード中にデータファイルの最初の行をスキップします。

データファイル内のヘッダ行は、ロードステートメントで指定した行セパレータを使用して区切られている必要があります。

###### csv.escape

行セパレータ、カラムセパレータ、エスケープ文字、`enclose` で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。これらは StarRocks によって通常の文字と見なされ、フィールド値の一部として解析されます。タイプ: シングルバイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ (`\`) で、SQL ステートメントではダブルスラッシュ (`\\`) として記述する必要があります。

> **NOTE**
>
> `escape` で指定された文字は、各ペアの `enclose` で指定された文字の内側と外側の両方に適用されます。
> 以下の 2 つの例があります:
> - `enclose` を `"` に設定し、`escape` を `\` に設定すると、StarRocks は `"say \"Hello world\""` を `say "Hello world"` に解析します。
> - カラムセパレータがカンマ (`,`) の場合、`escape` を `\` に設定すると、StarRocks は `a, b\, c` を 2 つの別々のフィールド値 `a` と `b, c` に解析します。

#### schema_detect

バージョン 3.2 以降、FILES() は同じバッチのデータファイルの自動スキーマ検出と統合をサポートしています。StarRocks は、バッチ内のランダムなデータファイルの特定のデータ行をサンプリングすることでデータのスキーマを検出し、バッチ内のすべてのデータファイルからカラムを統合します。

サンプリングルールは次のパラメータを使用して設定できます:

- `auto_detect_sample_files`: 各バッチでサンプリングするランダムなデータファイルの数。範囲: [0, + ∞]。デフォルト: `1`。
- `auto_detect_sample_rows`: 各サンプリングされたデータファイルでスキャンするデータ行の数。範囲: [0, + ∞]。デフォルト: `500`。

サンプリング後、StarRocks は次のルールに従ってすべてのデータファイルからカラムを統合します:

- 異なるカラム名またはインデックスを持つカラムは、個別のカラムとして識別され、最終的にすべての個別のカラムの統合が返されます。
- 同じカラム名を持つが異なるデータ型を持つカラムは、相対的に細かい粒度レベルで一般的なデータ型を持つ同じカラムとして識別されます。例えば、ファイル A のカラム `col1` が INT で、ファイル B のカラム `col1` が DECIMAL の場合、返されるカラムでは DOUBLE が使用されます。
  - すべての整数カラムは、全体的に粗い粒度レベルで整数型として統合されます。
  - 整数カラムと FLOAT 型カラムは、DECIMAL 型として統合されます。
  - 他の型を統合するために文字列型が使用されます。
- 一般的に、STRING 型はすべてのデータ型を統合するために使用できます。

例 5 を参照してください。

StarRocks がすべてのカラムを統合できない場合、エラー情報とすべてのファイルスキーマを含むスキーマエラーレポートを生成します。

> **CAUTION**
>
> 単一バッチ内のすべてのデータファイルは同じファイル形式でなければなりません。

#### StorageCredentialParams

StarRocks がストレージシステムにアクセスするために使用する認証情報。

StarRocks は現在、シンプル認証を使用して HDFS にアクセスし、IAM ユーザー認証を使用して AWS S3 と GCS にアクセスし、Shared Key を使用して Azure Blob Storage にアクセスすることをサポートしています。

- シンプル認証を使用して HDFS にアクセスする:

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **Key**                        | **Required** | **Description**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | hadoop.security.authentication | No           | 認証方法。有効な値: `simple` (デフォルト)。`simple` はシンプル認証を表し、認証が不要であることを意味します。 |
  | username                       | Yes          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのユーザー名。 |
  | password                       | Yes          | HDFS クラスターの NameNode にアクセスするために使用するアカウントのパスワード。 |

- IAM ユーザー認証を使用して AWS S3 にアクセスする:

  ```SQL
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | Yes          | Amazon S3 バケットにアクセスするために使用できるアクセスキー ID。 |
  | aws.s3.secret_key | Yes          | Amazon S3 バケットにアクセスするために使用できるシークレットアクセスキー。 |
  | aws.s3.region     | Yes          | AWS S3 バケットが存在するリージョン。例: `us-west-2`。 |

- IAM ユーザー認証を使用して GCS にアクセスする:

  ```SQL
  "fs.s3a.access.key" = "AAAAAAAAAAAAAAAAAAAA",
  "fs.s3a.secret.key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
  "fs.s3a.endpoint" = "<gcs_endpoint>"
  ```

| **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | fs.s3a.access.key | Yes          | GCS バケットにアクセスするために使用できるアクセスキー ID。 |
  | fs.s3a.secret.key | Yes          | GCS バケットにアクセスするために使用できるシークレットアクセスキー。|
  | fs.s3a.endpoint   | Yes          | GCS バケットにアクセスするために使用できるエンドポイント。例: `storage.googleapis.com`。エンドポイントアドレスに `https` を指定しないでください。 |

- Shared Key を使用して Azure Blob Storage にアクセスする:

  ```SQL
  "azure.blob.storage_account" = "<storage_account>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account | Yes          | Azure Blob Storage アカウントの名前。                  |
  | azure.blob.shared_key      | Yes          | Azure Blob Storage アカウントにアクセスするために使用できる Shared Key。 |

#### columns_from_path

バージョン 3.2 以降、StarRocks はファイルパスからキー/値ペアの値を抽出し、カラムの値として使用することができます。

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

データファイル **file1** が `/geo/country=US/city=LA/` という形式のパスに保存されているとします。この場合、`columns_from_path` パラメータを `"columns_from_path" = "country, city"` と指定して、ファイルパス内の地理情報を返されるカラムの値として抽出することができます。詳細な手順については、例 4 を参照してください。

### 戻り値

#### SELECT FROM FILES()

SELECT と共に使用される場合、FILES() はファイル内のデータをテーブルとして返します。

- CSV ファイルをクエリする場合、SELECT ステートメントで各カラムを表すために `$1`, `$2` ... を使用するか、`*` を指定してすべてのカラムからデータを取得できます。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE $1 > 5;
  +------+---------+------------+
  | $1   | $2      | $3         |
  +------+---------+------------+
  |    6 | 0.34413 | 2017-11-25 |
  |    7 | 0.40055 | 2017-11-26 |
  |    8 | 0.42437 | 2017-11-27 |
  |    9 | 0.67935 | 2017-11-27 |
  |   10 | 0.22783 | 2017-11-29 |
  +------+---------+------------+
  5 rows in set (0.30 sec)

  SELECT $1, $2 FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------+
  | $1   | $2      |
  +------+---------+
  |    1 | 0.71173 |
  |    2 | 0.16145 |
  |    3 | 0.80524 |
  |    4 | 0.91852 |
  |    5 | 0.37766 |
  |    6 | 0.34413 |
  |    7 | 0.40055 |
  |    8 | 0.42437 |
  |    9 | 0.67935 |
  |   10 | 0.22783 |
  +------+---------+
  10 rows in set (0.38 sec)
  ```

- Parquet または ORC ファイルをクエリする場合、SELECT ステートメントで目的のカラム名を直接指定するか、`*` を指定してすべてのカラムからデータを取得できます。

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

#### DESC FILES()

DESC と共に使用される場合、FILES() はファイルのスキーマを返します。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

## アンロード用の FILES()

バージョン 3.2.0 以降、FILES() はリモートストレージ内のファイルにデータを書き込むことをサポートしています。INSERT INTO FILES() を使用して、StarRocks からリモートストレージにデータをアンロードできます。

### 構文

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
```

### パラメータ

すべてのパラメータは `"key" = "value"` のペアで指定します。

#### data_location

[FILES() for loading - Parameters - data_location](#data_location) を参照してください。

#### data_format

[FILES() for loading - Parameters - data_format](#data_format) を参照してください。

#### StorageCredentialParams

[FILES() for loading - Parameters - StorageCredentialParams](#storagecredentialparams) を参照してください。

#### unload_data_param

```sql
unload_data_param ::=
    "compression" = { "uncompressed" | "gzip" | "snappy" | "zstd | "lz4" },
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **Key**          | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | Yes          | データをアンロードする際に使用する圧縮方法。有効な値:<ul><li>`uncompressed`: 圧縮アルゴリズムを使用しません。</li><li>`gzip`: gzip 圧縮アルゴリズムを使用します。</li><li>`snappy`: SNAPPY 圧縮アルゴリズムを使用します。</li><li>`zstd`: Zstd 圧縮アルゴリズムを使用します。</li><li>`lz4`: LZ4 圧縮アルゴリズムを使用します。</li></ul>**NOTE**<br />CSV ファイルへのアンロードはデータ圧縮をサポートしていません。この項目を `uncompressed` に設定する必要があります。                  |
| partition_by     | No           | データファイルを異なるストレージパスにパーティション分割するために使用されるカラムのリスト。複数のカラムはカンマ (,) で区切られます。FILES() は指定されたカラムのキー/値情報を抽出し、抽出されたキー/値ペアを特徴とするストレージパスの下にデータファイルを保存します。詳細な手順については、例 7 を参照してください。 |
| single           | No           | データを単一のファイルにアンロードするかどうか。有効な値:<ul><li>`true`: データは単一のデータファイルに保存されます。</li><li>`false` (デフォルト): アンロードされたデータ量が 512 MB を超える場合、データは複数のファイルに保存されます。</li></ul>                  |
| target_max_file_size | No           | アンロードされるバッチ内の各ファイルの最大サイズを最善の努力で指定します。単位: バイト。デフォルト値: 1073741824 (1 GB)。アンロードされるデータのサイズがこの値を超える場合、データは複数のファイルに分割され、各ファイルのサイズはこの値を大幅に超えないようにします。バージョン 3.2.7 で導入されました。 |

## 例

#### 例 1: ファイルからデータをクエリする

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/par-dup.parquet** からデータをクエリします:

```Plain
SELECT * FROM FILES(
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

NFS(NAS) 内の Parquet ファイルからデータをクエリします:

```SQL
SELECT * FROM FILES(
  'path' = 'file:///home/ubuntu/parquetfile/*.parquet', 
  'format' = 'parquet'
);
```

#### 例 2: ファイルからデータ行を挿入する

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `insert_wiki_edit` にデータ行を挿入します:

```Plain
INSERT INTO insert_wiki_edit
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

NFS(NAS) 内の CSV ファイルからテーブル `insert_wiki_edit` にデータ行を挿入します:

```SQL
INSERT INTO insert_wiki_edit
  SELECT * FROM FILES(
    'path' = 'file:///home/ubuntu/csvfile/*.csv', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimiter' = '\n'
  );
```

#### 例 3: ファイルからデータ行を使用して CTAS を実行する

AWS S3 バケット `inserttest` 内の Parquet ファイル **parquet/insert_wiki_edit_append.parquet** からテーブル `ctas_wiki_edit` を作成し、データ行を挿入します:

```Plain
CREATE TABLE ctas_wiki_edit AS
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

#### 例 4: ファイルからデータをクエリし、そのパス内のキー/値情報を抽出する

Parquet ファイル **/geo/country=US/city=LA/file1.parquet** (2 つのカラム `id` と `user` のみを含む) からデータをクエリし、そのパス内のキー/値情報を返されるカラムとして抽出します。

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

#### 例 5: 自動スキーマ検出と統合

以下の例は、S3 バケット内の 2 つの Parquet ファイルに基づいています:

- ファイル 1 は 3 つのカラムを含みます - INT カラム `c1`、FLOAT カラム `c2`、DATE カラム `c3`。

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

- ファイル 2 は 3 つのカラムを含みます - INT カラム `c1`、INT カラム `c2`、DATETIME カラム `c3`。

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

CTAS ステートメントを使用して `test_ctas_parquet` という名前のテーブルを作成し、2 つの Parquet ファイルからデータ行をテーブルに挿入します:

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

結果は、FLOAT と INT データを含む `c2` カラムが DECIMAL カラムとして統合され、DATE と DATETIME データを含む `c3` カラムが VARCHAR カラムとして統合されていることを示しています。

上記の結果は、Parquet ファイルが同じデータを含む CSV ファイルに変更された場合でも同じです:

```Plain
CREATE TABLE test_ctas_csv AS
  SELECT * FROM FILES(
    "path" = "s3://inserttest/csv/*",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
Query OK, 0 rows affected (30.90 sec)

SHOW CREATE TABLE test_ctas_csv\G
*************************** 1. row ***************************
       Table: test_ctas_csv
Create Table: CREATE TABLE `test_ctas_csv` (
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
1 row in set (0.27 sec)
```

#### 例 6: ファイルのスキーマを表示する

AWS S3 に保存されている Parquet ファイル `lineorder` のスキーマを DESC を使用して表示します。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

#### 例 7: データをアンロードする

HDFS クラスター内のパス **/unload/partitioned/** に複数の Parquet ファイルとして `sales_records` のすべてのデータ行をアンロードします。これらのファイルは、カラム `sales_time` の値によって区別される異なるサブパスに保存されます。

```SQL
INSERT INTO FILES(
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

NFS(NAS) 内の CSV および Parquet ファイルにクエリ結果をアンロードします:

```SQL
-- CSV
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/csvfile/', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Parquet
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/parquetfile/',
    'format' = 'parquet'
)
SELECT * FROM sales_records;
```