---
displayed_sidebar: docs
---

# ファイル外部テーブル

ファイル外部テーブルは、外部テーブルの特別なタイプです。データをStarRocksにロードせずに、外部ストレージシステムにあるParquetやORCデータファイルを直接クエリすることができます。さらに、ファイル外部テーブルはメタストアに依存しません。現在のバージョンでは、StarRocksは以下の外部ストレージシステムをサポートしています: HDFS、Amazon S3、およびその他のS3互換ストレージシステム。

この機能はStarRocks v2.5からサポートされています。

:::note

- v3.1以降、StarRocksは[INSERT](../loading/InsertInto.md#insert-data-directly-from-files-in-an-external-source-using-files) コマンドと [FILES](../sql-reference/sql-functions/table-functions/files.md) 関数を使用してクラウドストレージ上のファイルから直接データをロードすることをサポートしています。これにより、最初に外部カタログやファイル外部テーブルを作成する必要がありません。さらに、FILES() はファイルのテーブルスキーマを自動的に推測でき、データロードのプロセスを大幅に簡素化します。
- ファイル外部テーブル機能は、StarRocksにデータをロードするために設計されており、通常の操作として外部システムに対して効率的なクエリを実行するためのものではありません。よりパフォーマンスの高いソリューションは、データをStarRocksにロードすることです。

:::

## 制限事項

- ファイル外部テーブルは、[default_catalog](../data_source/catalog/default_catalog.md) 内のデータベースに作成する必要があります。クラスター内で作成されたカタログをクエリするには、[SHOW CATALOGS](../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を実行できます。
- Parquet、ORC、Avro、RCFile、およびSequenceFileデータファイルのみがサポートされています。
- ファイル外部テーブルは、ターゲットデータファイル内のデータをクエリするためにのみ使用できます。INSERT、DELETE、DROPなどのデータ書き込み操作はサポートされていません。

## 前提条件

ファイル外部テーブルを作成する前に、ターゲットデータファイルが保存されている外部ストレージシステムにStarRocksがアクセスできるように、StarRocksクラスターを構成する必要があります。ファイル外部テーブルに必要な構成は、Hiveカタログに必要な構成と同じですが、メタストアを構成する必要はありません。構成の詳細については、[Hive catalog - Integration preparations](../data_source/catalog/hive_catalog.md#integration-preparations) を参照してください。

## データベースの作成（オプション）

StarRocksクラスターに接続した後、既存のデータベースにファイル外部テーブルを作成するか、ファイル外部テーブルを管理するための新しいデータベースを作成できます。クラスター内の既存のデータベースをクエリするには、[SHOW DATABASES](../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を実行します。その後、`USE <db_name>` を実行してターゲットデータベースに切り替えることができます。

データベースを作成するための構文は次のとおりです。

```SQL
CREATE DATABASE [IF NOT EXISTS] <db_name>
```

## ファイル外部テーブルの作成

ターゲットデータベースにアクセスした後、このデータベースにファイル外部テーブルを作成できます。

### 構文

```SQL
CREATE EXTERNAL TABLE <table_name>
(
    <col_name> <col_type> [NULL | NOT NULL] [COMMENT "<comment>"]
) 
ENGINE=file
COMMENT ["comment"]
PROPERTIES
(
    FileLayoutParams,
    StorageCredentialParams
)
```

### パラメータ

| パラメータ        | 必須 | 説明                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| table_name       | Yes      | ファイル外部テーブルの名前。命名規則は次のとおりです:<ul><li> 名前には文字、数字（0-9）、およびアンダースコア（_）を含めることができます。文字で始める必要があります。</li><li> 名前は64文字を超えてはなりません。</li></ul> |
| col_name         | Yes      | ファイル外部テーブルの列名。ファイル外部テーブルの列名はターゲットデータファイルの列名と同じである必要がありますが、大文字と小文字は区別されません。ファイル外部テーブルの列の順序は、ターゲットデータファイルの順序と異なる場合があります。 |
| col_type         | Yes      | ファイル外部テーブルの列タイプ。このパラメータは、ターゲットデータファイルの列タイプに基づいて指定する必要があります。詳細については、[Mapping of column types](#mapping-of-column-types) を参照してください。 |
| NULL \| NOT NULL | No       | ファイル外部テーブルの列がNULLを許可するかどうか。 <ul><li>NULL: NULLが許可されます。 </li><li>NOT NULL: NULLは許可されません。</li></ul>この修飾子は、以下のルールに基づいて指定する必要があります: <ul><li>ターゲットデータファイルの列にこのパラメータが指定されていない場合、ファイル外部テーブルの列に対してこのパラメータを指定しないか、NULLを指定することができます。</li><li>ターゲットデータファイルの列にNULLが指定されている場合、ファイル外部テーブルの列に対してこのパラメータを指定しないか、NULLを指定することができます。</li><li>ターゲットデータファイルの列にNOT NULLが指定されている場合、ファイル外部テーブルの列にもNOT NULLを指定する必要があります。</li></ul> |
| comment          | No       | ファイル外部テーブルの列のコメント。            |
| ENGINE           | Yes      | エンジンのタイプ。値をfileに設定します。                   |
| comment          | No       | ファイル外部テーブルの説明。                  |
| PROPERTIES       | Yes      | <ul><li>`FileLayoutParams`: ターゲットファイルのパスとフォーマットを指定します。このプロパティは必須です。</li><li>`StorageCredentialParams`: オブジェクトストレージシステムにアクセスするために必要な認証情報を指定します。このプロパティは、AWS S3およびその他のS3互換ストレージシステムにのみ必要です。</li></ul> |

#### FileLayoutParams

ターゲットデータファイルにアクセスするためのパラメータのセット。

```SQL
"path" = "<file_path>",
"format" = "<file_format>"
"enable_recursive_listing" = "{ true | false }"
"enable_wildcards" = "{ true | false }"
```

| パラメータ                | 必須 | 説明                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| path                     | Yes      | データファイルのパス。 <ul><li>データファイルがHDFSに保存されている場合、パスの形式は `hdfs://<HDFSのIPアドレス>:<ポート>/<パス>` です。デフォルトのポート番号は8020です。デフォルトのポートを使用する場合、指定する必要はありません。</li><li>データファイルがAWS S3またはその他のS3互換ストレージシステムに保存されている場合、パスの形式は `s3://<バケット名>/<フォルダ>/` です。</li></ul> パスを入力する際のルールは次のとおりです: <ul><li>パス内のすべてのファイルにアクセスしたい場合、このパラメータをスラッシュ（`/`）で終わらせます。例: `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data/`。クエリを実行すると、StarRocksはパス下のすべてのデータファイルをトラバースします。再帰的にデータファイルをトラバースすることはありません。</li><li>単一のファイルにアクセスしたい場合、このファイルを直接指すパスを入力します。例: `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data`。クエリを実行すると、StarRocksはこのデータファイルのみをスキャンします。</li></ul> |
| format                   | Yes      | データファイルのフォーマット。有効な値: `parquet`, `orc`, `avro`, `rctext` または `rcbinary`, `sequence`. |
| enable_recursive_listing | No       | 現在のパス下のすべてのファイルを再帰的にトラバースするかどうかを指定します。デフォルト値: `true`。値 `true` はサブディレクトリを再帰的にリストすることを指定し、値 `false` はサブディレクトリを無視することを指定します。 |
| enable_wildcards         | No       | `path` でワイルドカード（`*`）を使用するかどうか。デフォルト値: `false`。例: `2024-07-*` は `2024-07-` プレフィックスを持つすべてのファイルに一致します。このパラメータはv3.1.9からサポートされています。 |

#### StorageCredentialParams (オプション)

ターゲットストレージシステムとStarRocksが統合する方法に関するパラメータのセット。このパラメータセットは**オプション**です。

ターゲットストレージシステムがAWS S3またはその他のS3互換ストレージの場合にのみ、`StorageCredentialParams` を構成する必要があります。

その他のストレージシステムの場合、`StorageCredentialParams` を無視できます。

##### AWS S3

AWS S3に保存されたデータファイルにアクセスする必要がある場合、`StorageCredentialParams` に次の認証パラメータを構成します。

- インスタンスプロファイルベースの認証方法を選択した場合、`StorageCredentialParams` を次のように構成します:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.region" = "<aws_s3_region>"
```

- アサインされたロールベースの認証方法を選択した場合、`StorageCredentialParams` を次のように構成します:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.iam_role_arn" = "<ARN of your assumed role>",
"aws.s3.region" = "<aws_s3_region>"
```

- IAMユーザーベースの認証方法を選択した場合、`StorageCredentialParams` を次のように構成します:

```JavaScript
"aws.s3.use_instance_profile" = "false",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>",
"aws.s3.region" = "<aws_s3_region>"
```

| パラメータ名              | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | AWS S3にアクセスする際にインスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | Yes      | AWS S3バケットに対する権限を持つIAMロールのARN。<br />AWS S3にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。その後、StarRocksはターゲットデータファイルにアクセスする際にこのロールをアサインします。 |
| aws.s3.region               | Yes      | AWS S3バケットが存在するリージョン。例: us-west-1。 |
| aws.s3.access_key           | No       | IAMユーザーのアクセスキー。IAMユーザーベースの認証方法を使用してAWS S3にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAMユーザーのシークレットキー。IAMユーザーベースの認証方法を使用してAWS S3にアクセスする場合、このパラメータを指定する必要があります。|

AWS S3にアクセスするための認証方法の選択方法とAWS IAMコンソールでのアクセス制御ポリシーの構成方法については、[Authentication parameters for accessing AWS S3](../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3互換ストレージ

MinIOなどのS3互換ストレージシステムにアクセスする必要がある場合、`StorageCredentialParams` を次のように構成して、統合を成功させます:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` に構成する必要があるパラメータを以下の表に示します。

| パラメータ                       | 必須 | 説明                                                  |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl               | Yes      | SSL接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIOの場合、値を `true` に設定する必要があります。<br />パススタイルのURLは次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイルのURLを使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                 | Yes      | AWS S3の代わりにS3互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key               | Yes      | IAMユーザーのアクセスキー。                         |
| aws.s3.secret_key               | Yes      | IAMユーザーのシークレットキー。                         |

#### 列タイプのマッピング

次の表は、ターゲットデータファイルとファイル外部テーブル間の列タイプのマッピングを示しています。

| データファイル   | ファイル外部テーブル                                          |
| ----------- | ------------------------------------------------------------ |
| INT/INTEGER | INT                                                          |
| BIGINT      | BIGINT                                                       |
| TIMESTAMP   | DATETIME。<br />TIMESTAMPは、現在のセッションのタイムゾーン設定に基づいてタイムゾーンなしのDATETIMEに変換され、一部の精度を失うことに注意してください。 |
| STRING      | STRING                                                       |
| VARCHAR     | VARCHAR                                                      |
| CHAR        | CHAR                                                         |
| DOUBLE      | DOUBLE                                                       |
| FLOAT       | FLOAT                                                        |
| DECIMAL     | DECIMAL                                                      |
| BOOLEAN     | BOOLEAN                                                      |
| ARRAY       | ARRAY                                                        |
| MAP         | MAP                                                          |
| STRUCT      | STRUCT                                                       |

### 例

#### HDFS

HDFSパスに保存されたParquetデータファイルをクエリするために、`t0` という名前のファイル外部テーブルを作成します。

```SQL
USE db_example;

CREATE EXTERNAL TABLE t0
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path"="hdfs://x.x.x.x:8020/user/hive/warehouse/person_parq/", 
    "format"="parquet"
);
```

#### AWS S3

例1: ファイル外部テーブルを作成し、AWS S3の**単一のParquetファイル**にアクセスするために**インスタンスプロファイル**を使用します。

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/raw_0.parquet", 
    "format" = "parquet",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-west-2" 
);
```

例2: ファイル外部テーブルを作成し、AWS S3のターゲットファイルパス下の**すべてのORCファイル**にアクセスするために**アサインされたロール**を使用します。

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/", 
    "format" = "orc",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::51234343412:role/role_name_in_aws_iam",
    "aws.s3.region" = "us-west-2" 
);
```

例3: ファイル外部テーブルを作成し、AWS S3のファイルパス下の**すべてのORCファイル**にアクセスするために**IAMユーザー**を使用します。

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/", 
    "format" = "orc",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>",
    "aws.s3.region" = "us-west-2" 
);
```

## ファイル外部テーブルをクエリする

構文:

```SQL
SELECT <clause> FROM <file_external_table>
```

例えば、[Examples - HDFS](#examples) で作成したファイル外部テーブル `t0` からデータをクエリするには、次のコマンドを実行します:

```plain
SELECT * FROM t0;

+--------+------+
| name   | id   |
+--------+------+
| jack   |    2 |
| lily   |    1 |
+--------+------+
2 rows in set (0.08 sec)
```

## ファイル外部テーブルを管理する

[DESC](../sql-reference/sql-statements/table_bucket_part_index/DESCRIBE.md) を使用してテーブルのスキーマを表示したり、[DROP TABLE](../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) を使用してテーブルを削除したりできます。