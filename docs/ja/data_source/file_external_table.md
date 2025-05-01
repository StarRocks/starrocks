---
displayed_sidebar: docs
---

# ファイル外部テーブル

ファイル外部テーブルは、特別なタイプの外部テーブルです。データを StarRocks にロードすることなく、外部ストレージシステムにある Parquet および ORC データファイルを直接クエリできます。さらに、ファイル外部テーブルはメタストアに依存しません。現在のバージョンでは、StarRocks は以下の外部ストレージシステムをサポートしています: HDFS、Amazon S3、およびその他の S3 互換ストレージシステム。

この機能は StarRocks v2.5 からサポートされています。

:::note
ファイル外部機能は、StarRocks にデータをインポートするために設計されており、通常の操作として外部システムに対してクエリを実行するためのものではありません。クエリを実行し、JOIN などの関数を使用することはできますが、パフォーマンスは良くありません。よりパフォーマンスの良いソリューションは、データを StarRocks にインポートすることです。
:::

## 制限事項

- ファイル外部テーブルは、 [default_catalog](../data_source/catalog/default_catalog.md) 内のデータベースに作成する必要があります。クラスター内で作成されたカタログをクエリするには、 [SHOW CATALOGS](../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を実行できます。
- Parquet および ORC データファイルのみがサポートされています。
- ファイル外部テーブルは、ターゲットデータファイル内のデータをクエリするためにのみ使用できます。INSERT、DELETE、DROP などのデータ書き込み操作はサポートされていません。

## 前提条件

ファイル外部テーブルを作成する前に、ターゲットデータファイルが保存されている外部ストレージシステムに StarRocks がアクセスできるように、StarRocks クラスターを設定する必要があります。ファイル外部テーブルに必要な設定は、Hive catalog に必要な設定と同じですが、メタストアを設定する必要はありません。設定の詳細については、 [Hive catalog - 統合準備](../data_source/catalog/hive_catalog.md#integration-preparations) を参照してください。

## データベースの作成 (オプション)

StarRocks クラスターに接続した後、既存のデータベースにファイル外部テーブルを作成するか、ファイル外部テーブルを管理するための新しいデータベースを作成できます。クラスター内の既存のデータベースをクエリするには、 [SHOW DATABASES](../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を実行します。その後、 `USE <db_name>` を実行してターゲットデータベースに切り替えることができます。

データベースを作成するための構文は以下の通りです。

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
| table_name       | はい      | ファイル外部テーブルの名前。命名規則は以下の通りです:<ul><li> 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。名前は文字で始める必要があります。</li><li> 名前は64文字を超えてはなりません。</li></ul> |
| col_name         | はい      | ファイル外部テーブルの列名。ファイル外部テーブルの列名はターゲットデータファイルの列名と同じである必要がありますが、大文字小文字は区別されません。ファイル外部テーブルの列の順序は、ターゲットデータファイルの順序と異なる場合があります。 |
| col_type         | はい      | ファイル外部テーブルの列タイプ。このパラメータは、ターゲットデータファイルの列タイプに基づいて指定する必要があります。詳細については、 [列タイプのマッピング](#mapping-of-column-types) を参照してください。 |
| NULL \| NOT NULL | いいえ       | ファイル外部テーブルの列が NULL を許可するかどうか。 <ul><li>NULL: NULL を許可します。 </li><li>NOT NULL: NULL を許可しません。</li></ul>この修飾子は、以下のルールに基づいて指定する必要があります: <ul><li>ターゲットデータファイルの列にこのパラメータが指定されていない場合、ファイル外部テーブルの列に対して指定しないか、NULL を指定することができます。</li><li>ターゲットデータファイルの列に NULL が指定されている場合、ファイル外部テーブルの列に対してこのパラメータを指定しないか、NULL を指定することができます。</li><li>ターゲットデータファイルの列に NOT NULL が指定されている場合、ファイル外部テーブルの列にも NOT NULL を指定する必要があります。</li></ul> |
| comment          | いいえ       | ファイル外部テーブルの列のコメント。            |
| ENGINE           | はい      | エンジンのタイプ。値を file に設定します。                   |
| comment          | いいえ       | ファイル外部テーブルの説明。                  |
| PROPERTIES       | はい      | <ul><li>`FileLayoutParams`: ターゲットファイルのパスと形式を指定します。このプロパティは必須です。</li><li>`StorageCredentialParams`: オブジェクトストレージシステムにアクセスするために必要な認証情報を指定します。このプロパティは AWS S3 およびその他の S3 互換ストレージシステムにのみ必要です。</li></ul> |

#### FileLayoutParams

ターゲットデータファイルにアクセスするための一連のパラメータ。

```SQL
"path" = "<file_path>",
"format" = "<file_format>"
"enable_recursive_listing" = "{ true | false }"
"enable_wildcards" = "{ true | false }"
```

| パラメータ                | 必須 | 説明                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| path                     | はい      | データファイルのパス。 <ul><li>データファイルが HDFS に保存されている場合、パス形式は `hdfs://<IP address of HDFS>:<port>/<path>` です。デフォルトのポート番号は 8020 です。デフォルトポートを使用する場合、指定する必要はありません。</li><li>データファイルが AWS S3 またはその他の S3 互換ストレージシステムに保存されている場合、パス形式は `s3://<bucket name>/<folder>/` です。</li></ul> パスを入力する際のルールは以下の通りです: <ul><li>パス内のすべてのファイルにアクセスしたい場合、このパラメータをスラッシュ (`/`) で終わらせます。例: `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data/`。クエリを実行すると、StarRocks はパス下のすべてのデータファイルをトラバースします。再帰を使用してデータファイルをトラバースすることはありません。</li><li>単一のファイルにアクセスしたい場合、このファイルを直接指すパスを入力します。例: `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data`。クエリを実行すると、StarRocks はこのデータファイルのみをスキャンします。</li></ul> |
| format                   | はい      | データファイルの形式。 有効な値: `parquet` および `orc`。 |
| enable_recursive_listing | いいえ       | 現在のパス下のすべてのファイルを再帰的にトラバースするかどうかを指定します。デフォルト値: `true`。値 `true` はサブディレクトリを再帰的にリストすることを指定し、値 `false` はサブディレクトリを無視することを指定します。 |
| enable_wildcards         | いいえ       | `path` でワイルドカード (`*`) を使用することをサポートするかどうか。デフォルト値: `false`。例: `2024-07-*` は `2024-07-` プレフィックスを持つすべてのファイルに一致します。このパラメータは v3.1.9 からサポートされています。 |

#### StorageCredentialParams (オプション)

ターゲットストレージシステムと StarRocks が統合する方法に関する一連のパラメータ。このパラメータセットは **オプション** です。

ターゲットストレージシステムが AWS S3 またはその他の S3 互換ストレージの場合にのみ `StorageCredentialParams` を設定する必要があります。

その他のストレージシステムの場合、`StorageCredentialParams` を無視できます。

##### AWS S3

AWS S3 に保存されたデータファイルにアクセスする必要がある場合、`StorageCredentialParams` に以下の認証パラメータを設定します。

- インスタンスプロファイルベースの認証方法を選択した場合、`StorageCredentialParams` を次のように設定します:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.region" = "<aws_s3_region>"
```

- 想定ロールベースの認証方法を選択した場合、`StorageCredentialParams` を次のように設定します:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.iam_role_arn" = "<ARN of your assumed role>",
"aws.s3.region" = "<aws_s3_region>"
```

- IAM ユーザーベースの認証方法を選択した場合、`StorageCredentialParams` を次のように設定します:

```JavaScript
"aws.s3.use_instance_profile" = "false",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>",
"aws.s3.region" = "<aws_s3_region>"
```

| パラメータ名              | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | はい      | AWS S3 にアクセスする際にインスタンスプロファイルベースの認証方法および想定ロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | はい      | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。 <br />AWS S3 にアクセスするために想定ロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。その後、StarRocks はターゲットデータファイルにアクセスする際にこのロールを想定します。 |
| aws.s3.region               | はい      | AWS S3 バケットが存在するリージョン。例: us-west-1。 |
| aws.s3.access_key           | いいえ       | IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ       | IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。|

AWS S3 にアクセスするための認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、 [AWS S3 へのアクセスのための認証パラメータ](../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージ

MinIO などの S3 互換ストレージシステムにアクセスする必要がある場合、`StorageCredentialParams` を次のように設定して、統合を成功させます:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` に設定する必要があるパラメータを説明しています。

| パラメータ                       | 必須 | 説明                                                  |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl               | はい      | SSL 接続を有効にするかどうかを指定します。 <br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access | はい      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイルの URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。たとえば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` という名前のバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイルの URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                 | はい      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key               | はい      | IAM ユーザーのアクセスキー。                         |
| aws.s3.secret_key               | はい      | IAM ユーザーのシークレットキー。                         |

#### 列タイプのマッピング

以下の表は、ターゲットデータファイルとファイル外部テーブル間の列タイプのマッピングを示しています。

| データファイル   | ファイル外部テーブル                                          |
| ----------- | ------------------------------------------------------------ |
| INT/INTEGER | INT                                                          |
| BIGINT      | BIGINT                                                       |
| TIMESTAMP   | DATETIME. <br />TIMESTAMP は、現在のセッションのタイムゾーン設定に基づいてタイムゾーンなしの DATETIME に変換され、その精度の一部を失うことに注意してください。 |
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

HDFS パスに保存された Parquet データファイルをクエリするために `t0` という名前のファイル外部テーブルを作成します。

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

例 1: ファイル外部テーブルを作成し、**インスタンスプロファイル** を使用して AWS S3 内の **単一の Parquet ファイル** にアクセスします。

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

例 2: ファイル外部テーブルを作成し、**想定ロール** を使用して AWS S3 内のターゲットファイルパス下の **すべての ORC ファイル** にアクセスします。

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

例 3: ファイル外部テーブルを作成し、**IAM ユーザー** を使用して AWS S3 内のファイルパス下の **すべての ORC ファイル** にアクセスします。

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
    "aws.s3.secret_key" = "<iam_user_access_key>",
    "aws.s3.region" = "us-west-2" 
);
```

## ファイル外部テーブルをクエリする

構文:

```SQL
SELECT <clause> FROM <file_external_table>
```

例えば、 [例 - HDFS](#examples) で作成したファイル外部テーブル `t0` からデータをクエリするには、次のコマンドを実行します:

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

テーブルのスキーマを表示するには [DESC](../sql-reference/sql-statements/table_bucket_part_index/DESCRIBE.md) を使用し、テーブルを削除するには [DROP TABLE](../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) を使用できます。