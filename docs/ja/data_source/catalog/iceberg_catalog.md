---
displayed_sidebar: docs
---

# Iceberg catalog

Iceberg catalog は、Apache Iceberg からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Iceberg catalogs を基に [ INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) を使用して Iceberg から直接データを変換し、ロードすることもできます。StarRocks は v2.4 以降の Iceberg catalogs をサポートしています。

Iceberg クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- AWS S3 や HDFS のようなオブジェクトストレージまたは分散ファイルシステム
- Hive メタストアや AWS Glue のようなメタストア

## 使用上の注意

- StarRocks がサポートする Iceberg のファイル形式は Parquet と ORC です。

  - Parquet ファイルは、以下の圧縮形式をサポートしています: SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION。
  - ORC ファイルは、以下の圧縮形式をサポートしています: ZLIB, SNAPPY, LZO, LZ4, ZSTD, NO_COMPRESSION。

- Iceberg catalogs は v1 テーブルをサポートしていますが、v2 テーブルはサポートしていません。

## 統合準備

Iceberg catalog を作成する前に、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Iceberg クラスターが AWS S3 をストレージとして使用している場合、または AWS Glue をメタストアとして使用している場合は、適切な認証方法を選択し、関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル
- 想定ロール
- IAM ユーザー

上記の認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、 [ AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations) を参照してください。

### HDFS

HDFS をストレージとして選択する場合、StarRocks クラスターを次のように構成します。

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするためのユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルおよび各 BE の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルにユーザー名を設定した後、各 FE および各 BE を再起動してパラメーター設定を有効にします。StarRocks クラスターごとに 1 つのユーザー名のみを設定できます。
- Iceberg データをクエリする際、StarRocks クラスターの FEs および BEs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように構成します。

- 各 FE および各 BE で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。そのため、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Iceberg catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "iceberg",
    MetastoreParams,
    StorageCredentialParams
)
```

### パラメーター

#### catalog_name

Iceberg catalog の名前。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、1023 文字を超えることはできません。

#### comment

Iceberg catalog の説明。このパラメーターはオプションです。

#### type

データソースのタイプ。値を `iceberg` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメーター。

##### Hive メタストア

データソースのメタストアとして Hive メタストアを選択する場合、`MetastoreParams` を次のように構成します。

```SQL
"iceberg.catalog.type" = "hive",
"iceberg.catalog.hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> Iceberg データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始したときに StarRocks が Hive メタストアにアクセスできない可能性があります。

以下の表は、`MetastoreParams` で構成する必要があるパラメーターを説明しています。

| パラメーター                           | 必須 | 説明                                                  |
| ----------------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type                | はい      | Iceberg クラスターで使用するメタストアのタイプ。値を `hive` に設定します。 |
| iceberg.catalog.hive.metastore.uris | はい      | Hive メタストアの URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />高可用性 (HA) が Hive メタストアで有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択する場合、次のいずれかのアクションを実行します。

- インスタンスプロファイルベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 想定ロールベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下の表は、`MetastoreParams` で構成する必要があるパラメーターを説明しています。

| パラメーター                     | 必須 | 説明                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type          | はい      | Iceberg クラスターで使用するメタストアのタイプ。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | はい      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | いいえ       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。<br />想定ロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.glue.region               | はい      | AWS Glue Data Catalog が存在するリージョン。例: `us-west-1`。 |
| aws.glue.access_key           | いいえ       | AWS IAM ユーザーのアクセスキー。<br />IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.glue.secret_key           | いいえ       | AWS IAM ユーザーのシークレットキー。<br />IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメーターを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や、AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、 [ AWS Glue にアクセスするための認証パラメーター](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue) を参照してください。

#### `StorageCredentialParams`

StarRocks がストレージシステムと統合する方法に関する一連のパラメーター。このパラメーターセットはオプションです。

Iceberg クラスターが AWS S3 をストレージとして使用している場合にのみ、`StorageCredentialParams` を構成する必要があります。

Iceberg クラスターが他のストレージシステムを使用している場合、`StorageCredentialParams` を無視できます。

##### AWS S3

Iceberg クラスターのストレージとして AWS S3 を選択する場合、次のいずれかのアクションを実行します。

- インスタンスプロファイルベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 想定ロールベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメーターを説明しています。

| パラメーター                   | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | はい      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。<br />想定ロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.s3.region               | はい      | AWS S3 バケットが存在するリージョン。例: `us-west-1`。 |
| aws.s3.access_key           | いいえ       | IAM ユーザーのアクセスキー。<br />IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.s3.secret_key           | いいえ       | IAM ユーザーのシークレットキー。<br />IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメーターを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や、AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、 [ AWS S3 にアクセスするための認証パラメーター](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージシステム

Iceberg catalogs は v2.5 以降、S3 互換ストレージシステムをサポートしています。

MinIO などの S3 互換ストレージシステムを Iceberg クラスターのストレージとして選択する場合、次のように `StorageCredentialParams` を構成して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "{true | false}",
"aws.s3.enable_path_style_access" = "{true | false}",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメーターを説明しています。

| パラメーター                        | 必須 | 説明                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | はい      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | はい      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。たとえば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | はい      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | はい      | IAM ユーザーのアクセスキー。 |
| aws.s3.secret_key                | はい      | IAM ユーザーのシークレットキー。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、Iceberg クラスターからデータをクエリするための `iceberg_catalog_hms` または `iceberg_catalog_glue` という Iceberg catalog を作成します。

#### HDFS

HDFS をストレージとして使用する場合、次のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースのクレデンシャルを選択する場合

- Iceberg クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 想定ロールベースのクレデンシャルを選択する場合

- Iceberg クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM ユーザーベースのクレデンシャルを選択する場合

- Iceberg クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

#### S3 互換ストレージシステム

MinIO を例にとります。次のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

## Iceberg テーブルのスキーマを表示

Iceberg テーブルのスキーマを表示するには、次のいずれかの構文を使用できます。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Iceberg テーブルをクエリ

1. Iceberg クラスター内のデータベースを表示するには、次の構文を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. ターゲットの Iceberg データベースに接続するには、次の構文を使用します。

   ```SQL
   USE <catalog_name>.<database_name>
   ```

3. Iceberg テーブルをクエリするには、次の構文を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Iceberg からデータをロード

`olap_tbl` という名前の OLAP テーブルがあると仮定し、次のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM iceberg_table
```