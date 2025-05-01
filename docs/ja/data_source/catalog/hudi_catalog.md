---
displayed_sidebar: docs
---

# Hudi catalog

Hudi catalog は、Apache Hudi からデータをインジェストせずにクエリを実行できる外部 catalog の一種です。

また、Hudi catalog を基に [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) を使用して、Hudi からデータを直接変換してロードすることもできます。StarRocks は v2.4 以降で Hudi catalog をサポートしています。

Hudi クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- AWS S3 や HDFS のようなオブジェクトストレージまたは分散ファイルシステム
- Hive metastore や AWS Glue のようなメタストア

## 使用上の注意

- StarRocks がサポートする Hudi のファイルフォーマットは Parquet です。Parquet ファイルは以下の圧縮フォーマットをサポートしています: SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION。
- StarRocks は Hudi の Copy On Write (COW) テーブルと Merge On Read (MOR) テーブルを完全にサポートしています。

## 統合準備

Hudi catalog を作成する前に、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Hudi クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

### HDFS

HDFS をストレージとして選択する場合、StarRocks クラスターを以下のように設定してください。

- (オプション) HDFS クラスターおよび Hive metastore にアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive metastore にアクセスするために FE および BE プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルおよび各 BE の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Hudi データをクエリする際、StarRocks クラスターの FEs および BEs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。以下の状況でのみ、StarRocks クラスターを設定する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive metastore で Kerberos 認証が有効になっている場合、StarRocks クラスターを以下のように設定してください。

- 各 FE および各 BE で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive metastore にアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Hudi catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "hudi",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### パラメータ

#### catalog_name

Hudi catalog の名前です。命名規則は以下の通りです。

- 名前には、文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### comment

Hudi catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `hudi` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive metastore

データソースのメタストアとして Hive metastore を選択する場合、`MetastoreParams` を以下のように設定します。

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> Hudi データをクエリする前に、Hive metastore ノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive metastore にアクセスできない可能性があります。

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| パラメータ           | 必須 | 説明                                                  |
| ------------------- | ---- | ----------------------------------------------------- |
| hive.metastore.type | はい  | Hudi クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | はい  | Hive metastore の URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />高可用性 (HA) が Hive metastore に対して有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択する場合、以下のいずれかのアクションを実行します。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| パラメータ                     | 必須 | 説明                                                  |
| ----------------------------- | ---- | ----------------------------------------------------- |
| hive.metastore.type           | はい  | Hudi クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | はい  | インスタンスプロファイルベースの認証方法およびアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | いいえ | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | はい  | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | いいえ | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | いいえ | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

Hudi クラスターが AWS S3 をストレージとして使用する場合にのみ `StorageCredentialParams` を設定する必要があります。

Hudi クラスターが他のストレージシステムを使用する場合、`StorageCredentialParams` を無視できます。

##### AWS S3

Hudi クラスターのストレージとして AWS S3 を選択する場合、以下のいずれかのアクションを実行します。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.use_instance_profile | はい  | インスタンスプロファイルベースの認証方法およびアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | はい  | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | いいえ | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

Hudi catalog は v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム (例: MinIO) を Hudi クラスターのストレージとして選択する場合、以下のように `StorageCredentialParams` を設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "{true | false}",
"aws.s3.enable_path_style_access" = "{true | false}",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| パラメータ                        | 必須 | 説明                                                  |
| -------------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.enable_ssl                | はい  | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | はい  | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。たとえば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | はい  | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | はい  | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | はい  | IAM ユーザーのシークレットキーです。 |

#### MetadataUpdateParams

StarRocks が Hudi のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

StarRocks はデフォルトで [自動非同期更新ポリシー](#appendix-understand-metadata-automatic-asynchronous-update) を実装しています。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Hudi でのデータ更新の頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化できます。

> **注意**
>
> ほとんどの場合、Hudi データが 1 時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。

| パラメータ                              | 必須 | 説明                                                  |
|----------------------------------------| ---- | ----------------------------------------------------- |
| enable_metastore_cache                 | いいえ | StarRocks が Hudi テーブルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。`true` の場合、キャッシュを有効にし、`false` の場合、キャッシュを無効にします。 |
| enable_remote_file_cache               | いいえ | StarRocks が Hudi テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。`true` の場合、キャッシュを有効にし、`false` の場合、キャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | いいえ | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションのメタデータを非同期に更新する時間間隔です。単位: 秒。デフォルト値: `7200` (2 時間)。 |
| remote_file_cache_refresh_interval_sec | いいえ | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションの基礎データファイルのメタデータを非同期に更新する時間間隔です。単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | いいえ | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションのメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `86400` (24 時間)。 |
| remote_file_cache_ttl_sec              | いいえ | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションの基礎データファイルのメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `129600` (36 時間)。 |

### 例

以下の例は、使用するメタストアのタイプに応じて `hudi_catalog_hms` または `hudi_catalog_glue` という名前の Hudi catalog を作成し、Hudi クラスターからデータをクエリします。

#### HDFS

HDFS をストレージとして使用する場合、以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG hudi_catalog_hms
PROPERTIES
(
    "type" = "hudi",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースのクレデンシャルを選択する場合

- Hudi クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_glue
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### アサインされたロールベースのクレデンシャルを選択する場合

- Hudi クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_glue
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM ユーザーベースのクレデンシャルを選択する場合

- Hudi クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_glue
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "glue",
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

MinIO を例にとります。以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG hudi_catalog_hms
PROPERTIES
(
    "type" = "hudi",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

## Hudi テーブルのスキーマを表示する

Hudi テーブルのスキーマを表示するには、以下の構文のいずれかを使用できます。

- スキーマを表示する

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示する

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Hudi テーブルをクエリする

1. Hudi クラスター内のデータベースを表示するには、以下の構文を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. ターゲットの Hudi データベースに接続するには、以下の構文を使用します。

   ```SQL
   USE <catalog_name>.<database_name>
   ```

3. Hudi テーブルをクエリするには、以下の構文を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Hudi からデータをロードする

OLAP テーブル `olap_tbl` を持っていると仮定すると、以下のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM hudi_table
```

## メタデータキャッシュを手動または自動で更新する

### 手動更新

デフォルトでは、StarRocks は Hudi のメタデータをキャッシュし、非同期モードでメタデータを自動的に更新して、より良いパフォーマンスを提供します。さらに、Hudi テーブルでいくつかのスキーマ変更やテーブル更新が行われた後、[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md) を使用してメタデータを手動で更新することもできます。これにより、StarRocks が最新のメタデータをできるだけ早く取得し、適切な実行プランを生成できるようになります。

```SQL
REFRESH EXTERNAL TABLE <table_name>
```

## 付録: メタデータの自動非同期更新を理解する

自動非同期更新は、StarRocks が Hudi catalog のメタデータを更新するために使用するデフォルトのポリシーです。

デフォルトでは (`enable_metastore_cache` および `enable_remote_file_cache` パラメータが両方とも `true` に設定されている場合)、クエリが Hudi テーブルのパーティションにヒットすると、StarRocks はそのパーティションのメタデータとそのパーティションの基礎データファイルのメタデータを自動的にキャッシュします。キャッシュされたメタデータは、遅延更新ポリシーを使用して更新されます。

たとえば、`table2` という名前の Hudi テーブルがあり、4 つのパーティション `p1`、`p2`、`p3`、`p4` を持っているとします。クエリが `p1` にヒットすると、StarRocks は `p1` のメタデータと `p1` の基礎データファイルのメタデータをキャッシュします。キャッシュされたメタデータを更新および破棄するデフォルトの時間間隔は次のとおりです。

- キャッシュされた `p1` のメタデータを非同期に更新する時間間隔 (`metastore_cache_refresh_interval_sec` パラメータで指定) は 2 時間です。
- キャッシュされた `p1` の基礎データファイルのメタデータを非同期に更新する時間間隔 (`remote_file_cache_refresh_interval_sec` パラメータで指定) は 60 秒です。
- キャッシュされた `p1` のメタデータを自動的に破棄する時間間隔 (`metastore_cache_ttl_sec` パラメータで指定) は 24 時間です。
- キャッシュされた `p1` の基礎データファイルのメタデータを自動的に破棄する時間間隔 (`remote_file_cache_ttl_sec` パラメータで指定) は 36 時間です。

以下の図は、理解を容易にするために、タイムライン上の時間間隔を示しています。

![キャッシュされたメタデータの更新と破棄のタイムライン](../../_assets/catalog_timeline.png)

その後、StarRocks は以下のルールに従ってメタデータを更新または破棄します。

- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒未満の場合、StarRocks は `p1` のキャッシュされたメタデータや `p1` の基礎データファイルのキャッシュされたメタデータを更新しません。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒を超える場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを更新します。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 2 時間を超える場合、StarRocks は `p1` のキャッシュされたメタデータを更新します。
- `p1` が最後の更新から 24 時間以内にアクセスされていない場合、StarRocks は `p1` のキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。
- `p1` が最後の更新から 36 時間以内にアクセスされていない場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。