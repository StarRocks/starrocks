---
displayed_sidebar: docs
---

# Delta Lake catalog

Delta Lake catalog は、Delta Lake からデータを取り込むことなくクエリを実行できる外部 catalog の一種です。

また、Delta Lake catalog を基に [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) を使用して、Delta Lake からデータを直接変換およびロードすることができます。StarRocks は v2.5 以降の Delta Lake catalog をサポートしています。

Delta Lake クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- AWS S3 や HDFS のようなオブジェクトストレージまたは分散ファイルシステム
- Hive メタストアや AWS Glue のようなメタストア

## 使用上の注意

- StarRocks がサポートする Delta Lake のファイル形式は Parquet です。Parquet ファイルは、SNAPPY、LZ4、ZSTD、GZIP、および NO_COMPRESSION の圧縮形式をサポートしています。
- StarRocks がサポートしていない Delta Lake のデータ型は MAP と STRUCT です。

## 統合準備

Delta Lake catalog を作成する前に、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Delta Lake クラスターが AWS S3 をストレージとして使用している場合、または AWS Glue をメタストアとして使用している場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations) を参照してください。

### HDFS

HDFS をストレージとして選択する場合、StarRocks クラスターを以下のように設定してください。

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルおよび各 BE の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名しか設定できません。
- Delta Lake データをクエリする際、StarRocks クラスターの FEs および BEs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。以下の状況でのみ StarRocks クラスターを設定する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスに追加します。

> **NOTE**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを以下のように設定してください。

- 各 FE および各 BE で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Delta Lake catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "deltalake",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### パラメータ

#### catalog_name

Delta Lake catalog の名前です。命名規則は以下の通りです。

- 名前には、文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

Delta Lake catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `deltalake` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive metastore

データソースのメタストアとして Hive metastore を選択する場合、`MetastoreParams` を以下のように設定します。

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Delta Lake データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始したときに StarRocks が Hive メタストアにアクセスできない可能性があります。

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive メタストアで高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

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

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue) を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

Delta Lake クラスターが AWS S3 をストレージとして使用している場合にのみ、`StorageCredentialParams` を設定する必要があります。

Delta Lake クラスターが他のストレージシステムを使用している場合、`StorageCredentialParams` を無視することができます。

##### AWS S3

Delta Lake クラスターのストレージとして AWS S3 を選択する場合、以下のいずれかのアクションを実行します。

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

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージシステム

Delta Lake catalog は v2.5 以降、S3 互換ストレージシステムをサポートしています。

MinIO のような S3 互換ストレージシステムを Delta Lake クラスターのストレージとして選択する場合、`StorageCredentialParams` を以下のように設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "{true | false}",
"aws.s3.enable_path_style_access" = "{true | false}",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。たとえば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` という名前のバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

#### MetadataUpdateParams

StarRocks が Delta Lake のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

StarRocks はデフォルトで [自動非同期更新ポリシー](#appendix-understand-metadata-automatic-asynchronous-update) を実装しています。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Delta Lake でのデータ更新の頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化することができます。

> **NOTE**
>
> ほとんどの場合、Delta Lake データが 1 時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。

| Parameter                              | Required | Description                                                  |
|----------------------------------------| -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | No       | StarRocks が Delta Lake テーブルのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| enable_remote_file_cache               | No       | StarRocks が Delta Lake テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | No       | StarRocks が Delta Lake テーブルまたはパーティションのキャッシュされたメタデータを非同期で更新する時間間隔です。単位: 秒。デフォルト値: `7200`、つまり 2 時間です。 |
| remote_file_cache_refresh_interval_sec | No       | StarRocks が Delta Lake テーブルまたはパーティションの基礎データファイルのキャッシュされたメタデータを非同期で更新する時間間隔です。単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | No       | StarRocks が Delta Lake テーブルまたはパーティションのキャッシュされたメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `86400`、つまり 24 時間です。 |
| remote_file_cache_ttl_sec              | No       | StarRocks が Delta Lake テーブルまたはパーティションの基礎データファイルのキャッシュされたメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `129600`、つまり 36 時間です。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、Delta Lake クラスターからデータをクエリするための `deltalake_catalog_hms` または `deltalake_catalog_glue` という名前の Delta Lake catalog を作成します。

#### HDFS

HDFS をストレージとして使用する場合、以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG deltalake_catalog_hms
PROPERTIES
(
    "type" = "deltalake",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースの認証を選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### アサインされたロールベースの認証を選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM ユーザーベースの認証を選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
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
CREATE EXTERNAL CATALOG deltalake_catalog_hms
PROPERTIES
(
    "type" = "deltalake",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

## Delta Lake テーブルのスキーマを表示

Delta Lake テーブルのスキーマを表示するには、以下の構文のいずれかを使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE 文からスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Delta Lake テーブルをクエリ

1. Delta Lake クラスター内のデータベースを表示するには、以下の構文を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. 目的の Delta Lake データベースに接続するには、以下の構文を使用します。

   ```SQL
   USE <catalog_name>.<database_name>
   ```

3. Delta Lake テーブルをクエリするには、以下の構文を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Delta Lake からデータをロード

OLAP テーブル `olap_tbl` があると仮定し、以下のようにデータを変換およびロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM deltalake_table
```