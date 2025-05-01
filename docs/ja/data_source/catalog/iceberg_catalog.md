---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Iceberg カタログ

Iceberg カタログは、StarRocks が v2.4 以降でサポートしている外部カタログの一種です。Iceberg カタログを使用すると、以下のことが可能です。

- Iceberg に保存されたデータを直接クエリし、手動でテーブルを作成する必要がありません。
- [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) または非同期マテリアライズドビュー（v2.5 以降でサポート）を使用して、Iceberg に保存されたデータを処理し、StarRocks にデータをロードします。
- StarRocks 上で操作を行い、Iceberg データベースやテーブルを作成または削除したり、StarRocks テーブルから Parquet 形式の Iceberg テーブルにデータをシンクすることができます（この機能は v3.1 以降でサポート）。

Iceberg クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム（例: MinIO）

- メタストアとして Hive メタストア、AWS Glue、または Tabular

  > **注意**
  >
  > - ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとしては HMS のみを使用できます。
  > - メタストアとして Tabular を選択した場合、Iceberg REST カタログを使用する必要があります。

## 使用上の注意

- StarRocks がサポートする Iceberg のファイル形式は Parquet と ORC です。

  - Parquet ファイルは、SNAPPY、LZ4、ZSTD、GZIP、NO_COMPRESSION の圧縮形式をサポートします。
  - ORC ファイルは、ZLIB、SNAPPY、LZO、LZ4、ZSTD、NO_COMPRESSION の圧縮形式をサポートします。

- Iceberg カタログは v1 テーブルをサポートし、StarRocks v3.0 以降では ORC 形式の v2 テーブルをサポートします。
- Iceberg カタログは v1 テーブルをサポートします。さらに、StarRocks v3.0 以降では ORC 形式の v2 テーブルをサポートし、StarRocks v3.1 以降では Parquet 形式の v2 テーブルをサポートします。

## 統合準備

Iceberg カタログを作成する前に、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Iceberg クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の三つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

### HDFS

ストレージとして HDFS を選択した場合、StarRocks クラスターを次のように設定します。

- （オプション）HDFS クラスターおよび Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は FE および BE または CN プロセスのユーザー名を使用して HDFS クラスターおよび Hive メタストアにアクセスします。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭、および各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加してユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに一つのユーザー名のみを設定できます。
- Iceberg データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。以下の状況でのみ StarRocks クラスターを設定する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。したがって、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイル、および各 BE の **$BE_HOME/conf/be.conf** ファイルまたは各 CN の **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Iceberg カタログを作成する

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

### パラメータ

#### catalog_name

Iceberg カタログの名前。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

Iceberg カタログの説明。このパラメータはオプションです。

#### type

データソースのタイプ。値を `iceberg` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータ。

##### Hive メタストア

データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> Iceberg データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive メタストアにアクセスできない可能性があります。

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| パラメータ                           | 必須 | 説明                                                  |
| ----------------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type                | はい      | Iceberg クラスターで使用するメタストアのタイプ。値を `hive` に設定します。 |
| hive.metastore.uris                 | はい      | Hive メタストアの URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive メタストアで高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。以下のいずれかの方法を選択します。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| パラメータ                     | 必須 | 説明                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type          | はい      | Iceberg クラスターで使用するメタストアのタイプ。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | はい      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | いいえ       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。アサインされたロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | はい      | AWS Glue Data Catalog が存在するリージョン。例: `us-west-1`。 |
| aws.glue.access_key           | いいえ       | AWS IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | いいえ       | AWS IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

##### Tabular

メタストアとして Tabular を使用する場合、メタストアのタイプを REST (`"iceberg.catalog.type" = "rest"`) として指定する必要があります。`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri" = "<rest_server_api_endpoint>",
"iceberg.catalog.credential" = "<credential>",
"iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>"
```

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| パラメータ                  | 必須 | 説明                                                         |
| -------------------------- | -------- | ------------------------------------------------------------------- |
| iceberg.catalog.type       | はい      | Iceberg クラスターで使用するメタストアのタイプ。値を `rest` に設定します。           |
| iceberg.catalog.uri        | はい      | Tabular サービスエンドポイントの URI。例: `https://api.tabular.io/ws`。      |
| iceberg.catalog.credential | はい      | Tabular サービスの認証情報。                                        |
| iceberg.catalog.warehouse  | いいえ       | Iceberg カタログのウェアハウスの場所または識別子。例: `s3://my_bucket/warehouse_location` または `sandbox`。 |

以下の例は、メタストアとして Tabular を使用する Iceberg カタログ `tabular` を作成するものです。

```SQL
CREATE EXTERNAL CATALOG tabular
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://api.tabular.io/ws",
    "iceberg.catalog.credential" = "t-5Ii8e3FIbT9m0:aaaa-3bbbbbbbbbbbbbbbbbbb",
    "iceberg.catalog.warehouse" = "sandbox"
);
```

#### `StorageCredentialParams`

StarRocks がストレージシステムと統合する方法に関する一連のパラメータ。このパラメータセットはオプションです。

以下の点に注意してください。

- ストレージとして HDFS を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

- メタストアとして Tabular を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。メタストアとして HMS または AWS Glue を使用する場合、`StorageCredentialParams` を設定する必要があります。

##### AWS S3

Iceberg クラスターのストレージとして AWS S3 を選択した場合、以下のいずれかの方法を選択します。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | はい      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | はい      | AWS S3 バケットが存在するリージョン。例: `us-west-1`。 |
| aws.s3.access_key           | いいえ       | IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ       | IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

Iceberg カタログは v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム（例: MinIO）を選択した場合、Iceberg クラスターのストレージとして、以下のように `StorageCredentialParams` を設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| パラメータ                        | 必須 | 説明                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | はい      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | はい      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | はい      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | はい      | IAM ユーザーのアクセスキー。 |
| aws.s3.secret_key                | はい      | IAM ユーザーのシークレットキー。 |

##### Microsoft Azure Storage

Iceberg カタログは v3.0 以降で Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Blob Storage を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの方法を選択します。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**              | **必須** | **説明**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | はい          | Blob Storage アカウントのユーザー名。   |
  | azure.blob.shared_key      | はい          | Blob Storage アカウントの共有キー。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**             | **必須** | **説明**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| はい          | Blob Storage アカウントのユーザー名。                   |
  | azure.blob.container      | はい          | データを保存する blob コンテナの名前。        |
  | azure.blob.sas_token      | はい          | Blob Storage アカウントにアクセスするために使用される SAS トークン。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの方法を選択します。

- マネージドサービスアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                            | **必須** | **説明**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | はい          | マネージドサービスアイデンティティ認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                 | **必須** | **説明**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | はい          | サービスプリンシパルのクライアント（アプリケーション）ID。        |
  | azure.adls1.oauth2_credential | はい          | 作成された新しいクライアント（アプリケーション）シークレットの値。    |
  | azure.adls1.oauth2_endpoint   | はい          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの方法を選択します。

- マネージドアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                           | **必須** | **説明**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | はい          | マネージドアイデンティティ認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | はい          | アクセスしたいデータが存在するテナントの ID。          |
  | azure.adls2.oauth2_client_id            | はい          | マネージドアイデンティティのクライアント（アプリケーション）ID。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**               | **必須** | **説明**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | はい          | Data Lake Storage Gen2 ストレージアカウントのユーザー名。 |
  | azure.adls2.shared_key      | はい          | Data Lake Storage Gen2 ストレージアカウントの共有キー。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                      | **必須** | **説明**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | はい          | サービスプリンシパルのクライアント（アプリケーション）ID。        |
  | azure.adls2.oauth2_client_secret   | はい          | 作成された新しいクライアント（アプリケーション）シークレットの値。    |
  | azure.adls2.oauth2_client_endpoint | はい          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

##### Google GCS

Iceberg カタログは v3.0 以降で Google GCS をサポートしています。

Google GCS を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの方法を選択します。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、次のように設定します。

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

    | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウント。            |

  - サービスアカウント（仮にメタサービスアカウントと呼ぶ）が別のサービスアカウント（仮にデータサービスアカウントと呼ぶ）をインパーソネートする場合、次のように設定します。

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

    | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウント。       |

### 例

以下の例は、使用するメタストアのタイプに応じて `iceberg_catalog_hms` または `iceberg_catalog_glue` という名前の Iceberg カタログを作成し、Iceberg クラスターからデータをクエリするものです。

#### HDFS

ストレージとして HDFS を使用する場合、以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースのクレデンシャルを選択した場合

- Iceberg クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

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

##### アサインされたロールベースのクレデンシャルを選択した場合

- Iceberg クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

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

##### IAM ユーザーベースのクレデンシャルを選択した場合

- Iceberg クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

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

MinIO を例にとります。以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

#### Microsoft Azure Storage

##### Azure Blob Storage

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージドサービスアイデンティティ認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージドアイデンティティ認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - サービスアカウントが別のサービスアカウントをインパーソネートする場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

## Iceberg カタログを表示する

現在の StarRocks クラスター内のすべてのカタログをクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部カタログの作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例は、`iceberg_catalog_glue` という名前の Iceberg カタログの作成ステートメントをクエリするものです。

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

## Iceberg カタログとその中のデータベースに切り替える

Iceberg カタログとその中のデータベースに切り替えるには、以下の方法のいずれかを使用します。

- 現在のセッションで Iceberg カタログを指定するには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。

  ```SQL
  -- 現在のセッションで指定されたカタログに切り替えます:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定します:
  USE <db_name>
  ```

- Iceberg カタログとその中のデータベースに直接切り替えるには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Iceberg カタログを削除する

外部カタログを削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例は、`iceberg_catalog_glue` という名前の Iceberg カタログを削除するものです。

```SQL
DROP Catalog iceberg_catalog_glue;
```

## Iceberg テーブルのスキーマを表示する

Iceberg テーブルのスキーマを表示するには、以下の構文のいずれかを使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Iceberg テーブルをクエリする

1. Iceberg クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Iceberg カタログとその中のデータベースに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Iceberg データベースを作成する

StarRocks の内部カタログと同様に、Iceberg カタログで [CREATE DATABASE](../../administration/user_privs/privilege_item.md#catalog) 権限を持っている場合、[CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) ステートメントを使用してその Iceberg カタログ内にデータベースを作成できます。この機能は v3.1 以降でサポートされています。

> **注意**
>
> [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

[Iceberg カタログに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)、その後、以下のステートメントを使用してそのカタログ内に Iceberg データベースを作成します。

```SQL
CREATE DATABASE <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

`location` パラメータを使用して、データベースを作成したいファイルパスを指定できます。HDFS とクラウドストレージの両方がサポートされています。`location` パラメータを指定しない場合、StarRocks は Iceberg カタログのデフォルトのファイルパスにデータベースを作成します。

`prefix` は使用するストレージシステムに基づいて異なります。

| **ストレージシステム**                                         | **`Prefix` 値**                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| HDFS                                                       | `hdfs`                                                       |
| Google GCS                                                 | `gs`                                                         |
| Azure Blob Storage                                         | <ul><li>ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `wasb` です。</li><li>ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `wasbs` です。</li></ul> |
| Azure Data Lake Storage Gen1                               | `adl`                                                        |
| Azure Data Lake Storage Gen2                               | <ul><li>ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `abfs` です。</li><li>ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `abfss` です。</li></ul> |
| AWS S3 またはその他の S3 互換ストレージ（例: MinIO） | `s3`                                                         |

## Iceberg データベースを削除する

StarRocks の内部データベースと同様に、Iceberg データベースで [DROP](../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、[DROP DATABASE](../../sql-reference/sql-statements/Database/DROP_DATABASE.md) ステートメントを使用してその Iceberg データベースを削除できます。この機能は v3.1 以降でサポートされています。空のデータベースのみを削除できます。

> **注意**
>
> [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

Iceberg データベースを削除すると、そのデータベースのファイルパスは HDFS クラスターまたはクラウドストレージ上に残りますが、データベースと共に削除されることはありません。

[Iceberg カタログに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)、その後、以下のステートメントを使用してそのカタログ内の Iceberg データベースを削除します。

```SQL
DROP DATABASE <database_name>;
```

## Iceberg テーブルを作成する

StarRocks の内部データベースと同様に、Iceberg データベースで [CREATE TABLE](../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) または [CREATE TABLE AS SELECT](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) ステートメントを使用してその Iceberg データベース内にテーブルを作成できます。この機能は v3.1 以降でサポートされています。

> **注意**
>
> [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

[Iceberg カタログとその中のデータベースに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)、その後、以下の構文を使用してそのデータベース内に Iceberg テーブルを作成します。

### 構文

```SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...
partition_column_definition1,partition_column_definition2...])
[partition_desc]
[PROPERTIES ("key" = "value", ...)]
[AS SELECT query]
```

### パラメータ

#### column_definition

`column_definition` の構文は以下の通りです。

```SQL
col_name col_type [COMMENT 'comment']
```

以下の表は、パラメータを説明しています。

| **パラメータ**     | **説明**                                              |
| ----------------- |------------------------------------------------------ |
| col_name          | カラムの名前。                                                |
| col_type          | カラムのデータ型。サポートされるデータ型は TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、DATE、DATETIME、CHAR、VARCHAR[(length)]、ARRAY、MAP、および STRUCT です。LARGEINT、HLL、および BITMAP データ型はサポートされていません。 |

> **注意**
>
> すべての非パーティションカラムは、デフォルト値として `NULL` を使用する必要があります。つまり、テーブル作成ステートメントで各非パーティションカラムに対して `DEFAULT "NULL"` を指定する必要があります。さらに、パーティションカラムは非パーティションカラムの後に定義され、デフォルト値として `NULL` を使用することはできません。

#### partition_desc

`partition_desc` の構文は以下の通りです。

```SQL
PARTITION BY (par_col1[, par_col2...])
```

現在、StarRocks は [identity transforms](https://iceberg.apache.org/spec/#partitioning) のみをサポートしており、これは StarRocks が各ユニークなパーティション値に対してパーティションを作成することを意味します。

> **注意**
>
> パーティションカラムは非パーティションカラムの後に定義される必要があります。パーティションカラムは FLOAT、DOUBLE、DECIMAL、および DATETIME を除くすべてのデータ型をサポートし、デフォルト値として `NULL` を使用することはできません。

#### PROPERTIES

`"key" = "value"` 形式でテーブル属性を `PROPERTIES` に指定できます。詳細は [Iceberg テーブル属性](https://iceberg.apache.org/docs/latest/configuration/) を参照してください。

以下の表は、いくつかの主要なプロパティを説明しています。

| **プロパティ**      | **説明**                                              |
| ----------------- | ------------------------------------------------------------ |
| location          | Iceberg テーブルを作成したいファイルパス。メタストアとして HMS を使用する場合、`location` パラメータを指定する必要はありません。StarRocks は現在の Iceberg カタログのデフォルトのファイルパスにテーブルを作成します。メタストアとして AWS Glue を使用する場合:<ul><li>テーブルを作成したいデータベースに対して `location` パラメータを指定している場合、テーブルに対して `location` パラメータを指定する必要はありません。この場合、テーブルは所属するデータベースのファイルパスにデフォルトで作成されます。</li><li>テーブルを作成したいデータベースに対して `location` を指定していない場合、テーブルに対して `location` パラメータを指定する必要があります。</li></ul> |
| file_format       | Iceberg テーブルのファイル形式。Parquet 形式のみがサポートされています。デフォルト値: `parquet`。 |
| compression_codec | Iceberg テーブルに使用される圧縮アルゴリズム。サポートされる圧縮アルゴリズムは SNAPPY、GZIP、ZSTD、および LZ4 です。デフォルト値: `gzip`。 |

### 例

1. `unpartition_tbl` という名前の非パーティションテーブルを作成します。このテーブルは `id` と `score` の二つのカラムで構成されています。

   ```SQL
   CREATE TABLE unpartition_tbl
   (
       id int,
       score double
   );
   ```

2. `partition_tbl_1` という名前のパーティションテーブルを作成します。このテーブルは `action`、`id`、および `dt` の三つのカラムで構成されており、`id` と `dt` がパーティションカラムとして定義されています。

   ```SQL
   CREATE TABLE partition_tbl_1
   (
       action varchar(20),
       id int,
       dt date
   )
   PARTITION BY (id,dt);
   ```

3. 既存のテーブル `partition_tbl_1` をクエリし、そのクエリ結果に基づいて `partition_tbl_2` という名前のパーティションテーブルを作成します。`partition_tbl_2` では、`id` と `dt` がパーティションカラムとして定義されています。

   ```SQL
   CREATE TABLE partition_tbl_2
   PARTITION BY (id, dt)
   AS SELECT * from employee;
   ```

## Iceberg テーブルにデータをシンクする

StarRocks の内部テーブルと同様に、Iceberg テーブルで [INSERT](../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、[INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用して StarRocks テーブルのデータをその Iceberg テーブルにシンクできます（現在、Parquet 形式の Iceberg テーブルのみがサポートされています）。この機能は v3.1 以降でサポートされています。

> **注意**
>
> [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

[Iceberg カタログとその中のデータベースに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)、その後、以下の構文を使用して StarRocks テーブルのデータをそのデータベース内の Parquet 形式の Iceberg テーブルにシンクします。

### 構文

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 指定されたパーティションにデータをシンクしたい場合、以下の構文を使用します:
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

> **注意**
>
> パーティションカラムは `NULL` 値を許可しません。したがって、Iceberg テーブルのパーティションカラムに空の値がロードされないようにする必要があります。

### パラメータ

| パラメータ   | 説明                                                         |
| ----------- | ------------------------------------------------------------ |
| INTO        | StarRocks テーブルのデータを Iceberg テーブルに追加します。                                       |
| OVERWRITE   | Iceberg テーブルの既存のデータを StarRocks テーブルのデータで上書きします。                                       |
| column_name | データをロードしたい宛先カラムの名前。一つ以上のカラムを指定できます。複数のカラムを指定する場合、カンマ (`,`) で区切ります。実際に Iceberg テーブルに存在するカラムのみを指定できます。また、指定する宛先カラムには Iceberg テーブルのパーティションカラムを含める必要があります。指定する宛先カラムは、StarRocks テーブルのカラムと順番に一対一でマッピングされます。宛先カラムが指定されていない場合、データは Iceberg テーブルのすべてのカラムにロードされます。StarRocks テーブルの非パーティションカラムが Iceberg テーブルのカラムにマッピングできない場合、StarRocks は Iceberg テーブルカラムにデフォルト値 `NULL` を書き込みます。INSERT ステートメントに含まれるクエリステートメントの戻りカラムタイプが宛先カラムのデータ型と異なる場合、StarRocks は不一致のカラムに対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。 |
| expression  | 宛先カラムに値を割り当てる式。                                   |
| DEFAULT     | 宛先カラムにデフォルト値を割り当てます。                                         |
| query       | Iceberg テーブルにロードされる結果を持つクエリステートメント。StarRocks がサポートする任意の SQL ステートメントを使用できます。 |
| PARTITION   | データをロードしたいパーティション。Iceberg テーブルのすべてのパーティションカラムをこのプロパティに指定する必要があります。このプロパティに指定するパーティションカラムは、テーブル作成ステートメントで定義したパーティションカラムと異なる順序であっても構いません。このプロパティを指定する場合、`column_name` プロパティを指定することはできません。 |

### 例

1. `partition_tbl_1` テーブルに三つのデータ行を挿入します。

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. 簡単な計算を含む SELECT クエリの結果を `partition_tbl_1` テーブルに挿入します。

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. `partition_tbl_1` テーブルからデータを読み取る SELECT クエリの結果を同じテーブルに挿入します。

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. `partition_tbl_2` テーブルの `dt='2023-09-01'` および `id=1` の条件を満たすパーティションに SELECT クエリの結果を挿入します。

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. `partition_tbl_1` テーブルの `dt='2023-09-01'` および `id=1` の条件を満たすパーティションのすべての `action` カラムの値を `close` で上書きします。

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

## Iceberg テーブルを削除する

StarRocks の内部テーブルと同様に、Iceberg テーブルで [DROP](../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、[DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) ステートメントを使用してその Iceberg テーブルを削除できます。この機能は v3.1 以降でサポートされています。

> **注意**
>
> [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

Iceberg テーブルを削除すると、そのテーブルのファイルパスとデータは HDFS クラスターまたはクラウドストレージ上に残りますが、テーブルと共に削除されることはありません。

Iceberg テーブルを強制的に削除する場合（つまり、DROP TABLE ステートメントで `FORCE` キーワードを指定した場合）、そのテーブルのデータは HDFS クラスターまたはクラウドストレージ上でテーブルと共に削除されますが、テーブルのファイルパスは保持されます。

[Iceberg カタログとその中のデータベースに切り替えます](#switch-to-an-iceberg-catalog-and-a-database-in-it)、その後、以下のステートメントを使用してそのデータベース内の Iceberg テーブルを削除します。

```SQL
DROP TABLE <table_name> [FORCE];
```

## メタデータキャッシュを設定する

Iceberg クラスターのメタデータファイルは、AWS S3 や HDFS などのリモートストレージに保存されている場合があります。デフォルトでは、StarRocks は Iceberg メタデータをメモリにキャッシュします。クエリを高速化するために、StarRocks は二段階のメタデータキャッシングメカニズムを採用しており、メモリとディスクの両方にメタデータをキャッシュできます。各初回クエリに対して、StarRocks はその計算結果をキャッシュします。以前のクエリと意味的に同等のクエリが発行された場合、StarRocks はまずキャッシュから要求されたメタデータを取得し、キャッシュにヒットしない場合のみリモートストレージからメタデータを取得します。

StarRocks は、最も最近使用されていないものから削除する（Least Recently Used, LRU）アルゴリズムを使用してデータをキャッシュおよび削除します。基本的なルールは以下の通りです。

- StarRocks はまずメモリから要求されたメタデータを取得しようとします。メモリにヒットしない場合、StarRocks はディスクからメタデータを取得しようとします。ディスクから取得したメタデータはメモリにロードされます。ディスクにもヒットしない場合、StarRocks はリモートストレージからメタデータを取得し、取得したメタデータをメモリにキャッシュします。
- StarRocks はメモリから削除されたメタデータをディスクに書き込みますが、ディスクから削除されたメタデータは直接破棄します。

Iceberg メタデータキャッシングメカニズムを設定するために使用できる FE の設定項目は以下の通りです。

##### enable_iceberg_metadata_disk_cache

単位: N/A
デフォルト値: `false`
説明: ディスクキャッシュを有効にするかどうかを指定します。

##### iceberg_metadata_cache_disk_path

単位: N/A
デフォルト値: `StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"`
説明: ディスク上のキャッシュされたメタデータファイルの保存パス。

##### iceberg_metadata_disk_cache_capacity

単位: バイト
デフォルト値: `2147483648`（2 GB に相当）
説明: ディスク上に許可されるキャッシュされたメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_capacity

単位: バイト
デフォルト値: `536870912`（512 MB に相当）
説明: メモリに許可されるキャッシュされたメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_expiration_seconds

単位: 秒  
デフォルト値: `86500`
説明: メモリ内のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_disk_cache_expiration_seconds

単位: 秒  
デフォルト値: `604800`（1 週間に相当）
説明: ディスク上のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_cache_max_entry_size

単位: バイト
デフォルト値: `8388608`（8 MB に相当）
説明: キャッシュ可能なファイルの最大サイズ。このパラメータの値を超えるサイズのファイルはキャッシュできません。クエリがこれらのファイルを要求する場合、StarRocks はリモートストレージからそれらを取得します。