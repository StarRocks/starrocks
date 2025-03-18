---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Hudi catalog

Hudi catalog は、Apache Hudi からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Hudi catalogs に基づいて [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Hudi からデータを直接変換してロードすることもできます。StarRocks は v2.4 以降で Hudi catalogs をサポートしています。

Hudi クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ

- Hive メタストアや AWS Glue のようなメタストア

  > **NOTE**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。

## 使用上の注意

- StarRocks がサポートする Hudi のファイル形式は Parquet です。Parquet ファイルは以下の圧縮形式をサポートしています: SNAPPY、LZ4、ZSTD、GZIP、NO_COMPRESSION。
- StarRocks は Hudi の Copy On Write (COW) テーブルと Merge On Read (MOR) テーブルを完全にサポートしています。

## 統合準備

Hudi catalog を作成する前に、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Hudi クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

### HDFS

HDFS をストレージとして選択した場合、StarRocks クラスターを次のように設定してください。

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするためのユーザー名を設定します。デフォルトでは、StarRocks は FE および BE または CN プロセスのユーザー名を使用して HDFS クラスターおよび Hive メタストアにアクセスします。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭、および各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動して、パラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Hudi データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。次の状況でのみ、StarRocks クラスターを設定する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

> **NOTE**
>
> クエリを送信したときに不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定してください。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイル、および各 BE の **$BE_HOME/conf/be.conf** ファイルまたは各 CN の **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

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

Hudi catalog の名前です。命名規則は次のとおりです。

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは1023文字を超えてはいけません。

#### comment

Hudi catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `hudi` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive メタストア

データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように設定します。

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Hudi データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始するときに StarRocks が Hive メタストアにアクセスできない可能性があります。

次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Hudi クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive メタストアで高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。次のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Hudi クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

##### AWS S3

Hudi クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います。

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

次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

Hudi catalogs は v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム (例: MinIO) を Hudi クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例: US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスする場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Hudi catalogs は v3.0 以降で Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Blob Storage を Hudi クラスターのストレージとして選択した場合、次のいずれかの操作を行います。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する blob コンテナの名前です。        |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Hudi クラスターのストレージとして選択した場合、次のいずれかの操作を行います。

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Hudi クラスターのストレージとして選択した場合、次のいずれかの操作を行います。

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Hudi catalogs は v3.0 以降で Google GCS をサポートしています。

Google GCS を Hudi クラスターのストレージとして選択した場合、次のいずれかの操作を行います。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  - VM インスタンスがサービスアカウントをインパーソネートする場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。            |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) が別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートする場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。       |

#### MetadataUpdateParams

StarRocks が Hudi のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

StarRocks はデフォルトで [自動非同期更新ポリシー](#appendix-understand-metadata-automatic-asynchronous-update) を実装しています。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Hudi でのデータ更新の頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化できます。

> **NOTE**
>
> ほとんどの場合、Hudi データが 1 時間以下の粒度で更新される場合、データ更新の頻度は高いと見なされます。

| Parameter                              | Required | Description                                                  |
|----------------------------------------| -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | No       | StarRocks が Hudi テーブルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| enable_remote_file_cache               | No       | StarRocks が Hudi テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | No       | StarRocks が Hudi テーブルまたはパーティションのキャッシュされたメタデータを非同期で更新する時間間隔です。<br />単位: 秒。デフォルト値: `7200` (2 時間)。 |
| remote_file_cache_refresh_interval_sec | No       | StarRocks が Hudi テーブルまたはパーティションの基礎データファイルのキャッシュされたメタデータを非同期で更新する時間間隔です。<br />単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | No       | StarRocks が Hudi テーブルまたはパーティションのキャッシュされたメタデータを自動的に破棄する時間間隔です。<br />単位: 秒。デフォルト値: `86400` (24 時間)。 |
| remote_file_cache_ttl_sec              | No       | StarRocks が Hudi テーブルまたはパーティションの基礎データファイルのキャッシュされたメタデータを自動的に破棄する時間間隔です。<br />単位: 秒。デフォルト値: `129600` (36 時間)。 |

### 例

次の例は、使用するメタストアのタイプに応じて、`hudi_catalog_hms` または `hudi_catalog_glue` という名前の Hudi catalog を作成し、Hudi クラスターからデータをクエリします。

#### HDFS

HDFS をストレージとして使用する場合、次のようなコマンドを実行します。

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

##### インスタンスプロファイルベースのクレデンシャルを選択した場合

- Hudi クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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

##### アサインされたロールベースのクレデンシャルを選択した場合

- Hudi クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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

##### IAM ユーザーベースのクレデンシャルを選択した場合

- Hudi クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Hudi クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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

MinIO を例として使用します。次のようなコマンドを実行します。

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

#### Microsoft Azure Storage

##### Azure Blob Storage

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージドサービス ID 認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージド ID 認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- サービスアカウントベースの認証方法を選択する場合、次のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hudi_catalog_hms
  PROPERTIES
  (
      "type" = "hudi",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスがサービスアカウントをインパーソネートする場合、次のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG hudi_catalog_hms
    PROPERTIES
    (
        "type" = "hudi",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - サービスアカウントが別のサービスアカウントをインパーソネートする場合、次のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG hudi_catalog_hms
    PROPERTIES
    (
        "type" = "hudi",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## Hudi catalogs の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

また、外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。次の例では、`hudi_catalog_glue` という名前の Hudi catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG hudi_catalog_glue;
```

## Hudi Catalog とその中のデータベースに切り替える

Hudi catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます。

- 現在のセッションで Hudi catalog を指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

- 直接 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、Hudi catalog とその中のデータベースに切り替えます。

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Hudi catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

次の例では、`hudi_catalog_glue` という名前の Hudi catalog を削除します。

```SQL
DROP Catalog hudi_catalog_glue;
```

## Hudi テーブルのスキーマを表示する

Hudi テーブルのスキーマを表示するには、次のいずれかの構文を使用できます。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Hudi テーブルをクエリする

1. Hudi クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Hudi Catalog とその中のデータベースに切り替える](#switch-to-a-hudi-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の宛先テーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Hudi からデータをロードする

`olap_tbl` という名前の OLAP テーブルがあると仮定して、次のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM hudi_table
```

## メタデータキャッシュを手動または自動で更新する

### 手動更新

デフォルトでは、StarRocks は Hudi のメタデータをキャッシュし、非同期モードでメタデータを自動的に更新して、より良いパフォーマンスを提供します。さらに、Hudi テーブルでいくつかのスキーマ変更やテーブル更新が行われた後、[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) を使用してメタデータを手動で更新することもできます。これにより、StarRocks が最新のメタデータをできるだけ早く取得し、適切な実行プランを生成できるようにします。

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```

## 付録: メタデータ自動非同期更新を理解する

自動非同期更新は、StarRocks が Hudi catalogs のメタデータを更新するために使用するデフォルトのポリシーです。

デフォルトでは (`enable_metastore_cache` と `enable_remote_file_cache` パラメータが両方とも `true` に設定されている場合)、クエリが Hudi テーブルのパーティションにヒットすると、StarRocks はそのパーティションのメタデータとそのパーティションの基礎データファイルのメタデータを自動的にキャッシュします。キャッシュされたメタデータは、遅延更新ポリシーを使用して更新されます。

たとえば、`table2` という名前の Hudi テーブルがあり、4 つのパーティション `p1`、`p2`、`p3`、`p4` を持っているとします。クエリが `p1` にヒットすると、StarRocks は `p1` のメタデータと `p1` の基礎データファイルのメタデータをキャッシュします。キャッシュされたメタデータを更新および破棄するデフォルトの時間間隔は次のとおりです。

- `p1` のキャッシュされたメタデータを非同期で更新する時間間隔 (`metastore_cache_refresh_interval_sec` パラメータで指定) は 2 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを非同期で更新する時間間隔 (`remote_file_cache_refresh_interval_sec` パラメータで指定) は 60 秒です。
- `p1` のキャッシュされたメタデータを自動的に破棄する時間間隔 (`metastore_cache_ttl_sec` パラメータで指定) は 24 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを自動的に破棄する時間間隔 (`remote_file_cache_ttl_sec` パラメータで指定) は 36 時間です。

次の図は、キャッシュされたメタデータの更新と破棄の時間間隔をタイムラインで示しています。

![キャッシュされたメタデータの更新と破棄のタイムライン](../../_assets/catalog_timeline.png)

その後、StarRocks は次のルールに従ってメタデータを更新または破棄します。

- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒未満の場合、StarRocks は `p1` のキャッシュされたメタデータや `p1` の基礎データファイルのキャッシュされたメタデータを更新しません。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒を超える場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを更新します。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 2 時間を超える場合、StarRocks は `p1` のキャッシュされたメタデータを更新します。
- `p1` が最後の更新から 24 時間以内にアクセスされていない場合、StarRocks は `p1` のキャッシュされたメタデータを破棄します。メタデータは次のクエリでキャッシュされます。
- `p1` が最後の更新から 36 時間以内にアクセスされていない場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを破棄します。メタデータは次のクエリでキャッシュされます。