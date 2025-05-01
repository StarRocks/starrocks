---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Hudi catalog

Hudi catalog は、Apache Hudi からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Hudi catalogs を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Hudi からデータを直接変換してロードすることもできます。StarRocks は v2.4 以降で Hudi catalogs をサポートしています。

Hudi クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ

- Hive metastore や AWS Glue のようなメタストア

  > **NOTE**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。

## 使用上の注意

- StarRocks がサポートする Hudi のファイル形式は Parquet です。Parquet ファイルは以下の圧縮形式をサポートしています: SNAPPY、LZ4、ZSTD、GZIP、および NO_COMPRESSION。
- StarRocks は Hudi の Copy On Write (COW) テーブルと Merge On Read (MOR) テーブルを完全にサポートしています。

## 統合準備

Hudi catalog を作成する前に、StarRocks クラスターが Hudi クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Hudi クラスターがストレージとして AWS S3 を使用している場合、またはメタストアとして AWS Glue を使用している場合、適切な認証方法を選択し、関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、 [AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication) を参照してください。

### HDFS

ストレージとして HDFS を選択した場合、StarRocks クラスターを以下のように構成します。

- (オプション) HDFS クラスターと Hive metastore にアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターと Hive metastore にアクセスするために FE と BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭と、各 BE または CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加してユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE と各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Hudi データをクエリする際、StarRocks クラスターの FEs と BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。以下の状況でのみ StarRocks クラスターを構成する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。

> **NOTE**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive metastore に対して Kerberos 認証が有効になっている場合、StarRocks クラスターを以下のように構成します。

- 各 FE と各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターと Hive metastore にアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルと各 BE または CN の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

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

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### comment

Hudi catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `hudi` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive metastore

データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を以下のように構成します。

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Hudi データをクエリする前に、Hive metastore ノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive metastore にアクセスできない可能性があります。

以下の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Hudi クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive metastore の URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />高可用性 (HA) が Hive metastore に対して有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。以下のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択するには、`MetastoreParams` を以下のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサインされたロールベースの認証方法を選択するには、`MetastoreParams` を以下のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`MetastoreParams` を以下のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Hudi クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、 [AWS Glue にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue) を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

ストレージとして HDFS を使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

Hudi クラスターのストレージとして AWS S3 を選択した場合、以下のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインされたロールベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするためにアサインされたロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、 [AWS S3 にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージシステム

Hudi catalogs は v2.5 以降で S3 互換ストレージシステムをサポートしています。

Hudi クラスターのストレージとして MinIO などの S3 互換ストレージシステムを選択した場合、`StorageCredentialParams` を以下のように構成して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイルの URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。たとえば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` という名前のバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイルの URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Hudi catalogs は v3.0 以降で Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Hudi クラスターのストレージとして Blob Storage を選択した場合、以下のいずれかの操作を行います。

- Shared Key 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

- SAS Token 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する blob コンテナの名前です。                  |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Hudi クラスターのストレージとして Data Lake Storage Gen2 を選択した場合、以下のいずれかの操作を行います。

- Managed Identity 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | Managed Identity 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。                      |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。     |

- Shared Key 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- Service Principal 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Hudi クラスターのストレージとして Data Lake Storage Gen1 を選択した場合、以下のいずれかの操作を行います。

- Managed Service Identity 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | Managed Service Identity 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- Service Principal 認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Hudi catalogs は v3.0 以降で Google GCS をサポートしています。

Hudi クラスターのストレージとして Google GCS を選択した場合、以下のいずれかの操作を行います。

- VM ベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキーです。 |

- インパーソネーションベースの認証方法を選択するには、`StorageCredentialParams` を以下のように構成します。

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。               |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。         |

#### MetadataUpdateParams

StarRocks が Hudi のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

StarRocks はデフォルトで [自動非同期更新ポリシー](#appendix-understand-metadata-automatic-asynchronous-update) を実装しています。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Hudi のデータ更新頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化することができます。

> **NOTE**
>
> ほとんどの場合、Hudi データが 1 時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。

| Parameter                              | Required | Description                                                  |
|----------------------------------------| -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | No       | StarRocks が Hudi テーブルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| enable_remote_file_cache               | No       | StarRocks が Hudi テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | No       | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションのメタデータを非同期で更新する時間間隔です。<br />単位: 秒。デフォルト値: `7200` (2 時間)。 |
| remote_file_cache_refresh_interval_sec | No       | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションの基礎データファイルのメタデータを非同期で更新する時間間隔です。<br />単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | No       | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションのメタデータを自動的に破棄する時間間隔です。<br />単位: 秒。デフォルト値: `86400` (24 時間)。 |
| remote_file_cache_ttl_sec              | No       | StarRocks が自身にキャッシュされた Hudi テーブルまたはパーティションの基礎データファイルのメタデータを自動的に破棄する時間間隔です。<br />単位: 秒。デフォルト値: `129600` (36 時間)。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、`hudi_catalog_hms` または `hudi_catalog_glue` という名前の Hudi catalog を作成し、Hudi クラスターからデータをクエリします。

#### HDFS

ストレージとして HDFS を使用する場合、以下のようなコマンドを実行します。

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

##### インスタンスプロファイルベースの認証情報を選択した場合

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

##### アサインされたロールベースの認証情報を選択した場合

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

##### IAM ユーザーベースの認証情報を選択した場合

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

MinIO を例として使用します。以下のようなコマンドを実行します。

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

- Shared Key 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- SAS Token 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- Managed Service Identity 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- Service Principal 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- Managed Identity 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- Shared Key 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- Service Principal 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

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

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します。

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

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

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

  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

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

## Hudi catalogs を表示する

現在の StarRocks クラスター内のすべての catalogs をクエリするには、 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例では、`hudi_catalog_glue` という名前の Hudi catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG hudi_catalog_glue;
```

## Hudi Catalog とその中のデータベースに切り替える

Hudi catalog とその中のデータベースに切り替えるには、以下のいずれかの方法を使用します。

- 現在のセッションで指定された Hudi catalog に切り替えるには、 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。

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

## Hudi catalog を削除する

外部 catalog を削除するには、 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例では、`hudi_catalog_glue` という名前の Hudi catalog を削除します。

```SQL
DROP Catalog hudi_catalog_glue;
```

## Hudi テーブルのスキーマを表示する

Hudi テーブルのスキーマを表示するには、以下のいずれかの構文を使用します。

- スキーマを表示する

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示する

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Hudi テーブルをクエリする

1. Hudi クラスター内のデータベースを表示するには、 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Hudi Catalog とその中のデータベースに切り替える](#switch-to-a-hudi-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の宛先テーブルをクエリするには、 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Hudi からデータをロードする

`olap_tbl` という名前の OLAP テーブルがあると仮定し、以下のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM hudi_table
```

## メタデータキャッシュを手動または自動で更新する

### 手動更新

デフォルトでは、StarRocks は Hudi のメタデータをキャッシュし、非同期モードでメタデータを自動的に更新して、より良いパフォーマンスを提供します。さらに、Hudi テーブルでいくつかのスキーマ変更やテーブル更新が行われた後、 [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) を使用してメタデータを手動で更新することもできます。これにより、StarRocks が最新のメタデータをできるだけ早く取得し、適切な実行プランを生成できるようになります。

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```

## 付録: メタデータ自動非同期更新を理解する

自動非同期更新は、StarRocks が Hudi catalogs のメタデータを更新するために使用するデフォルトのポリシーです。

デフォルトでは (つまり、`enable_metastore_cache` および `enable_remote_file_cache` パラメータが両方とも `true` に設定されている場合)、クエリが Hudi テーブルのパーティションにヒットすると、StarRocks はそのパーティションのメタデータとそのパーティションの基礎データファイルのメタデータを自動的にキャッシュします。キャッシュされたメタデータは、遅延更新ポリシーを使用して更新されます。

たとえば、`table2` という名前の Hudi テーブルがあり、4 つのパーティション `p1`、`p2`、`p3`、`p4` があるとします。クエリが `p1` にヒットすると、StarRocks は `p1` のメタデータと `p1` の基礎データファイルのメタデータをキャッシュします。キャッシュされたメタデータを更新および破棄するデフォルトの時間間隔は次のとおりです。

- `p1` のキャッシュされたメタデータを非同期で更新する時間間隔 ( `metastore_cache_refresh_interval_sec` パラメータで指定) は 2 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを非同期で更新する時間間隔 ( `remote_file_cache_refresh_interval_sec` パラメータで指定) は 60 秒です。
- `p1` のキャッシュされたメタデータを自動的に破棄する時間間隔 ( `metastore_cache_ttl_sec` パラメータで指定) は 24 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを自動的に破棄する時間間隔 ( `remote_file_cache_ttl_sec` パラメータで指定) は 36 時間です。

以下の図は、理解を容易にするために、タイムライン上の時間間隔を示しています。

![キャッシュされたメタデータの更新と破棄のタイムライン](../../_assets/catalog_timeline.png)

その後、StarRocks は次のルールに従ってメタデータを更新または破棄します。

- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒未満の場合、StarRocks は `p1` のキャッシュされたメタデータまたは `p1` の基礎データファイルのキャッシュされたメタデータを更新しません。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒を超える場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを更新します。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 2 時間を超える場合、StarRocks は `p1` のキャッシュされたメタデータを更新します。
- `p1` が最後の更新から 24 時間以内にアクセスされていない場合、StarRocks は `p1` のキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。
- `p1` が最後の更新から 36 時間以内にアクセスされていない場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。