---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Delta Lake catalog

Delta Lake catalog は、Delta Lake からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Delta Lake catalog を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Delta Lake から直接データを変換してロードすることもできます。StarRocks は v2.5 以降で Delta Lake catalog をサポートしています。

Delta Lake クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています：

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ

- Hive メタストアや AWS Glue のようなメタストア

  > **NOTE**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとしては HMS のみを使用できます。

## 使用上の注意

- StarRocks がサポートする Delta Lake のファイル形式は Parquet です。Parquet ファイルは以下の圧縮形式をサポートしています：SNAPPY、LZ4、ZSTD、GZIP、および NO_COMPRESSION。
- StarRocks がサポートしていない Delta Lake のデータ型は MAP と STRUCT です。

## 統合準備

Delta Lake catalog を作成する前に、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Delta Lake クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます：

- インスタンスプロファイル
- 想定ロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication)を参照してください。

### HDFS

ストレージとして HDFS を選択した場合、StarRocks クラスターを次のように構成します：

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするために使用するユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭および各 BE または CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Delta Lake データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります：

  - HDFS クラスターで高可用性 (HA) が有効になっている場合：HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE または CN の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合：HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE または CN の **$BE_HOME/conf** パスに追加します。

> **NOTE**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように構成します：

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE または CN の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Delta Lake catalog を作成する

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

Delta Lake catalog の名前です。命名規則は以下の通りです：

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは1023文字を超えてはなりません。

#### comment

Delta Lake catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `deltalake` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive メタストア

データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように構成します：

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Delta Lake データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive メタストアにアクセスできない可能性があります。

以下の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive メタストアの URI です。形式：`thrift://<metastore_IP_address>:<metastore_port>`。<br />高可用性 (HA) が Hive メタストアで有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例：`"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。以下のいずれかのアクションを実行します：

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します：

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します：

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します：

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
| hive.metastore.type           | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値：`true` および `false`。デフォルト値：`false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。想定ロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例：`us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS Glue にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

Delta Lake クラスターのストレージとして AWS S3 を選択した場合、以下のいずれかのアクションを実行します：

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値：`true` および `false`。デフォルト値：`false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。想定ロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例：`us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS S3 にアクセスするための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

Delta Lake catalog は v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム (例: MinIO) を Delta Lake クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように構成して、統合を成功させます：

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
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値：`true` および `false`。デフォルト値：`true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値：`true` および `false`。デフォルト値：`false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Delta Lake catalog は v3.0 以降で Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Delta Lake クラスターのストレージとして Blob Storage を選択した場合、以下のいずれかのアクションを実行します：

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する blob コンテナの名前です。        |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Delta Lake クラスターのストレージとして Data Lake Storage Gen2 を選択した場合、以下のいずれかのアクションを実行します：

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Delta Lake クラスターのストレージとして Data Lake Storage Gen1 を選択した場合、以下のいずれかのアクションを実行します：

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Delta Lake catalog は v3.0 以降で Google GCS をサポートしています。

Delta Lake クラスターのストレージとして Google GCS を選択した場合、以下のいずれかのアクションを実行します：

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>",
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します：

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。            |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。       |

#### MetadataUpdateParams

StarRocks が Delta Lake のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

v3.3.3 以降、Delta Lake Catalog は [メタデータのローカルキャッシュと取得](#appendix-metadata-local-cache-and-retrieval) をサポートしています。ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Delta Lake のデータ更新頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化できます。

> **NOTE**
>
> ほとんどの場合、Delta Lake データが 1 時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。

| **Parameter**                                      | **Unit** | **Default** | **Description**                                |
|----------------------------------------------------| -------- | ------------------------------------------------------------ |
| enable_deltalake_table_cache                       | -        | true         | Delta Lake のメタデータキャッシュで Table Cache を有効にするかどうか。 |
| enable_deltalake_json_meta_cache                   | -        | true         | Delta Log JSON ファイルのキャッシュを有効にするかどうか。 |
| deltalake_json_meta_cache_ttl_sec                  | Second   | 48 * 60 * 60 | Delta Log JSON ファイルキャッシュの有効期限 (TTL)。 |
| deltalake_json_meta_cache_memory_usage_ratio       | -        | 0.1          | Delta Log JSON ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |
| enable_deltalake_checkpoint_meta_cache             | -        | true         | Delta Log Checkpoint ファイルのキャッシュを有効にするかどうか。 |
| deltalake_checkpoint_meta_cache_ttl_sec            | Second   | 48 * 60 * 60 | Delta Log Checkpoint ファイルキャッシュの有効期限 (TTL)。  |
| deltalake_checkpoint_meta_cache_memory_usage_ratio | -        | 0.1          | Delta Log Checkpoint ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、Delta Lake クラスターからデータをクエリするための `deltalake_catalog_hms` または `deltalake_catalog_glue` という名前の Delta Lake catalog を作成します。

#### HDFS

ストレージとして HDFS を使用する場合、以下のようなコマンドを実行します：

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

##### インスタンスプロファイルベースのクレデンシャルを選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

##### 想定ロールベースのクレデンシャルを選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

##### IAM ユーザーベースのクレデンシャルを選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

MinIO を例にとります。以下のようなコマンドを実行します：

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

#### Microsoft Azure Storage

##### Azure Blob Storage

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージドサービス ID 認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージド ID 認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- インパーソネーションベースの認証方法を選択する場合：

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します：

    ```SQL
    CREATE EXTERNAL CATALOG deltalake_catalog_hms
    PROPERTIES
    (
        "type" = "deltalake",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します：

    ```SQL
    CREATE EXTERNAL CATALOG deltalake_catalog_hms
    PROPERTIES
    (
        "type" = "deltalake",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## Delta Lake catalog を表示する

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます：

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。以下の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog の作成ステートメントをクエリします：

```SQL
SHOW CREATE CATALOG deltalake_catalog_glue;
```

## Delta Lake Catalog とその中のデータベースに切り替える

Delta Lake catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます：

- 現在のセッションで Delta Lake catalog を指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します：

  ```SQL
  -- 現在のセッションで指定された catalog に切り替えます：
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定します：
  USE <db_name>
  ```

- 直接 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用して Delta Lake catalog とその中のデータベースに切り替えます：

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Delta Lake catalog を削除する

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

以下の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog を削除します：

```SQL
DROP Catalog deltalake_catalog_glue;
```

## Delta Lake テーブルのスキーマを表示する

Delta Lake テーブルのスキーマを表示するには、次のいずれかの構文を使用できます：

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Delta Lake テーブルをクエリする

1. Delta Lake クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Delta Lake Catalog とその中のデータベースに切り替える](#switch-to-a-delta-lake-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の対象テーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Delta Lake からデータをロードする

`olap_tbl` という名前の OLAP テーブルがあると仮定して、以下のようにデータを変換してロードできます：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM deltalake_table
```

## メタデータキャッシュと更新戦略を構成する

v3.3.3 以降、Delta Lake Catalog は [メタデータのローカルキャッシュと取得](#appendix-metadata-local-cache-and-retrieval) をサポートしています。

次の FE パラメータを通じて Delta Lake メタデータキャッシュの更新を構成できます：

| **Configuration item**                                       | **Default** | **Description**                                               |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------- |
| enable_background_refresh_connector_metadata                 | `true`      | 定期的な Delta Lake メタデータキャッシュの更新を有効にするかどうか。これを有効にすると、StarRocks は Delta Lake クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Delta Lake catalog のキャッシュされたメタデータを更新してデータの変更を検知します。`true` は Delta Lake メタデータキャッシュの更新を有効にし、`false` は無効にします。 |
| background_refresh_metadata_interval_millis                  | `600000`    | 2 回の連続した Delta Lake メタデータキャッシュ更新の間隔。単位：ミリ秒。 |
| background_refresh_metadata_time_secs_since_last_access_secs | `86400`     | Delta Lake メタデータキャッシュ更新タスクの有効期限。アクセスされた Delta Lake catalog について、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Delta Lake catalog については、StarRocks はそのキャッシュされたメタデータを更新しません。単位：秒。 |

## 付録：メタデータのローカルキャッシュと取得

メタデータファイルの繰り返しの解凍と解析は不要な遅延を引き起こす可能性があるため、StarRocks はデシリアライズされたメモリオブジェクトをキャッシュする新しいメタデータキャッシュ戦略を採用しています。これらのデシリアライズされたファイルをメモリに保存することで、後続のクエリで解凍と解析の段階をスキップできます。このキャッシングメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ需要やマテリアライズドビューの書き換えニーズに対応しやすくなります。

この動作は、Catalog プロパティ [MetadataUpdateParams](#metadataupdateparams) および [関連する構成項目](#configure-metadata-cache-and-update-strategy) を通じて構成できます。

## 機能サポート

現在、Delta Lake catalog は以下のテーブル機能をサポートしています：

- V2 Checkpoint (v3.3.0 以降)
- タイムゾーンなしのタイムスタンプ (v3.3.1 以降)
- カラムマッピング (v3.3.6 以降)
- Deletion Vector (v3.4.1 以降)