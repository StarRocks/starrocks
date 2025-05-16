---
description: Delta Lake から直接データをクエリする
displayed_sidebar: docs
---
import Intro from '../../_assets/catalog/_deltalake_intro.mdx'
import DatabricksParams from '../../_assets/catalog/_databricks_params.mdx'

# Delta Lake catalog

<Intro />

## 使用上の注意

- StarRocks がサポートする Delta Lake のファイル形式は Parquet です。Parquet ファイルは次の圧縮形式をサポートしています: SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION。
- StarRocks がサポートしていない Delta Lake のデータ型は MAP と STRUCT です。

## 統合準備

Delta Lake catalog を作成する前に、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Delta Lake クラスターが AWS S3 をストレージとして使用している場合、または AWS Glue をメタストアとして使用している場合、適切な認証方法を選択し、関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです:

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、 [Preparation for authentication in AWS IAM](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication) を参照してください。

### HDFS

HDFS をストレージとして選択した場合、StarRocks クラスターを次のように構成します:

- (オプション) HDFS クラスターと Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は FE と BE または CN プロセスのユーザー名を使用して HDFS クラスターと Hive メタストアにアクセスします。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭と、各 BE または CN の **be/conf/hadoop_env.sh** または **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルにユーザー名を設定した後、各 FE と各 BE または CN を再起動して、パラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名しか設定できません。
- Delta Lake データをクエリする際、StarRocks クラスターの FEs と BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります:

  - 高可用性 (HA) が HDFS クラスターで有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと、各 BE または CN の **$BE_HOME/conf** または **$CN_HOME/conf** パスに追加します。
  - View File System (ViewFs) が HDFS クラスターで有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと、各 BE または CN の **$BE_HOME/conf** または **$CN_HOME/conf** パスに追加します。

  :::note
  クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。
  :::

### Kerberos 認証

Kerberos 認証が HDFS クラスターまたは Hive メタストアで有効になっている場合、StarRocks クラスターを次のように構成します:

- 各 FE と各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターと Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用して定期的にこのコマンドを実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルと、各 BE または CN の **$BE_HOME/conf/be.conf** または **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

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

Delta Lake catalog の名前です。命名規則は次の通りです:

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは1023文字を超えてはなりません。

#### comment

Delta Lake catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `deltalake` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive メタストア

データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように構成します:

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note
Delta Lake データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始したときに StarRocks が Hive メタストアにアクセスできない可能性があります。
:::

次の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />高可用性 (HA) が Hive メタストアで有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。次のいずれかの操作を行います:

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

次の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Delta Lake クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。アサインされたロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、 [Authentication parameters for accessing AWS Glue](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue) を参照してください。

<DatabricksParams />

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

Delta Lake クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います:

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、 [Authentication parameters for accessing AWS S3](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージシステム

Delta Lake catalogs は v2.5 以降、S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム (例: MinIO) を Delta Lake クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように構成して、統合を成功させます:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Delta Lake catalogs は v3.0 以降、Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Blob Storage を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する Blob コンテナの名前です。                  |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。                    |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。   |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Delta Lake catalogs は v3.0 以降、Google GCS をサポートしています。

Google GCS を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>",
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。              |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。       |

#### MetadataUpdateParams

StarRocks が Delta Lake のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。ほとんどの場合、`MetadataUpdateParams` を無視しても問題なく、ポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使えるパフォーマンスを提供します。

ただし、Delta Lake のデータ更新頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化することができます。

:::note
ほとんどの場合、Delta Lake データが1時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。
:::

| **Parameter**                                      | **Unit** | **Default** | **Description**                                |
|----------------------------------------------------| -------- | -------------|----------------------------------------------- |
| enable_deltalake_table_cache                       | -        | true         | Delta Lake のメタデータキャッシュで Table Cache を有効にするかどうか。 |
| enable_deltalake_json_meta_cache                   | -        | true         | Delta Log JSON ファイルのキャッシュを有効にするかどうか。 |
| deltalake_json_meta_cache_ttl_sec                  | Second   | 48 * 60 * 60 | Delta Log JSON ファイルキャッシュの有効期限 (TTL)。 |
| deltalake_json_meta_cache_memory_usage_ratio       | -        | 0.1          | Delta Log JSON ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |
| enable_deltalake_checkpoint_meta_cache             | -        | true         | Delta Log Checkpoint ファイルのキャッシュを有効にするかどうか。 |
| deltalake_checkpoint_meta_cache_ttl_sec            | Second   | 48 * 60 * 60 | Delta Log Checkpoint ファイルキャッシュの有効期限 (TTL)。  |
| deltalake_checkpoint_meta_cache_memory_usage_ratio | -        | 0.1          | Delta Log Checkpoint ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |

### 例

次の例では、使用するメタストアのタイプに応じて `deltalake_catalog_hms` または `deltalake_catalog_glue` という名前の Delta Lake catalog を作成し、Delta Lake クラスターからデータをクエリします。

#### HDFS

HDFS をストレージとして使用する場合、次のようなコマンドを実行します:

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

- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:

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

##### アサインされたロールベースのクレデンシャルを選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:

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

- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:

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

- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:

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

MinIO を例にとります。次のようなコマンドを実行します:

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

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します:

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

- SAS トークン認証方法を選択する場合、次のようなコマンドを実行します:

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

- マネージドサービス ID 認証方法を選択する場合、次のようなコマンドを実行します:

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

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します:

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

- マネージド ID 認証方法を選択する場合、次のようなコマンドを実行します:

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

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します:

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

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します:

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

- VM ベースの認証方法を選択する場合、次のようなコマンドを実行します:

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

- サービスアカウントベースの認証方法を選択する場合、次のようなコマンドを実行します:

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

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します:

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

  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します:

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

## Delta Lake catalogs の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます:

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。次の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog の作成ステートメントをクエリします:

```SQL
SHOW CREATE CATALOG deltalake_catalog_glue;
```

## Delta Lake Catalog とその中のデータベースへの切り替え

Delta Lake catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます:

- 現在のセッションで Delta Lake catalog を指定するには、 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、次に [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します:

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

- 直接 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、Delta Lake catalog とその中のデータベースに切り替えます:

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Delta Lake catalog の削除

外部 catalog を削除するには、 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

次の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog を削除します:

```SQL
DROP Catalog deltalake_catalog_glue;
```

## Delta Lake テーブルのスキーマを表示

Delta Lake テーブルのスキーマを表示するには、次のいずれかの構文を使用できます:

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Delta Lake テーブルをクエリ

1. Delta Lake クラスター内のデータベースを表示するには、 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します:

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Delta Lake Catalog とその中のデータベースへの切り替え](#switch-to-a-delta-lake-catalog-and-a-database-in-it) を行います。

3. 指定されたデータベース内の宛先テーブルをクエリするには、 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Delta Lake からデータをロード

`olap_tbl` という名前の OLAP テーブルがあると仮定すると、次のようにデータを変換してロードできます:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM deltalake_table
```

## メタデータキャッシュと更新戦略の構成

v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。

次の FE パラメータを通じて Delta Lake メタデータキャッシュの更新を構成できます:

| **Configuration item**                                       | **Default** | **Description**                                               |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------- |
| enable_background_refresh_connector_metadata                 | `true`      | 定期的な Delta Lake メタデータキャッシュの更新を有効にするかどうか。これを有効にすると、StarRocks は Delta Lake クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Delta Lake catalog のキャッシュされたメタデータを更新してデータの変更を検知します。`true` は Delta Lake メタデータキャッシュの更新を有効にし、`false` は無効にします。 |
| background_refresh_metadata_interval_millis                  | `600000`    | 2 つの連続した Delta Lake メタデータキャッシュ更新の間隔。単位: ミリ秒。 |
| background_refresh_metadata_time_secs_since_last_access_secs | `86400`     | Delta Lake メタデータキャッシュ更新タスクの有効期限。アクセスされた Delta Lake catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Delta Lake catalog に対して、StarRocks はそのキャッシュされたメタデータを更新しません。単位: 秒。 |

## 付録: メタデータローカルキャッシュと取得

メタデータファイルの繰り返しの解凍と解析は不要な遅延を引き起こす可能性があるため、StarRocks はデシリアライズされたメモリオブジェクトをキャッシュする新しいメタデータキャッシュ戦略を採用しています。これらのデシリアライズされたファイルをメモリに保存することで、システムは後続のクエリで解凍と解析の段階をバイパスできます。このキャッシュメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ需要やマテリアライズドビューの書き換えニーズに対応しやすくなります。

この動作は、Catalog プロパティ [MetadataUpdateParams](#metadataupdateparams) と [関連する構成項目](#configure-metadata-cache-and-update-strategy) を通じて構成できます。

## 機能サポート

現在、Delta Lake catalogs は次のテーブル機能をサポートしています:

- V2 Checkpoint (v3.3.0 以降)
- タイムゾーンなしのタイムスタンプ (v3.3.1 以降)
- カラムマッピング (v3.3.6 以降)
- Deletion Vector (v3.4.1 以降)