---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# 統合カタログ

統合カタログは、StarRocks が v3.2 以降で提供する外部カタログの一種で、Apache Hive™、Apache Iceberg、Apache Hudi、Delta Lake のデータソースを取り込みなしで統合データソースとして扱うことができます。統合カタログを使用すると、以下のことが可能です。

- Hive、Iceberg、Hudi、Delta Lake に保存されたデータを直接クエリし、手動でテーブルを作成する必要がありません。
- [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) または非同期マテリアライズドビュー（v2.5 以降でサポート）を使用して、Hive、Iceberg、Hudi、Delta Lake に保存されたデータを処理し、StarRocks にロードします。
- StarRocks 上で操作を行い、Hive および Iceberg のデータベースやテーブルを作成または削除します。

統合データソースでの SQL ワークロードを成功させるためには、StarRocks クラスターが統合データソースのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム（例: MinIO）

- メタストアとして Hive メタストアまたは AWS Glue

  > **注意**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして使用できるのは HMS のみです。

## 制限事項

1 つの統合カタログは、単一のストレージシステムと単一のメタストアサービスとのみ統合できます。したがって、StarRocks と統合したいすべてのデータソースが同じストレージシステムとメタストアサービスを使用していることを確認してください。

## 使用上の注意

- サポートされているファイル形式とデータ型を理解するために、[Hive catalog](../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../../data_source/catalog/iceberg_catalog.md)、[Hudi catalog](../../data_source/catalog/hudi_catalog.md)、および [Delta Lake catalog](../../data_source/catalog/deltalake_catalog.md) の「使用上の注意」セクションを参照してください。

- 特定のテーブル形式にのみ対応した操作がサポートされています。例えば、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) と [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) は Hive と Iceberg のみでサポートされており、[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) は Hive と Hudi のみでサポートされています。

  統合カタログ内で [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントを使用してテーブルを作成する際には、`ENGINE` パラメータを使用してテーブル形式（Hive または Iceberg）を指定してください。

## 統合準備

統合カタログを作成する前に、StarRocks クラスターが統合データソースのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

ストレージとして AWS S3 を使用する場合、またはメタストアとして AWS Glue を使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。詳細については、[AWS リソースへの認証 - 準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

### HDFS

ストレージとして HDFS を選択した場合、StarRocks クラスターを次のように構成します。

- （オプション）HDFS クラスターおよび Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭、および各 BE または CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動して、パラメータ設定を有効にします。StarRocks クラスターごとに 1 つのユーザー名しか設定できません。
- データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります。
  - HDFS クラスターに高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE または CN の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターに View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE または CN の **$BE_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアに Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように構成します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行し、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイル、および各 BE または CN の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は krb5.conf ファイルの保存パスです。必要に応じてパスを変更できます。

## 統合カタログの作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "unified",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### パラメータ

#### catalog_name

統合カタログの名前。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

統合カタログの説明。このパラメータはオプションです。

#### type

データソースのタイプ。値を `unified` に設定します。

#### MetastoreParams

StarRocks がメタストアと統合する方法に関する一連のパラメータ。

##### Hive メタストア

統合データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように構成します。

```SQL
"unified.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive メタストアにアクセスできない可能性があります。

以下の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| パラメータ              | 必須 | 説明                                                  |
| ---------------------- | ---- | ----------------------------------------------------- |
| unified.metastore.type | はい | 統合データソースに使用するメタストアのタイプ。値を `hive` に設定します。 |
| hive.metastore.uris    | はい | Hive メタストアの URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。高可用性 (HA) が Hive メタストアに有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。以下のいずれかの方法を取ります。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します。

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- アサムドロールベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します。

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します。

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下の表は、`MetastoreParams` で構成する必要があるパラメータを説明しています。

| パラメータ                     | 必須 | 説明                                                  |
| ----------------------------- | ---- | ----------------------------------------------------- |
| unified.metastore.type        | はい | 統合データソースに使用するメタストアのタイプ。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | はい | インスタンスプロファイルベースの認証方法およびアサムドロールベースの認証を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | いいえ | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。AWS Glue にアクセスするためにアサムドロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | はい | AWS Glue Data Catalog が存在するリージョン。例: `us-west-1`。 |
| aws.glue.access_key           | いいえ | AWS IAM ユーザーのアクセスキー。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | いいえ | AWS IAM ユーザーのシークレットキー。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS Glue へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータ。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

AWS S3 をストレージとして選択した場合、以下のいずれかの方法を取ります。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサムドロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.use_instance_profile | はい | インスタンスプロファイルベースの認証方法およびアサムドロールベースの認証方法を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。AWS S3 にアクセスするためにアサムドロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | はい | AWS S3 バケットが存在するリージョン。例: `us-west-1`。 |
| aws.s3.access_key           | いいえ | IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ | IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

S3 互換ストレージシステムを選択した場合、例えば MinIO をストレージとして使用する場合、統合を成功させるために `StorageCredentialParams` を次のように構成します。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| パラメータ                        | 必須 | 説明                                                  |
| -------------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.enable_ssl                | はい | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | はい | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | はい | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | はい | IAM ユーザーのアクセスキー。 |
| aws.s3.secret_key                | はい | IAM ユーザーのシークレットキー。 |

##### Microsoft Azure Storage

###### Azure Blob Storage

Blob Storage をストレージとして選択した場合、以下のいずれかの方法を取ります。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**              | **必須** | **説明**                              |
  | -------------------------- | -------- | ------------------------------------- |
  | azure.blob.storage_account | はい     | Blob Storage アカウントのユーザー名。 |
  | azure.blob.shared_key      | はい     | Blob Storage アカウントの共有キー。   |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**             | **必須** | **説明**                                              |
  | ------------------------- | -------- | ----------------------------------------------------- |
  | azure.blob.storage_account| はい     | Blob Storage アカウントのユーザー名。                   |
  | azure.blob.container      | はい     | データを保存する Blob コンテナの名前。                  |
  | azure.blob.sas_token      | はい     | Blob Storage アカウントにアクセスするための SAS トークン。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 をストレージとして選択した場合、以下のいずれかの方法を取ります。

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                           | **必須** | **説明**                                              |
  | --------------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_use_managed_identity | はい     | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | はい     | アクセスしたいデータのテナント ID。                    |
  | azure.adls2.oauth2_client_id            | はい     | マネージド ID のクライアント (アプリケーション) ID。   |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**               | **必須** | **説明**                                              |
  | --------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.storage_account | はい     | Data Lake Storage Gen2 ストレージアカウントのユーザー名。 |
  | azure.adls2.shared_key      | はい     | Data Lake Storage Gen2 ストレージアカウントの共有キー。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                      | **必須** | **説明**                                              |
  | ---------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_client_id       | はい     | サービスプリンシパルのクライアント (アプリケーション) ID。 |
  | azure.adls2.oauth2_client_secret   | はい     | 作成された新しいクライアント (アプリケーション) シークレットの値。 |
  | azure.adls2.oauth2_client_endpoint | はい     | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1)。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 をストレージとして選択した場合、以下のいずれかの方法を取ります。

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                            | **必須** | **説明**                                              |
  | ---------------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls1.use_managed_service_identity | はい     | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                 | **必須** | **説明**                                              |
  | ----------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls1.oauth2_client_id  | はい     | サービスプリンシパルのクライアント (アプリケーション) ID。 |
  | azure.adls1.oauth2_credential | はい     | 作成された新しいクライアント (アプリケーション) シークレットの値。 |
  | azure.adls1.oauth2_endpoint   | はい     | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1)。 |

##### Google GCS

Google GCS をストレージとして選択した場合、以下のいずれかの方法を取ります。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
  | ------------------------------------------ | --------------- | --------- | ----------------------------------------------------- |
  | gcp.gcs.use_compute_engine_service_account | false           | true      | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
  | -------------------------------------- | --------------- | ------------------------------------------------ | ----------------------------------------------------- |
  | gcp.gcs.service_account_email          | ""              | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""              | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"       | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
  | gcp.gcs.service_account_private_key    | ""              | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
    | ------------------------------------------ | --------------- | --------- | ----------------------------------------------------- |
    | gcp.gcs.use_compute_engine_service_account | false           | true      | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""              | "hello"   | インパーソネートしたいサービスアカウント。            |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

    | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
    | -------------------------------------- | --------------- | ------------------------------------------------ | ----------------------------------------------------- |
    | gcp.gcs.service_account_email          | ""              | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""              | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"       | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
    | gcp.gcs.service_account_private_key    | ""              | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""              | "hello"                                          | インパーソネートしたいデータサービスアカウント。       |

#### MetadataUpdateParams

StarRocks が Hive、Hudi、Delta Lake のキャッシュされたメタデータを更新する方法に関する一連のパラメータ。このパラメータセットはオプションです。Hive、Hudi、Delta Lake からキャッシュされたメタデータを更新するポリシーの詳細については、[Hive catalog](../../data_source/catalog/hive_catalog.md)、[Hudi catalog](../../data_source/catalog/hudi_catalog.md)、および [Delta Lake catalog](../../data_source/catalog/deltalake_catalog.md) を参照してください。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。

ただし、Hive、Hudi、Delta Lake のデータ更新頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化できます。

| パラメータ                              | 必須 | 説明                                                  |
| -------------------------------------- | ---- | ----------------------------------------------------- |
| enable_metastore_cache                 | いいえ | StarRocks が Hive、Hudi、または Delta Lake テーブルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。`true` の値はキャッシュを有効にし、`false` の値はキャッシュを無効にします。 |
| enable_remote_file_cache               | いいえ | StarRocks が Hive、Hudi、または Delta Lake テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。`true` の値はキャッシュを有効にし、`false` の値はキャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | いいえ | StarRocks が自身にキャッシュされた Hive、Hudi、または Delta Lake テーブルまたはパーティションのメタデータを非同期で更新する時間間隔。単位: 秒。デフォルト値: `7200`、つまり 2 時間。 |
| remote_file_cache_refresh_interval_sec | いいえ | StarRocks が自身にキャッシュされた Hive、Hudi、または Delta Lake テーブルまたはパーティションの基礎データファイルのメタデータを非同期で更新する時間間隔。単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | いいえ | StarRocks が自身にキャッシュされた Hive、Hudi、または Delta Lake テーブルまたはパーティションのメタデータを自動的に破棄する時間間隔。単位: 秒。デフォルト値: `86400`、つまり 24 時間。 |
| remote_file_cache_ttl_sec              | いいえ | StarRocks が自身にキャッシュされた Hive、Hudi、または Delta Lake テーブルまたはパーティションの基礎データファイルのメタデータを自動的に破棄する時間間隔。単位: 秒。デフォルト値: `129600`、つまり 36 時間。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、`unified_catalog_hms` または `unified_catalog_glue` という名前の統合カタログを作成し、統合データソースからデータをクエリします。

#### HDFS

ストレージとして HDFS を使用する場合、以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG unified_catalog_hms
PROPERTIES
(
    "type" = "unified",
    "unified.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースの認証

- Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR と AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### アサムドロールベースの認証

- Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR と AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM ユーザーベースの認証

- Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR と AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
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
CREATE EXTERNAL CATALOG unified_catalog_hms
PROPERTIES
(
    "type" = "unified",
    "unified.metastore.type" = "hive",
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
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージドサービス ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージド ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG unified_catalog_hms
    PROPERTIES
    (
        "type" = "unified",
        "unified.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG unified_catalog_hms
    PROPERTIES
    (
        "type" = "unified",
        "unified.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## 統合カタログの表示

現在の StarRocks クラスター内のすべてのカタログをクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部カタログの作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例では、`unified_catalog_glue` という名前の統合カタログの作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG unified_catalog_glue;
```

## 統合カタログとその中のデータベースへの切り替え

統合カタログとその中のデータベースに切り替えるには、以下の方法のいずれかを使用します。

- 現在のセッションで特定の統合カタログを指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、次にアクティブなデータベースを指定するには [USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

  ```SQL
  -- 現在のセッションで指定されたカタログに切り替えます:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定します:
  USE <db_name>
  ```

- [USE](../../sql-reference/sql-statements/Database/USE.md) を直接使用して、統合カタログとその中のデータベースに切り替えます。

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## 統合カタログの削除

外部カタログを削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例では、`unified_catalog_glue` という名前の統合カタログを削除します。

```SQL
DROP CATALOG unified_catalog_glue;
```

## 統合カタログからテーブルのスキーマを表示

統合カタログからテーブルのスキーマを表示するには、以下のいずれかの構文を使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## 統合カタログからデータをクエリ

統合カタログからデータをクエリするには、以下の手順に従います。

1. 統合カタログが関連付けられている統合データソース内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Hive Catalog とその中のデータベースに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)ます。

3. 指定されたデータベース内の対象テーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Hive、Iceberg、Hudi、または Delta Lake からデータをロード

Hive、Iceberg、Hudi、または Delta Lake テーブルのデータを統合カタログ内に作成された StarRocks テーブルにロードするには、[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用します。

以下の例では、Hive テーブル `hive_table` のデータを、統合カタログ `unified_catalog` に属するデータベース `test_database` に作成された StarRocks テーブル `test_tbl` にロードします。

```SQL
INSERT INTO unified_catalog.test_database.test_table SELECT * FROM hive_table
```

## 統合カタログにデータベースを作成

StarRocks の内部カタログと同様に、統合カタログに対して CREATE DATABASE 権限を持っている場合、そのカタログにデータベースを作成するために CREATE DATABASE ステートメントを使用できます。

> **注意**
>
> 権限の付与と取り消しは、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して行うことができます。

StarRocks は、統合カタログ内で Hive および Iceberg データベースの作成のみをサポートしています。

[統合カタログに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)、次にそのカタログにデータベースを作成するために次のステートメントを使用します。

```SQL
CREATE DATABASE <database_name>
[properties ("location" = "<prefix>://<path_to_database>/<database_name.db>")]
```

`location` パラメータは、データベースを作成したいファイルパスを指定します。これは HDFS またはクラウドストレージのいずれかにすることができます。

- データソースのメタストアとして Hive メタストアを使用する場合、`location` パラメータはデフォルトで `<warehouse_location>/<database_name.db>` になり、データベース作成時にそのパラメータを指定しない場合でも Hive メタストアでサポートされます。
- データソースのメタストアとして AWS Glue を使用する場合、`location` パラメータにはデフォルト値がなく、したがってデータベース作成時にそのパラメータを指定する必要があります。

`prefix` は使用するストレージシステムに基づいて異なります。

| **ストレージシステム**                                         | **`Prefix`** **値**                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| HDFS                                                       | `hdfs`                                                       |
| Google GCS                                                 | `gs`                                                         |
| Azure Blob Storage                                         | <ul><li>ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `wasb` です。</li><li>ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `wasbs` です。</li></ul> |
| Azure Data Lake Storage Gen1                               | `adl`                                                        |
| Azure Data Lake Storage Gen2                               | <ul><li>ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `abfs` です。</li><li>ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `abfss` です。</li></ul> |
| AWS S3 またはその他の S3 互換ストレージ（例: MinIO）        | `s3`                                                         |

## 統合カタログからデータベースを削除

StarRocks の内部データベースと同様に、統合カタログ内に作成されたデータベースに対して [DROP](../../administration/user_privs/privilege_overview.md#database) 権限を持っている場合、そのデータベースを削除するために [DROP DATABASE](../../sql-reference/sql-statements/Database/DROP_DATABASE.md) ステートメントを使用できます。空のデータベースのみを削除できます。

> **注意**
>
> 権限の付与と取り消しは、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して行うことができます。

StarRocks は、統合カタログから Hive および Iceberg データベースの削除のみをサポートしています。

統合カタログからデータベースを削除する際、HDFS クラスターまたはクラウドストレージ上のデータベースのファイルパスはデータベースと共に削除されません。

[統合カタログに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)、次にそのカタログにデータベースを削除するために次のステートメントを使用します。

```SQL
DROP DATABASE <database_name>
```

## 統合カタログにテーブルを作成

StarRocks の内部データベースと同様に、統合カタログ内に作成されたデータベースに対して [CREATE TABLE](../../administration/user_privs/privilege_overview.md#database) 権限を持っている場合、そのデータベースにテーブルを作成するために [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) または [CREATE TABLE AS SELECT](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) ステートメントを使用できます。

> **注意**
>
> 権限の付与と取り消しは、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して行うことができます。

StarRocks は、統合カタログ内で Hive および Iceberg テーブルの作成のみをサポートしています。

[Hive Catalog とその中のデータベースに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)。次に、そのデータベースに Hive または Iceberg テーブルを作成するために [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を使用します。

```SQL
CREATE TABLE <table_name>
(column_definition1[, column_definition2, ...]
ENGINE = {|hive|iceberg}
[partition_desc]
```

詳細については、[Hive テーブルの作成](../catalog/hive_catalog.md#create-a-hive-table) および [Iceberg テーブルの作成](../catalog/iceberg_catalog.md#create-an-iceberg-table) を参照してください。

以下の例では、`hive_table` という名前の Hive テーブルを作成します。このテーブルは、`action`、`id`、および `dt` の 3 つの列で構成されており、`id` と `dt` はパーティション列です。

```SQL
CREATE TABLE hive_table
(
    action varchar(65533),
    id int,
    dt date
)
ENGINE = hive
PARTITION BY (id,dt);
```

## 統合カタログのテーブルにデータをシンク

StarRocks の内部テーブルと同様に、統合カタログ内に作成されたテーブルに対して [INSERT](../../administration/user_privs/privilege_overview.md#table) 権限を持っている場合、StarRocks テーブルのデータをその統合カタログテーブルにシンクするために [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用できます（現在サポートされているのは Parquet 形式の統合カタログテーブルのみです）。

> **注意**
>
> 権限の付与と取り消しは、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して行うことができます。

StarRocks は、統合カタログ内で Hive および Iceberg テーブルへのデータシンクのみをサポートしています。

[Hive Catalog とその中のデータベースに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)。次に、そのデータベースに Hive または Iceberg テーブルにデータを挿入するために [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用します。

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 指定されたパーティションにデータをシンクしたい場合、次の構文を使用します。
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

詳細については、[Hive テーブルへのデータシンク](../catalog/hive_catalog.md#sink-data-to-a-hive-table) および [Iceberg テーブルへのデータシンク](../catalog/iceberg_catalog.md#sink-data-to-an-iceberg-table) を参照してください。

以下の例では、`hive_table` という名前の Hive テーブルに 3 行のデータを挿入します。

```SQL
INSERT INTO hive_table
VALUES
    ("buy", 1, "2023-09-01"),
    ("sell", 2, "2023-09-02"),
    ("buy", 3, "2023-09-03");
```

## 統合カタログからテーブルを削除

StarRocks の内部テーブルと同様に、統合カタログ内に作成されたテーブルに対して [DROP](../../administration/user_privs/privilege_overview.md#table) 権限を持っている場合、そのテーブルを削除するために [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) ステートメントを使用できます。

> **注意**
>
> 権限の付与と取り消しは、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して行うことができます。

StarRocks は、統合カタログから Hive および Iceberg テーブルの削除のみをサポートしています。

[Hive Catalog とその中のデータベースに切り替え](#switch-to-a-unified-catalog-and-a-database-in-it)。次に、そのデータベースに Hive または Iceberg テーブルを削除するために [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) を使用します。

```SQL
DROP TABLE <table_name>
```

詳細については、[Hive テーブルの削除](../catalog/hive_catalog.md#drop-a-hive-table) および [Iceberg テーブルの削除](../catalog/iceberg_catalog.md#drop-an-iceberg-table) を参照してください。

以下の例では、`hive_table` という名前の Hive テーブルを削除します。

```SQL
DROP TABLE hive_table FORCE
```