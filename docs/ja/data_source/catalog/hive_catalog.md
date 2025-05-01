---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Hive catalog

Hive catalog は、Apache Hive™ からデータを取り込まずにクエリを実行できる外部 catalog の一種です。また、Hive catalogs を使用して [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を基に、Hive からデータを直接変換してロードすることもできます。

StarRocks は v2.4 以降で Hive catalogs をサポートしており、v3.1 以降では Hive catalogs 内のテーブルに作成されたビューへのアクセスもサポートしています。

Hive クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Hive クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ

- Hive metastore または AWS Glue のようなメタストア

  > **注意**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとしては HMS のみを使用できます。

## 使用上の注意

- StarRocks がサポートする Hive のファイル形式は Parquet、ORC、Textfile です。

  - Parquet ファイルは以下の圧縮形式をサポートしています: SNAPPY、LZ4、ZSTD、GZIP、NO_COMPRESSION。v3.1.5 以降では LZO 圧縮形式もサポートしています。
  - ORC ファイルは以下の圧縮形式をサポートしています: ZLIB、SNAPPY、LZO、LZ4、ZSTD、NO_COMPRESSION。
  - Textfile ファイルは v3.1.5 以降で LZO 圧縮形式をサポートしています。

- StarRocks がサポートしていない Hive のデータ型は INTERVAL、BINARY、UNION です。さらに、StarRocks は Textfile 形式の Hive テーブルに対して MAP および STRUCT データ型をサポートしていません。
- Hive catalogs を使用してデータをクエリすることはできますが、Hive クラスターにデータを削除、削除、または挿入することはできません。

## 統合準備

Hive catalog を作成する前に、StarRocks クラスターが Hive クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Hive クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル
- 想定ロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

### HDFS

HDFS をストレージとして選択した場合、StarRocks クラスターを次のように構成します。

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭および各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルにユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメーター設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Hive データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります。

  - HDFS クラスターに高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターに View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に未知のホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアに Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように構成します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。したがって、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE の **$BE_HOME/conf/be.conf** ファイルまたは各 CN の **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Hive catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "hive",
    GeneralParams,
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### パラメーター

#### catalog_name

Hive catalog の名前です。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、1023 文字を超えることはできません。

#### comment

Hive catalog の説明です。このパラメーターはオプションです。

#### type

データソースのタイプです。値を `hive` に設定します。

#### GeneralParams

一般的なパラメーターのセットです。

`GeneralParams` で設定できるパラメーターは次の表に記載されています。

| パラメーター                | 必須 | 説明                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| enable_recursive_listing | No       | StarRocks がテーブルとそのパーティション、およびテーブルとそのパーティションの物理的な場所内のサブディレクトリからデータを読み取るかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はサブディレクトリを再帰的にリストすることを指定し、値 `false` はサブディレクトリを無視することを指定します。 |

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関するパラメーターのセットです。

##### Hive metastore

データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を次のように構成します。

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> Hive データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive メタストアにアクセスできない可能性があります。

`MetastoreParams` で設定する必要があるパラメーターは次の表に記載されています。

| パラメーター           | 必須 | 説明                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Hive クラスターで使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive メタストアに高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。以下のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 想定ロールベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`MetastoreParams` を次のように構成します。

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

`MetastoreParams` で設定する必要があるパラメーターは次の表に記載されています。

| パラメーター                     | 必須 | 説明                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Hive クラスターで使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするために想定ロールベースの認証方法を使用する場合、このパラメーターを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメーターを指定する必要があります。 |

AWS Glue にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue にアクセスするための認証パラメーター](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関するパラメーターのセットです。このパラメーターセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

Hive クラスターのストレージとして AWS S3 を選択した場合、以下のいずれかの操作を行います。

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

`StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

| パラメーター                   | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするために想定ロールベースの認証方法を使用する場合、このパラメーターを指定する必要があります。  |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメーターを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメーターを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 にアクセスするための認証パラメーター](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

Hive catalogs は v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム (例: MinIO) を選択した場合、`StorageCredentialParams` を次のように構成して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

| パラメーター                        | 必須 | 説明                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイルの URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイルの URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Hive catalogs は v3.0 以降で Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

Blob Storage を Hive クラスターのストレージとして選択した場合、以下のいずれかの操作を行います。

- 共有キー認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**              | **必須** | **説明**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

- SAS トークン認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**             | **必須** | **説明**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する Blob コンテナの名前です。        |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Hive クラスターのストレージとして選択した場合、以下のいずれかの操作を行います。

- マネージド ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                           | **必須** | **説明**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。         |

- 共有キー認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**               | **必須** | **説明**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                      | **必須** | **説明**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Hive クラスターのストレージとして選択した場合、以下のいずれかの操作を行います。

- マネージドサービス ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                            | **必須** | **説明**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                 | **必須** | **説明**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Hive catalogs は v3.0 以降で Google GCS をサポートしています。

Google GCS を Hive クラスターのストレージとして選択した場合、以下のいずれかの操作を行います。

- VM ベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                              | **デフォルト値** | **値の例** | **説明**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

  | **パラメーター**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |

- インパーソネーションベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します。

  - VM インスタンスがサービスアカウントをインパーソネートする場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

    | **パラメーター**                              | **デフォルト値** | **値の例** | **説明**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。            |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) が別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートする場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` で設定する必要があるパラメーターは次の表に記載されています。

    | **パラメーター**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。       |

#### MetadataUpdateParams

StarRocks が Hive のキャッシュされたメタデータを更新する方法に関するパラメーターのセットです。このパラメーターセットはオプションです。

StarRocks はデフォルトで [自動非同期更新ポリシー](#appendix-understand-metadata-automatic-asynchronous-update) を実装しています。

ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメーターを調整する必要はありません。これらのパラメーターのデフォルト値は、すぐに使えるパフォーマンスを提供します。

ただし、Hive でのデータ更新の頻度が高い場合、これらのパラメーターを調整して自動非同期更新のパフォーマンスをさらに最適化できます。

> **注意**
>
> ほとんどの場合、Hive データが 1 時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。

| パラメーター                              | 必須 | 説明                                                  |
|----------------------------------------| -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | No       | StarRocks が Hive テーブルのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| enable_remote_file_cache               | No       | StarRocks が Hive テーブルまたはパーティションの基礎データファイルのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| metastore_cache_refresh_interval_sec   | No       | StarRocks が自身にキャッシュされた Hive テーブルまたはパーティションのメタデータを非同期で更新する時間間隔です。単位: 秒。デフォルト値: `7200` (2 時間)。 |
| remote_file_cache_refresh_interval_sec | No       | StarRocks が自身にキャッシュされた Hive テーブルまたはパーティションの基礎データファイルのメタデータを非同期で更新する時間間隔です。単位: 秒。デフォルト値: `60`。 |
| metastore_cache_ttl_sec                | No       | StarRocks が自身にキャッシュされた Hive テーブルまたはパーティションのメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `86400` (24 時間)。 |
| remote_file_cache_ttl_sec              | No       | StarRocks が自身にキャッシュされた Hive テーブルまたはパーティションの基礎データファイルのメタデータを自動的に破棄する時間間隔です。単位: 秒。デフォルト値: `129600` (36 時間)。 |
| enable_cache_list_names                | No       | StarRocks が Hive パーティション名をキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、`hive_catalog_hms` または `hive_catalog_glue` という名前の Hive catalog を作成し、Hive クラスターからデータをクエリします。

#### HDFS

HDFS をストレージとして使用する場合、以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_hms
PROPERTIES
(
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### インスタンスプロファイルベースの認証

- Hive クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 想定ロールベースの認証

- Hive クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM ユーザーベースの認証

- Hive クラスターで Hive メタストアを使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
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
CREATE EXTERNAL CATALOG hive_catalog_hms
PROPERTIES
(
    "type" = "hive",
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

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージドサービス ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージド ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスがサービスアカウントをインパーソネートする場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG hive_catalog_hms
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - サービスアカウントが別のサービスアカウントをインパーソネートする場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG hive_catalog_hms
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## Hive catalogs の表示

現在の StarRocks クラスター内のすべての catalogs をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。以下の例では、`hive_catalog_glue` という名前の Hive catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG hive_catalog_glue;
```

## Hive Catalog とその中のデータベースに切り替える

Hive catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます。

- 現在のセッションで Hive catalog を指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

- [USE](../../sql-reference/sql-statements/Database/USE.md) を直接使用して、Hive catalog とその中のデータベースに切り替えます。

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Hive catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

以下の例では、`hive_catalog_glue` という名前の Hive catalog を削除します。

```SQL
DROP Catalog hive_catalog_glue;
```

## Hive テーブルのスキーマを表示

Hive テーブルのスキーマを表示するには、次のいずれかの構文を使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Hive テーブルをクエリ

1. Hive クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

```SQL
SHOW DATABASES FROM <catalog_name>
```

2. [Hive Catalog とその中のデータベースに切り替える](#switch-to-a-hive-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

```SQL
SELECT count(*) FROM <table_name> LIMIT 10
```

## Hive からデータをロード

`olap_tbl` という名前の OLAP テーブルがあると仮定し、以下のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM hive_table
```

## Hive テーブルとビューへの権限を付与

Hive catalog 内のすべてのテーブルとビューに対する権限を特定のロールに付与するには、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) ステートメントを使用できます。コマンド構文は次のとおりです。

```SQL
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE <role_name>
```

例えば、`hive_role_table` という名前のロールを作成し、Hive catalog `hive_catalog` に切り替え、その後 `hive_role_table` ロールに Hive catalog `hive_catalog` 内のすべてのテーブルとビューをクエリする権限を付与するには、次のコマンドを使用します。

```SQL
-- hive_role_table という名前のロールを作成します。
CREATE ROLE hive_role_table;

-- Hive catalog hive_catalog に切り替えます。
SET CATALOG hive_catalog;

-- Hive catalog hive_catalog 内のすべてのテーブルとビューをクエリする権限を hive_role_table ロールに付与します。
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE hive_role_table;
```

## メタデータキャッシュを手動または自動で更新

### 手動更新

デフォルトでは、StarRocks は Hive のメタデータをキャッシュし、非同期モードでメタデータを自動的に更新して、より良いパフォーマンスを提供します。さらに、Hive テーブルに対していくつかのスキーマ変更やテーブル更新が行われた後、[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) を使用してメタデータを手動で更新することもできます。これにより、StarRocks が最新のメタデータをできるだけ早く取得し、適切な実行プランを生成できるようにします。

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```

次の状況では、メタデータを手動で更新する必要があります。

- 既存のパーティション内のデータファイルが変更された場合 (例: `INSERT OVERWRITE ... PARTITION ...` コマンドを実行する)。
- Hive テーブルにスキーマ変更が行われた場合。
- DROP ステートメントを使用して既存の Hive テーブルが削除され、削除された Hive テーブルと同じ名前の新しい Hive テーブルが作成された場合。
- Hive catalog の作成時に `PROPERTIES` で `"enable_cache_list_names" = "true"` を指定し、Hive クラスターで新しく作成したパーティションをクエリしたい場合。

  > **注意**
  >
  > v2.5.5 以降、StarRocks は定期的な Hive メタデータキャッシュの更新機能を提供しています。詳細については、以下の「[メタデータキャッシュを定期的に更新](#periodically-refresh-metadata-cache)」セクションを参照してください。この機能を有効にすると、StarRocks はデフォルトで 10 分ごとに Hive メタデータキャッシュを更新します。したがって、ほとんどの場合、手動での更新は必要ありません。新しいパーティションを Hive クラスターで作成した直後にクエリしたい場合にのみ、手動での更新が必要です。

REFRESH EXTERNAL TABLE は、FEs にキャッシュされたテーブルとパーティションのみを更新します。

## メタデータキャッシュを定期的に更新

v2.5.5 以降、StarRocks は頻繁にアクセスされる Hive catalogs のキャッシュされたメタデータを定期的に更新して、データの変更を検知できます。以下の [FE パラメーター](../../administration/management/FE_configuration.md) を通じて、Hive メタデータキャッシュの更新を構成できます。

| 設定項目                                           | デフォルト                              | 説明                          |
| ------------------------------------------------------------ | ------------------------------------ | ------------------------------------ |
| enable_background_refresh_connector_metadata                 | `true` in v3.0<br />`false` in v2.5  | 定期的な Hive メタデータキャッシュの更新を有効にするかどうか。これを有効にすると、StarRocks は Hive クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Hive catalogs のキャッシュされたメタデータを更新してデータの変更を検知します。`true` は Hive メタデータキャッシュの更新を有効にし、`false` は無効にします。この項目は [FE 動的パラメーター](../../administration/management/FE_configuration.md#configure-fe-dynamic-parameters) です。[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) コマンドを使用して変更できます。 |
| background_refresh_metadata_interval_millis                  | `600000` (10 分)                | 2 回の連続した Hive メタデータキャッシュの更新の間隔です。単位: ミリ秒。この項目は [FE 動的パラメーター](../../administration/management/FE_configuration.md#configure-fe-dynamic-parameters) です。[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) コマンドを使用して変更できます。 |
| background_refresh_metadata_time_secs_since_last_access_secs | `86400` (24 時間)                   | Hive メタデータキャッシュ更新タスクの有効期限です。アクセスされた Hive catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Hive catalog に対して、StarRocks はそのキャッシュされたメタデータを更新しません。単位: 秒。この項目は [FE 動的パラメーター](../../administration/management/FE_configuration.md#configure-fe-dynamic-parameters) です。[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) コマンドを使用して変更できます。 |

定期的な Hive メタデータキャッシュの更新機能とメタデータ自動非同期更新ポリシーを組み合わせて使用することで、データアクセスが大幅に高速化され、外部データソースからの読み取り負荷が軽減され、クエリパフォーマンスが向上します。

## 付録: メタデータ自動非同期更新を理解する

自動非同期更新は、StarRocks が Hive catalogs のメタデータを更新するために使用するデフォルトのポリシーです。

デフォルトでは (つまり、`enable_metastore_cache` および `enable_remote_file_cache` パラメーターが両方とも `true` に設定されている場合)、クエリが Hive テーブルのパーティションにヒットすると、StarRocks はそのパーティションのメタデータとそのパーティションの基礎データファイルのメタデータを自動的にキャッシュします。キャッシュされたメタデータは、遅延更新ポリシーを使用して更新されます。

例えば、`table2` という名前の Hive テーブルがあり、4 つのパーティション `p1`、`p2`、`p3`、`p4` を持っているとします。クエリが `p1` にヒットすると、StarRocks は `p1` のメタデータと `p1` の基礎データファイルのメタデータをキャッシュします。キャッシュされたメタデータを更新および破棄するためのデフォルトの時間間隔は次のとおりです。

- キャッシュされた `p1` のメタデータを非同期で更新する時間間隔 (パラメーター `metastore_cache_refresh_interval_sec` で指定) は 2 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを非同期で更新する時間間隔 (パラメーター `remote_file_cache_refresh_interval_sec` で指定) は 60 秒です。
- キャッシュされた `p1` のメタデータを自動的に破棄する時間間隔 (パラメーター `metastore_cache_ttl_sec` で指定) は 24 時間です。
- `p1` の基礎データファイルのキャッシュされたメタデータを自動的に破棄する時間間隔 (パラメーター `remote_file_cache_ttl_sec` で指定) は 36 時間です。

以下の図は、キャッシュされたメタデータの更新および破棄の時間間隔をタイムライン上で示しています。

![キャッシュされたメタデータの更新および破棄のタイムライン](../../_assets/catalog_timeline.png)

その後、StarRocks は次のルールに従ってメタデータを更新または破棄します。

- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒未満の場合、StarRocks は `p1` のキャッシュされたメタデータや `p1` の基礎データファイルのキャッシュされたメタデータを更新しません。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 60 秒を超える場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを更新します。
- 別のクエリが再び `p1` にヒットし、最後の更新からの現在の時間が 2 時間を超える場合、StarRocks は `p1` のキャッシュされたメタデータを更新します。
- `p1` が最後の更新から 24 時間以内にアクセスされていない場合、StarRocks は `p1` のキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。
- `p1` が最後の更新から 36 時間以内にアクセスされていない場合、StarRocks は `p1` の基礎データファイルのキャッシュされたメタデータを破棄します。次のクエリでメタデータがキャッシュされます。
```