---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Paimon catalog

StarRocks は v3.1 以降で Paimon catalog をサポートしています。

Paimon catalog は、Apache Paimon からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Paimon catalog を使用して [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を基に Paimon からデータを直接変換してロードすることもできます。

Paimon クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Paimon クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、または他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ
- ファイルシステムまたは Hive メタストアのようなメタストア

## 使用上の注意

Paimon catalog はデータのクエリにのみ使用できます。Paimon catalog を使用して Paimon クラスターにデータを削除、削除、または挿入することはできません。

## Paimon to StarRocks data types

| Paimon Type           | StarRocks Type              |
|-----------------------|-----------------------------|
| `BINARY`              | `VARBINARY`                 |
| `VARBINARY`           | `VARBINARY`                 |
| `CHAR`                | `CHAR(length)`              |
| `VARCHAR`             | `VARCHAR`                   |
| `BOOLEAN`             | `BOOLEAN`                   |
| `DECIMAL`             | `DECIMAL(precision, scale)` |
| `TINYINT`             | `TINYINT`                   |
| `SMALLINT`            | `SMALLINT`                  |
| `INT`                 | `INT`                       |
| `BIGINT`              | `BIGINT`                    |
| `FLOAT`               | `FLOAT`                     |
| `DOUBLE`              | `DOUBLE`                    |
| `DATE`                | `DATE`                      |
| `TIME`                | `TIME`                      |
| `TIMESTAMP`           | `DATETIME`                  |
| `LocalZonedTimestamp` | `DATETIME`                  |
| `ARRAY`               | `ARRAY<element_type>`       |
| `MAP`                 | `MAP<key_type, value_type>` |
| `ROW/STRUCT`          | `STRUCT<field1:type1, ...>` |

## 統合準備

Paimon catalog を作成する前に、StarRocks クラスターが Paimon クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Paimon クラスターが AWS S3 をストレージとして使用している場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル (推奨)
- アサインされたロール
- IAM ユーザー

上記の三つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication) を参照してください。

### HDFS

HDFS をストレージとして選択する場合、StarRocks クラスターを次のように構成します。

- (オプション) HDFS クラスターと Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターと Hive メタストアにアクセスするために FE と BE または CN プロセスのユーザー名を使用します。各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭と各 BE または CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE と各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに一つのユーザー名しか設定できません。
- Paimon データをクエリする際、StarRocks クラスターの FEs と BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります。
  - 高可用性 (HA) が HDFS クラスターに対して有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。
  - View File System (ViewFs) が HDFS クラスターに対して有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

Kerberos 認証が HDFS クラスターまたは Hive メタストアに対して有効になっている場合、StarRocks クラスターを次のように構成します。

- 各 FE と各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターと Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。したがって、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルと各 BE または CN の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

## Paimon catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "paimon",
    CatalogParams,
    StorageCredentialParams,
)
```

### パラメータ

#### catalog_name

Paimon catalog の名前です。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。名前は文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

Paimon catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `paimon` に設定します。

#### CatalogParams

StarRocks が Paimon クラスターのメタデータにアクセスする方法に関する一連のパラメータです。

`CatalogParams` で構成する必要があるパラメータを以下の表に示します。

| パラメータ                | 必須 | 説明                                                  |
| ------------------------ | ---- | ---------------------------------------------------- |
| paimon.catalog.type      | はい | Paimon クラスターで使用するメタストアのタイプです。このパラメータを `filesystem` または `hive` に設定します。 |
| paimon.catalog.warehouse | はい | Paimon データのウェアハウスストレージパスです。 |
| hive.metastore.uris      | いいえ | Hive メタストアの URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。Hive メタストアに高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

> **注意**
>
> Hive メタストアを使用する場合、Paimon データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive メタストアにアクセスできない可能性があります。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

##### AWS S3

Paimon クラスターのストレージとして AWS S3 を選択する場合、以下のいずれかのアクションを実行します。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- アサインされたロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

`StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | ---- | ---------------------------------------------------- |
| aws.s3.use_instance_profile | はい | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.endpoint             | はい | AWS S3 バケットに接続するために使用されるエンドポイントです。例: `https://s3.us-west-2.amazonaws.com`。 |
| aws.s3.access_key           | いいえ | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

##### S3 互換ストレージシステム

S3 互換ストレージシステム (例: MinIO) を Paimon クラスターのストレージとして選択する場合、次のように `StorageCredentialParams` を構成して統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

| パラメータ                       | 必須 | 説明                                                  |
| ------------------------------- | ---- | ---------------------------------------------------- |
| aws.s3.enable_ssl               | はい | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access | はい | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br /> パススタイルの URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例: US West (オレゴン) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスする場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                 | はい | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key               | はい | IAM ユーザーのアクセスキーです。                             |
| aws.s3.secret_key               | はい | IAM ユーザーのシークレットキーです。                             |

##### Microsoft Azure Storage

###### Azure Blob Storage

Blob Storage を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                  | 必須 | 説明                                  |
  | -------------------------- | ---- | ------------------------------------ |
  | azure.blob.storage_account | はい | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | はい | Blob Storage アカウントの共有キーです。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                 | 必須 | 説明                                                  |
  | ------------------------- | ---- | ---------------------------------------------------- |
  | azure.blob.storage_account| はい | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | はい | データを格納する Blob コンテナの名前です。        |
  | azure.blob.sas_token      | はい | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                               | 必須 | 説明                                                  |
  | --------------------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls2.oauth2_use_managed_identity | はい | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | はい | アクセスしたいデータのテナント ID です。          |
  | azure.adls2.oauth2_client_id            | はい | マネージド ID のクライアント (アプリケーション) ID です。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                   | 必須 | 説明                                                  |
  | --------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls2.storage_account | はい | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | はい | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

- サービス プリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                          | 必須 | 説明                                                  |
  | ---------------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls2.oauth2_client_id       | はい | サービス プリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls2.oauth2_client_secret   | はい | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls2.oauth2_client_endpoint | はい | サービス プリンシパルまたはアプリケーションの OAuth 2.0 トークン エンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- マネージド サービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                                | 必須 | 説明                                                  |
  | ---------------------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls1.use_managed_service_identity | はい | マネージド サービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービス プリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                     | 必須 | 説明                                                  |
  | ----------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls1.oauth2_client_id  | はい | サービス プリンシパルのクライアント (アプリケーション) ID です。        |
  | azure.adls1.oauth2_credential | はい | 作成された新しいクライアント (アプリケーション) シークレットの値です。    |
  | azure.adls1.oauth2_endpoint   | はい | サービス プリンシパルまたはアプリケーションの OAuth 2.0 トークン エンドポイント (v1) です。 |

##### Google GCS

Google GCS を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                                  | デフォルト値 | 値の例 | 説明                                                  |
  | ------------------------------------------ | ----------- | ------- | ---------------------------------------------------- |
  | gcp.gcs.use_compute_engine_service_account | FALSE       | TRUE    | コンピュート エンジンにバインドされたサービス アカウントを直接使用するかどうかを指定します。 |

- サービス アカウント ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                              | デフォルト値 | 値の例                                                | 説明                                                  |
  | -------------------------------------- | ----------- | ---------------------------------------------------- | ---------------------------------------------------- |
  | gcp.gcs.service_account_email          | ""          | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービス アカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""          | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"           | サービス アカウントの作成時に生成された JSON ファイルのプライベート キー ID です。 |
  | gcp.gcs.service_account_private_key    | ""          | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | サービス アカウントの作成時に生成された JSON ファイルのプライベート キーです。 |

- インパーソネーション ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  - VM インスタンスにサービス アカウントをインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

    | パラメータ                                  | デフォルト値 | 値の例 | 説明                                                  |
    | ------------------------------------------ | ----------- | ------- | ---------------------------------------------------- |
    | gcp.gcs.use_compute_engine_service_account | FALSE       | TRUE    | コンピュート エンジンにバインドされたサービス アカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""          | "hello" | インパーソネートしたいサービス アカウントです。            |

  - サービス アカウント (一時的にメタ サービス アカウントと呼ばれる) に別のサービス アカウント (一時的にデータ サービス アカウントと呼ばれる) をインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

    | パラメータ                              | デフォルト値 | 値の例                                                | 説明                                                  |
    | -------------------------------------- | ----------- | ---------------------------------------------------- | ---------------------------------------------------- |
    | gcp.gcs.service_account_email          | ""          | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタ サービス アカウントの作成時に生成された JSON ファイルのメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""          | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"           | メタ サービス アカウントの作成時に生成された JSON ファイルのプライベート キー ID です。 |
    | gcp.gcs.service_account_private_key    | ""          | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | メタ サービス アカウントの作成時に生成された JSON ファイルのプライベート キーです。 |
    | gcp.gcs.impersonation_service_account  | ""          | "hello"                                              | インパーソネートしたいデータ サービス アカウントです。       |

### 例

以下の例は、Paimon クラスターからデータをクエリするためにメタストアタイプ `paimon.catalog.type` を `filesystem` に設定した Paimon catalog `paimon_catalog_fs` を作成します。

#### AWS S3

- インスタンスプロファイルベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- アサインされたロールベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- IAM ユーザーベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

#### S3 互換ストレージシステム

MinIO を例にとります。以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG paimon_catalog_fs
PROPERTIES
(
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "paimon.catalog.warehouse" = "<paimon_warehouse_path>",
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
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- SAS トークン認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- マネージド サービス ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.use_managed_service_identity" = "true"
  );
  ```

- サービス プリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- マネージド ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 共有キー認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"
  );
  ```

- サービス プリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- VM ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.use_compute_engine_service_account" = "true"
  );
  ```

- サービス アカウント ベースの認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  );
  ```

- インパーソネーション ベースの認証方法を選択する場合:

  - VM インスタンスにサービス アカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    );
    ```

  - サービス アカウントに別のサービス アカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

    ```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

## Paimon catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例では、`paimon_catalog_fs` という名前の Paimon catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG paimon_catalog_fs;
```

## Paimon catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例では、`paimon_catalog_fs` という名前の Paimon catalog を削除します。

```SQL
DROP Catalog paimon_catalog_fs;
```

## Paimon テーブルのスキーマを表示

Paimon テーブルのスキーマを表示するには、次のいずれかの構文を使用します。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Paimon テーブルのクエリ

1. Paimon クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 現在のセッションで目的の catalog に切り替えるには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用します。

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   次に、現在のセッションでアクティブなデータベースを指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <db_name>;
   ```

   または、目的の catalog でアクティブなデータベースを直接指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Paimon からデータをロード

OLAP テーブル `olap_tbl` があると仮定して、次のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM paimon_table;
```