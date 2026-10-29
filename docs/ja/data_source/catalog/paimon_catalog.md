---
sidebar_position: 100
displayed_sidebar: docs
description: "StarRocks v3.1 以降の Paimon catalog で、Apache Paimon からデータをインジェストせずにクエリおよび変換ロード。"
toc_max_heading_level: 5
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Paimon catalog

<Beta />

StarRocks は v3.1 以降で Paimon catalog をサポートしています。

Paimon catalog は、Apache Paimon からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Paimon catalog を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Paimon からデータを直接変換してロードすることもできます。

Paimon クラスターで SQL ワークロードを成功させるには、StarRocks クラスターが Paimon クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム（例: MinIO）などのオブジェクトストレージ
- ファイルシステムまたは Hive メタストアのようなメタストア

## 使用上の注意

現在の Paimon 統合では、クエリに加え、`CREATE/DROP DATABASE`、`CREATE/DROP TABLE`、バッチ `INSERT INTO`、`INSERT OVERWRITE`、`CREATE TABLE AS SELECT` (CTAS)、同一 catalog 内の `CREATE TABLE LIKE` が実装されています。行単位の `UPDATE` と `DELETE` はサポートしていません。削除できるデータベースは空のものに限られます。

:::caution
DDL とバッチ書き込みはまだ統合作業中です。以下のテーブル作成と書き込みの説明は現在の実装を示しており、実行時に検証済みのリリースでの提供を保証するものではありません。例とエンドツーエンドの再読み取り動作には、引き続き実行時検証が必要です。
:::

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
| `VARIANT`             | `VARIANT`                   |

:::note
`VARIANT` はv4.2以降、ネイティブリーダーで読み取られるsplit（Append-Onlyテーブル、およびPrimary KeyテーブルのCompaction済みデータ）でのみサポートされています。Compaction前のPrimary Keyデータから`VARIANT`を読み取るにはJNIリーダーが必要であり、現時点ではまだサポートされていません。該当するクエリはプランニング時にエラーとして拒否されます。
:::

## 統合準備

Paimon catalog を作成する前に、StarRocks クラスターが Paimon クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

Paimon クラスターが AWS S3 をストレージとして使用している場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。

- インスタンスプロファイル（推奨）
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/csp_auth/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication)を参照してください。

### HDFS

HDFS をストレージとして選択する場合、StarRocks クラスターを次のように構成します。

- (オプション) HDFS クラスターおよび Hive メタストアにアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE および BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭と各 BE または CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Paimon データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを構成する必要はなく、StarRocks はデフォルトの構成を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを構成する必要があります。
  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE または CN の **$BE_HOME/conf** パスに追加します。

> **注意**
>
> クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive メタストアで Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように構成します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、このコマンドを定期的に実行するために cron を使用する必要があります。
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

Paimon catalog の名前。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### comment

Paimon catalog の説明。このパラメータはオプションです。

#### type

データソースのタイプ。値を `paimon` に設定します。

#### CatalogParams

StarRocks が Paimon クラスターのメタデータにアクセスする方法に関するパラメータのセット。

`CatalogParams` で構成する必要があるパラメータを以下の表に示します。

| パラメータ                | 必須 | 説明                                                  |
| ------------------------ | ---- | ---------------------------------------------------- |
| paimon.catalog.type      | はい | Paimon クラスターで使用するメタストアのタイプ。このパラメータを `filesystem` または `hive` に設定します。 |
| paimon.catalog.warehouse | はい | Paimon データのウェアハウスストレージパス。 |
| hive.metastore.uris      | いいえ | Hive メタストアの URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。Hive メタストアで高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

> **注意**
>
> Hive メタストアを使用する場合、Paimon データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive メタストアにアクセスできない可能性があります。

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関するパラメータのセット。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を構成する必要があります。

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
| aws.s3.use_instance_profile | はい | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.endpoint             | はい | AWS S3 バケットに接続するために使用されるエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。 |
| aws.s3.access_key           | いいえ | IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ | IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/csp_auth/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

##### S3 互換ストレージシステム

S3 互換ストレージシステム（例: MinIO）を選択する場合、Paimon クラスターのストレージとして、次のように `StorageCredentialParams` を構成して、統合を成功させます。

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
| aws.s3.enable_ssl               | はい | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access | はい | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br /> パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例: US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスする場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                 | はい | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key               | はい | IAM ユーザーのアクセスキー。                             |
| aws.s3.secret_key               | はい | IAM ユーザーのシークレットキー。                             |

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
  | azure.blob.storage_account | はい | Blob Storage アカウントのユーザー名。   |
  | azure.blob.shared_key      | はい | Blob Storage アカウントの共有キー。 |

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                 | 必須 | 説明                                                  |
  | ------------------------- | ---- | ---------------------------------------------------- |
  | azure.blob.storage_account| はい | Blob Storage アカウントのユーザー名。                   |
  | azure.blob.container      | はい | データを格納する Blob コンテナの名前。        |
  | azure.blob.sas_token      | はい | Blob Storage アカウントにアクセスするために使用される SAS トークン。 |

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
  | azure.adls2.oauth2_tenant_id            | はい | アクセスしたいデータのテナント ID。          |
  | azure.adls2.oauth2_client_id            | はい | マネージド ID のクライアント (アプリケーション) ID。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                   | 必須 | 説明                                                  |
  | --------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls2.storage_account | はい | Data Lake Storage Gen2 ストレージアカウントのユーザー名。 |
  | azure.adls2.shared_key      | はい | Data Lake Storage Gen2 ストレージアカウントの共有キー。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                          | 必須 | 説明                                                  |
  | ---------------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls2.oauth2_client_id       | はい | サービスプリンシパルのクライアント (アプリケーション) ID。        |
  | azure.adls2.oauth2_client_secret   | はい | 作成された新しいクライアント (アプリケーション) シークレットの値。    |
  | azure.adls2.oauth2_client_endpoint | はい | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1)。 |

- ワークロード ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  ```SQL
  "azure.adls2.oauth2_token_file" = "<path_to_token>",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_token_file           | Yes          | Azure ワークロード ID ウェブフックによってポッドにマッピングされた、OAuth2 トークンファイルへの絶対ファイルパス。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。                    |
  | azure.adls2.oauth2_client_id            | Yes          | ワークロード ID に関連付けられている Azure AD アプリケーション（ユーザー割り当ての マネージド ID またはアプリ登録）のクライアント ID（アプリケーション ID）。 |

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                                | 必須 | 説明                                                  |
  | ---------------------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls1.use_managed_service_identity | はい | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                     | 必須 | 説明                                                  |
  | ----------------------------- | ---- | ---------------------------------------------------- |
  | azure.adls1.oauth2_client_id  | はい | サービスプリンシパルのクライアント (アプリケーション) ID。        |
  | azure.adls1.oauth2_credential | はい | 作成された新しいクライアント (アプリケーション) シークレットの値。    |
  | azure.adls1.oauth2_endpoint   | はい | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1)。 |

##### Google GCS

Google GCS を Paimon クラスターのストレージとして選択する場合、以下のいずれかのアクションを実行します。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                                  | デフォルト値 | 値の例 | 説明                                                  |
  | ------------------------------------------ | ----------- | ------- | ---------------------------------------------------- |
  | gcp.gcs.use_compute_engine_service_account | FALSE       | TRUE    | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

  | パラメータ                              | デフォルト値 | 値の例                                                | 説明                                                  |
  | -------------------------------------- | ----------- | ---------------------------------------------------- | ---------------------------------------------------- |
  | gcp.gcs.service_account_email          | ""          | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""          | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"           | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
  | gcp.gcs.service_account_private_key    | ""          | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します。

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

    | パラメータ                                  | デフォルト値 | 値の例 | 説明                                                  |
    | ------------------------------------------ | ----------- | ------- | ---------------------------------------------------- |
    | gcp.gcs.use_compute_engine_service_account | FALSE       | TRUE    | コンピュートエンジンにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""          | "hello" | インパーソネートしたいサービスアカウント。            |

  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` で構成する必要があるパラメータを以下の表に示します。

    | パラメータ                              | デフォルト値 | 値の例                                                | 説明                                                  |
    | -------------------------------------- | ----------- | ---------------------------------------------------- | ---------------------------------------------------- |
    | gcp.gcs.service_account_email          | ""          | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""          | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"           | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
    | gcp.gcs.service_account_private_key    | ""          | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""          | "hello"                                              | インパーソネートしたいデータサービスアカウント。       |

#### MetadataCacheParams（オプション）

StarRocks は Paimon のメタデータをキャッシュし、バックグラウンドで更新します。以下のオプションのプロパティでそのキャッシュを調整できます。

| パラメータ                                | デフォルト値 | 説明                                                         |
| ---------------------------------------- | ------------ | ------------------------------------------------------------ |
| paimon_meta_cache_ttl_sec                | 86400        | キャッシュエントリの有効期間（秒）。 |
| paimon_table_cache_refresh_interval_sec  | 60           | 同一テーブルに対してクエリが起点となる更新を 2 回行う間の最小間隔（秒）。クエリがより新しい Snapshot を読み取った場合に発生し、更新はバックグラウンドで実行されるためクエリは待機しません。`0` を設定すると、定期的なバックグラウンド更新のみになります。v4.2.0 以降でサポートされます。 |

### 例

以下の例では、Paimon クラスターからデータをクエリするために、メタストアタイプ `paimon.catalog.type` が `filesystem` に設定された `paimon_catalog_fs` という名前の Paimon catalog を作成します。

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

- マネージドサービス ID 認証方法を選択する場合、以下のようなコマンドを実行します。

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

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

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

- サービスプリンシパル認証方法を選択する場合、以下のようなコマンドを実行します。

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

- ワークロード ID 認証方法を選択する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
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

- サービスアカウントベースの認証方法を選択する場合、以下のようなコマンドを実行します。

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

- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

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

  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、以下のようなコマンドを実行します。

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

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

また、外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。以下の例では、`paimon_catalog_fs` という名前の Paimon catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG paimon_catalog_fs;
```

## Paimon catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

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

## Paimon テーブルをクエリ

1. [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用して、Paimon クラスター内のデータベースを表示します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用して、現在のセッションで目的の catalog に切り替えます。

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   次に、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、現在のセッションでアクティブなデータベースを指定します。

   ```SQL
   USE <db_name>;
   ```

   または、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、目的の catalog 内で直接アクティブなデータベースを指定することもできます。

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) を使用して、指定されたデータベース内の目的のテーブルをクエリします。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Paimon からデータをロード

OLAP テーブル `olap_tbl` があると仮定し、以下のようにデータを変換してロードできます。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM paimon_table;
```

## Paimon テーブルの作成と書き込み

以下は、まだ統合作業中の DDL とバッチライターの説明です。既存の Paimon catalog とデータベースを使用するか、次の例のようにデータベースを作成します。ウェアハウス、メタストア、ストレージ認証情報は catalog の `PROPERTIES` に設定し、テーブルプロパティには指定しないでください。`CREATE DATABASE` では catalog や格納場所の上書き指定はできません。

### データベースとテーブルの作成

`write_db` が存在する場合は `CREATE DATABASE` を省略し、そのデータベースを使用します。テーブルの格納場所は catalog が決定します。`ENGINE` は対象 catalog から `paimon` と推論されます。

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS write_db;
USE write_db;

CREATE TABLE orders (
    id BIGINT NOT NULL,
    dt DATE NOT NULL,
    amount DECIMAL(12, 2)
)
PRIMARY KEY (id, dt)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "4",
    "file.format" = "parquet",
    "merge-engine" = "deduplicate"
);

CREATE TABLE event_log (
    id BIGINT,
    dt DATE,
    event_time DATETIME,
    tags ARRAY<STRING>,
    attributes MAP<STRING, STRING>,
    details STRUCT<source STRING, attempt INT>,
    payload VARIANT
)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "-1",
    "file.format" = "parquet"
);
```

`PRIMARY KEY` は OLAP テーブルモデルではなく、Paimon ネイティブの主キーを定義します。主キーカラムは NULL を許可しません。書き込み可能な主キーテーブルでは、`bucket > 0` を明示的に設定してください。主キーのないテーブルは Append テーブルです。パーティションには通常のカラムを `PARTITION BY (column, ...)` で指定し、パーティション定義や式は使用しません。ネイティブのバケット設定には、`DISTRIBUTED BY` ではなく、`PROPERTIES` の `bucket` と必要に応じて `bucket-key` を使用します。OLAP の `ORDER BY`、`ROLLUP`、集約カラム、生成カラム、`AUTO_INCREMENT` はサポートしていません。

### 書き込み型とデフォルト値

- 新しいテーブルでは、表現可能なスカラー型 `BOOLEAN`、`TINYINT`、`SMALLINT`、`INT`、`BIGINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、`CHAR`、`VARCHAR`/`STRING`、`VARBINARY`、`DATE`、`DATETIME` をサポートします。`ARRAY`、`MAP`、`STRUCT`、`VARIANT` と、サポート対象型を使用したネスト構造もサポートします。
- 新しい `DATETIME` カラムはタイムゾーンなしの Paimon `TIMESTAMP(6)` になります。既存テーブルへの書き込みはネイティブのタイムスタンプ種別を使用しますが、公開 C++ 0.3 ライターがサポートする精度は `0`、`3`、`6`、`9` のみです。精度 `1`、`2`、`4`、`5`、`7`、`8` は書き込み能力の検証時に拒否します。タイムゾーン付きの書き込み先では、元の `DATETIME` をセッションのタイムゾーンで解釈します。作成先の精度が `6` 未満で、元の値のマイクロ秒部分を正確に表現できない場合、ライターは暗黙に切り捨てず、その値を拒否します。元の値にマイクロ秒を超える精度を追加するものではなく、リーダー側のタイムスタンプのマッピングや制限も変わりません。
- 新規作成するネストされた子要素は NULL を許可します。SQL `CREATE TABLE` ではネストされた `NOT NULL` を宣言できません。書き込み時には既存のネイティブの NULL 制約が適用され、`CREATE TABLE LIKE` ではそれらが保持されます。
- 通常の `LARGEINT` カラムや、サポート対象外または表現できない型は、暗黙に範囲を縮小せず明示的に拒否します。`VARIANT` のサポートは、公開 Paimon リーダーとの符号付き 128 ビット整数全域の相互運用性を保証するものではありません。この相互運用性は未検証です。
- `DEFAULT` を省略するか、NULL を許可するカラムに限り `DEFAULT NULL` を指定します。NULL 以外のリテラルや式のデフォルト値はサポートしていません。ネイティブの非 NULL デフォルト値を持つ既存テーブルへ挿入する場合は、カラムの省略や `DEFAULT` に依存せず、明示的な値を指定してください。必須カラムと主キーカラムにも値を指定する必要があります。

### バッチ INSERT と CTAS

```sql
INSERT INTO paimon_catalog_fs.write_db.orders
VALUES (1, '2026-01-01', 12.34);

CREATE TABLE paimon_catalog_fs.write_db.orders_copy
PROPERTIES ("bucket" = "-1", "file.format" = "parquet")
AS SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders;
```

[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) は値またはクエリ結果を受け取ります。主キーのある書き込み先では、StarRocks OLAP の部分更新ではなく、ネイティブの `deduplicate` マージエンジンを使用します。[CTAS](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) はネイティブの新しいテーブルを作成してからクエリ結果を書き込みます。元のテーブルのキーやテーブルオプションは複製しません。この例では Append テーブルを作成します。主キーのある書き込み先を作成するには、`PRIMARY KEY` を宣言し、`bucket > 0` を明示的に設定してください。

CTAS ではテーブル作成とデータ公開を合わせた原子性は保証されません。書き込みに失敗した場合、公開 catalog ではテーブルの識別情報を条件とする原子的な削除ができないため、現在の実装は作成済みテーブルを保守的に保持します。手動でのクリーンアップや再試行の前に、書き込み先を確認して状態を照合してください。公開結果が不明なテーブルを自動削除してはいけません。

### CREATE TABLE LIKE

元のテーブルと作成先は同一の Paimon catalog に属する必要がありますが、異なるデータベースでもかまいません。

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS copy_db;
CREATE TABLE paimon_catalog_fs.copy_db.orders_like
LIKE paimon_catalog_fs.write_db.orders;
```

`LIKE` は、主キーとパーティションキー、ネイティブ型、タイムスタンプの種別と精度、トップレベルおよびネストされた `NOT NULL` 制約を含むネイティブスキーマと、再利用可能なテーブルオプションをコピーします。作成先には新しい識別情報と格納場所が割り当てられ、元のデータファイルやスナップショットはコピーされません。元のテーブル固有の格納場所や認証情報もコピーしません。`LIKE` による作成でも、以下の書き込み能力のチェックは適用されます。

### ライターの制限

- 書き込み先はメインブランチ上のネイティブ File-Store テーブルであり、単一の自動コミット文を使用する必要があります。明示的な複数文トランザクションや、FE 再起動をまたぐ Exactly-Once リカバリーはサポートしていません。
- データファイルは Parquet のみをサポートします。レベルごとのファイル形式設定もこの制限に従います。このライターは外部データパス (`data-file.external-paths`)、別のデータディレクトリ (`data-file.path-directory`)、補助ファイルを含む committable をサポートしていません。
- 主キーテーブルには固定バケット (`bucket > 0`) と `merge-engine = deduplicate` が必要です。Append テーブルは Unaware バケット (`bucket = -1`) または固定バケット (`bucket > 0`、ネイティブで必要な `bucket-key` 設定を含む) をサポートします。
- 主キーテーブルの動的バケット (`bucket = -1`)、Postpone バケット (`bucket = -2`)、グローバルインデックス、Data Evolution、Row Tracking、`FILEBLOB` はサポートしていません。パーティションをまたぐ主キー更新もサポートしていないため、すべてのパーティションカラムを主キーに含める必要があります。
- パーティション値は現在 `BOOLEAN`、整数型、`CHAR`/`VARCHAR`、`DATE` のみをサポートします。データカラムでのタイムスタンプや小数のサポートは、パーティションカラムには適用されません。NULL を許可するパーティションカラムはネイティブの NULL マーカーを使用します。公開 SDK が空文字列や空白のみの文字列を NULL と区別できないため、そのような文字列パーティション値は明示的に拒否します。予約済みのデフォルトパーティション名も拒否します。
- データ書き込みには、分離された `libstarrocks_paimon.so` shim を介してネイティブ C++ ライターのみを使用します。ライターライブラリがない場合や互換性がない場合は失敗し、JNI ライターも JNI フォールバックもありません。FE の Java catalog とスナップショットコミットのプロトコルはデータライターとは別であり、JNI を使用しません。公開 Paimon C++ 0.3 と Java 2.0 SDK のベースラインは変更されていません。

### INSERT OVERWRITE

Paimon の上書きは、StarRocks の一時パーティションを入れ替えるのではなく、スナップショットを公開します。パーティション名や一時パーティションは指定できません。静的パーティションにはカラムと値の組を使用し、すべてのパーティションカラムを指定する必要があります。

| モード | 選択条件 | 入力が空の場合 |
| --- | --- | --- |
| 全体 | `PARTITION` 句がなく、動的上書きが無効な場合。非パーティションテーブルでも使用します。テーブル全体を置き換えます。 | テーブル全体を空にします。 |
| 静的 | `PARTITION (column = value, ...)`。指定したパーティションを置き換えます。 | 指定したパーティションを空にします。 |
| 動的 | 静的パーティション句がなく、パーティションテーブルで `dynamic_overwrite = true` の場合。入力行に含まれるパーティションのみを置き換え、他は保持します。 | 対象パーティションがないため、既存データは変更されません。 |

```sql
INSERT OVERWRITE paimon_catalog_fs.write_db.orders
PARTITION (dt = '2026-01-01')
SELECT id, amount FROM paimon_catalog_fs.write_db.orders_copy
WHERE dt = '2026-01-01';

INSERT /*+ SET_VAR(dynamic_overwrite = true) */ OVERWRITE paimon_catalog_fs.write_db.orders
SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders_copy;
```

入力が空の `INSERT INTO` もデータを変更しません。全体上書きと静的上書きでは、入力が空でも対象データを消去します。

### コミット結果と再読み取り

空でない書き込み、および入力が空の全体上書きや静的上書きは、FE が Paimon スナップショットの公開を確認した後にのみ成功を返します。BE がファイル書き込みを完了しただけでは成功ではありません。入力が空の追加や動的上書きは、データ変更なしとして確認され、新しいスナップショットは不要です。

:::warning
エラーに `Paimon commit outcome is unknown for operation ...; preserve its files and reconcile the snapshot before retrying` が含まれる場合、公開結果はすでに永続化されている可能性があります。ファイルと CTAS の作成先を保持し、操作識別子を記録して、Paimon スナップショットを照合してから再試行の要否を判断してください。クライアントに失敗やキャンセルが返っても、文の無条件な再実行や出力の削除はしないでください。
:::

書き込みによって、上記の `VARIANT` の制限やタイムスタンプとタイムゾーンのマッピングを含む既存のリーダーの制限がなくなるわけではありません。この機能の再読み取り検証は、ネイティブ C++ リーダーと StarRocks ネイティブ Parquet リーダーに限定し、JNI 相互運用性は対象外です。主キーテーブルの結果には、物理的な入力行数ではなくネイティブのマージセマンティクスが適用されます。リーダーの選択、Compaction、タイムスタンプとタイムゾーンの動作、ネイティブ経路での再読み取りには、引き続きエンドツーエンドの実行時検証が必要です。
