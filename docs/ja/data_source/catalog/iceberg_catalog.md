---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg']
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import QSTip from '../../_assets/commonMarkdown/quickstart-iceberg-tip.mdx'

# Iceberg catalog

<QSTip />

Iceberg catalog は、StarRocks が v2.4 以降でサポートする external catalog の一種です。Iceberg catalog を使用すると、以下のことが可能です。

- Iceberg に保存されたデータを直接クエリし、手動でテーブルを作成する必要がありません。
- Iceberg に保存されたデータを処理し、StarRocks にロードするために [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) または非同期マテリアライズドビュー（v2.5 以降でサポート）を使用します。
- StarRocks 上で操作を行い、Iceberg データベースやテーブルを作成または削除したり、StarRocks テーブルから Parquet 形式の Iceberg テーブルにデータをシンクすることができます（この機能は v3.1 以降でサポート）。

Iceberg クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム（HDFS）またはオブジェクトストレージ（AWS S3、Microsoft Azure Storage、Google GCS、または他の S3 互換ストレージシステム（例: MinIO））

- メタストア（Hive metastore、AWS Glue、または Tabular）

:::note

- ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。
- メタストアとして Tabular を選択した場合、Iceberg REST catalog を使用する必要があります。

:::

## 使用上の注意

- StarRocks がサポートする Iceberg のファイル形式は Parquet と ORC です。

  - Parquet ファイルは以下の圧縮形式をサポートします: SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION。
  - ORC ファイルは以下の圧縮形式をサポートします: ZLIB, SNAPPY, LZO, LZ4, ZSTD, NO_COMPRESSION。

- Iceberg catalog は v1 テーブルをサポートします。さらに、Iceberg catalog は StarRocks v3.0 以降で ORC 形式の v2 テーブルをサポートし、StarRocks v3.1 以降で Parquet 形式の v2 テーブルをサポートします。

## 統合準備

Iceberg catalog を作成する前に、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアと統合できることを確認してください。

---

### ストレージ

ストレージタイプに応じたタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

Iceberg クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます。

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

ストレージとして HDFS を選択した場合、StarRocks クラスターを次のように設定します。

- （オプション）HDFS クラスターおよび Hive メタストアにアクセスするためのユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive メタストアにアクセスするために FE と BE または CN プロセスのユーザー名を使用します。各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭と、各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することで、ユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE と各 BE または CN を再起動して、パラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名のみを設定できます。
- Iceberg データをクエリする際、StarRocks クラスターの FEs と BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。以下の状況でのみ StarRocks クラスターを設定する必要があります。

  - HDFS クラスターに高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターに View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスと各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

:::tip

クエリを送信したときに不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

:::

---

#### Kerberos 認証

HDFS クラスターまたは Hive メタストアに Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定します。

- 各 FE と各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターと Hive メタストアにアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感です。そのため、このコマンドを定期的に実行するために cron を使用する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルと各 BE の **$BE_HOME/conf/be.conf** ファイルまたは各 CN の **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

</TabItem>

</Tabs>

---

## Iceberg catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "iceberg",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

---

### パラメータ

#### catalog_name

Iceberg catalog の名前。命名規則は次のとおりです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字小文字を区別し、長さは 1023 文字を超えることはできません。

#### comment

Iceberg catalog の説明。このパラメータはオプションです。

#### type

データソースのタイプ。値を `iceberg` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータ。メタストアタイプに応じたタブを選択してください。

<Tabs groupId="metastore">
<TabItem value="HIVE" label="Hive metastore" default>

##### Hive metastore

データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note

Iceberg データをクエリする前に、Hive メタストアノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始するときに StarRocks が Hive メタストアにアクセスできない可能性があります。

:::

次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

##### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `hive` に設定します。

##### hive.metastore.uris

必須: はい
説明: Hive メタストアの URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive メタストアに高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。

</TabItem>
<TabItem value="GLUE" label="AWS Glue">

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。次のいずれかの操作を行います。

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

AWS Glue の `MetastoreParams`:

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `glue` に設定します。

###### aws.glue.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` と `false`。 デフォルト値: `false`。

###### aws.glue.iam_role_arn

必須: いいえ
説明: AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。アサインされたロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.region

必須: はい
説明: AWS Glue Data Catalog が存在するリージョン。例: `us-west-1`。

###### aws.glue.access_key

必須: いいえ
説明: AWS IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.secret_key

必須: いいえ
説明: AWS IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

AWS Glue へのアクセスに使用する認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

</TabItem>
<TabItem value="TABULAR" label="Tabular">

##### Tabular

メタストアとして Tabular を使用する場合、メタストアタイプを REST (`"iceberg.catalog.type" = "rest"`) として指定する必要があります。`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri" = "<rest_server_api_endpoint>",
"iceberg.catalog.credential" = "<credential>",
"iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>"
```

Tabular の `MetastoreParams`:

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `rest` に設定します。

###### iceberg.catalog.uri

必須: はい
説明: Tabular サービスエンドポイントの URI。例: `https://api.tabular.io/ws`。

###### iceberg.catalog.credential

必須: はい
説明: Tabular サービスの認証情報。

###### iceberg.catalog.warehouse

必須: いいえ
説明: Iceberg catalog のウェアハウスの場所または識別子。例: `s3://my_bucket/warehouse_location` または `sandbox`。

次の例は、メタストアとして Tabular を使用する Iceberg catalog `tabular` を作成します。

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
</TabItem>

</Tabs>

---

#### `StorageCredentialParams`

StarRocks がストレージシステムと統合する方法に関する一連のパラメータ。このパラメータセットはオプションです。

次の点に注意してください。

- ストレージとして HDFS を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。AWS S3、他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

- メタストアとして Tabular を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。メタストアとして HMS または AWS Glue を使用する場合、`StorageCredentialParams` を設定する必要があります。

ストレージタイプに応じたタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

##### AWS S3

Iceberg クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います。

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

`StorageCredentialParams` for AWS S3:

###### aws.s3.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` と `false`。 デフォルト値: `false`。

###### aws.s3.iam_role_arn

必須: いいえ
説明: AWS S3 バケットに対する権限を持つ IAM ロールの ARN。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.region

必須: はい
説明: AWS S3 バケットが存在するリージョン。例: `us-west-1`。

###### aws.s3.access_key

必須: いいえ
説明: IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.secret_key

必須: いいえ
説明: IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

AWS S3 へのアクセスに使用する認証方法の選択方法や AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

HDFS ストレージを使用する場合、ストレージクレデンシャルをスキップします。

</TabItem>

<TabItem value="MINIO" label="MinIO" >

##### S3 互換ストレージシステム

Iceberg catalog は v2.5 以降で S3 互換ストレージシステムをサポートします。

S3 互換ストレージシステム（例: MinIO）を Iceberg クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

MinIO およびその他の S3 互換システムの `StorageCredentialParams`:

###### aws.s3.enable_ssl

必須: はい
説明: SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。 デフォルト値: `true`。

###### aws.s3.enable_path_style_access

必須: はい
説明: パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。 デフォルト値: `false`。 MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。 例: US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスする場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。

###### aws.s3.endpoint

必須: はい
説明: AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイント。

###### aws.s3.access_key

必須: はい
説明: IAM ユーザーのアクセスキー。

###### aws.s3.secret_key

必須: はい
説明: IAM ユーザーのシークレットキー。

</TabItem>

<TabItem value="AZURE" label="Microsoft Azure Blob Storage" >

##### Microsoft Azure Storage

Iceberg catalog は v3.0 以降で Microsoft Azure Storage をサポートします。

###### Azure Blob Storage

Iceberg クラスターのストレージとして Blob Storage を選択した場合、次のいずれかの操作を行います。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

Microsoft Azure の `StorageCredentialParams`:

###### azure.blob.storage_account

必須: はい
説明: Blob Storage アカウントのユーザー名。

###### azure.blob.shared_key

必須: はい
説明: Blob Storage アカウントの共有キー。

###### azure.blob.account_name

必須: はい
説明: Blob Storage アカウントのユーザー名。

###### azure.blob.container

必須: はい
説明: データを保存する Blob コンテナの名前。

###### azure.blob.sas_token

必須: はい
説明: Blob Storage アカウントにアクセスするために使用される SAS トークン。

###### Azure Data Lake Storage Gen1

Iceberg クラスターのストレージとして Data Lake Storage Gen1 を選択した場合、次のいずれかの操作を行います。

- マネージドサービスアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

または:

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

###### Azure Data Lake Storage Gen2

Iceberg クラスターのストレージとして Data Lake Storage Gen2 を選択した場合、次のいずれかの操作を行います。

- マネージドアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

または:

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

または:

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

</TabItem>

<TabItem value="GCS" label="Google GCS" >

##### Google GCS

Iceberg catalog は v3.0 以降で Google GCS をサポートします。

Iceberg クラスターのストレージとして Google GCS を選択した場合、次のいずれかの操作を行います。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  - VM インスタンスをサービスアカウントにインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

  - サービスアカウント（仮にメタサービスアカウントと呼ぶ）が別のサービスアカウント（仮にデータサービスアカウントと呼ぶ）にインパーソネートする場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

Google GCS の `StorageCredentialParams`:

###### gcp.gcs.service_account_email

デフォルト値: ""
例: "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)"
説明: サービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。

###### gcp.gcs.service_account_private_key_id

デフォルト値: ""
例: "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"
説明: サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。

###### gcp.gcs.service_account_private_key

デフォルト値: ""
例: "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"
説明: サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。

###### gcp.gcs.impersonation_service_account

デフォルト値: ""
例: "hello"
説明: インパーソネートしたいサービスアカウント。

</TabItem>

</Tabs>

---

#### MetadataUpdateParams

StarRocks が Hive のメタデータをキャッシュする方法に関する一連のパラメータ。このパラメータセットはオプションです。

現在、このパラメータセットには、Iceberg テーブルのポインタとパーティション名をキャッシュするかどうかを指定する `enable_iceberg_metadata_cache` という1つのパラメータのみが含まれています。このパラメータは v3.2.1 以降でサポートされています。

- v3.2.1 から v3.2.3 までは、使用するメタストアサービスに関係なく、このパラメータはデフォルトで `true` に設定されています。
- v3.2.4 以降では、Iceberg クラスターがメタストアとして AWS Glue を使用する場合、このパラメータは引き続きデフォルトで `true` に設定されています。しかし、Iceberg クラスターが Hive metastore などの他のメタストアサービスを使用する場合、このパラメータはデフォルトで `false` に設定されています。

### 例

以下の例は、使用するメタストアのタイプに応じて、Iceberg クラスターからデータをクエリするための Iceberg catalog `iceberg_catalog_hms` または `iceberg_catalog_glue` を作成します。ストレージタイプに応じたタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

#### AWS S3

##### インスタンスプロファイルベースのクレデンシャルを選択した場合

- Iceberg クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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

- Iceberg クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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

- Iceberg クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。

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
</TabItem>

<TabItem value="HDFS" label="HDFS" >

#### HDFS

ストレージとして HDFS を使用する場合、次のようなコマンドを実行します。

```SQL
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

</TabItem>

<TabItem value="MINIO" label="MinIO" >

#### S3 互換ストレージシステム

MinIO を例として使用します。次のようなコマンドを実行します。

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
</TabItem>

<TabItem value="AZURE" label="Microsoft Azure Blob Storage" >

#### Microsoft Azure Storage

##### Azure Blob Storage

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します。

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

- SAS トークン認証方法を選択する場合、次のようなコマンドを実行します。

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

- マネージドサービスアイデンティティ認証方法を選択する場合、次のようなコマンドを実行します。

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

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します。

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

- マネージドアイデンティティ認証方法を選択する場合、次のようなコマンドを実行します。

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

- 共有キー認証方法を選択する場合、次のようなコマンドを実行します。

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

- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します。

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

</TabItem>

<TabItem value="GCS" label="Google GCS" >

#### Google GCS

- VM ベースの認証方法を選択する場合、次のようなコマンドを実行します。

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

- サービスアカウントベースの認証方法を選択する場合、次のようなコマンドを実行します。

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

  - VM インスタンスをサービスアカウントにインパーソネートさせる場合、次のようなコマンドを実行します。

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

  - サービスアカウントが別のサービスアカウントにインパーソネートする場合、次のようなコマンドを実行します。

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
</TabItem>

</Tabs>
 
 ---

## カタログの使用

### Iceberg catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。次の例では、`iceberg_catalog_glue` という名前の Iceberg catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

---

### Iceberg Catalog とその中のデータベースに切り替える

Iceberg catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます。

- 現在のセッションで Iceberg catalog を指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

- [USE](../../sql-reference/sql-statements/Database/USE.md) を直接使用して、Iceberg catalog とその中のデータベースに切り替えます。

  ```SQL
  USE <catalog_name>.<db_name>
  ```
 
---

### Iceberg catalog を削除する

外部 catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

次の例では、`iceberg_catalog_glue` という名前の Iceberg catalog を削除します。

```SQL
DROP Catalog iceberg_catalog_glue;
```

---

### Iceberg テーブルのスキーマを表示する

Iceberg テーブルのスキーマを表示するには、次のいずれかの構文を使用できます。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

---

### Iceberg テーブルをクエリする

1. Iceberg クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の宛先テーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

---

### Iceberg データベースを作成する

StarRocks の内部 catalog と同様に、Iceberg catalog に対して [CREATE DATABASE](../../administration/user_privs/privilege_item.md#catalog) 権限を持っている場合、その Iceberg catalog 内でデータベースを作成するために [CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

:::tip

[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) と [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

:::

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、その catalog 内で Iceberg データベースを作成するために次のステートメントを使用します。

```SQL
CREATE DATABASE <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

`location` パラメータを使用して、データベースを作成するファイルパスを指定できます。HDFS とクラウドストレージの両方がサポートされています。`location` パラメータを指定しない場合、StarRocks は Iceberg catalog のデフォルトのファイルパスにデータベースを作成します。

`prefix` は使用するストレージシステムに基づいて異なります。

#### HDFS

`Prefix` 値: `hdfs`

#### Google GCS

`Prefix` 値: `gs`

#### Azure Blob Storage

`Prefix` 値:

- ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `wasb` です。
- ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `wasbs` です。

#### Azure Data Lake Storage Gen1

`Prefix` 値: `adl`

#### Azure Data Lake Storage Gen2

`Prefix` 値:

- ストレージアカウントが HTTP 経由でのアクセスを許可する場合、`prefix` は `abfs` です。
- ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、`prefix` は `abfss` です。

#### AWS S3 または他の S3 互換ストレージ（例: MinIO）

`Prefix` 値: `s3`

---

### Iceberg データベースを削除する

StarRocks の内部データベースと同様に、Iceberg データベースに対して [DROP](../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、その Iceberg データベースを削除するために [DROP DATABASE](../../sql-reference/sql-statements/Database/DROP_DATABASE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。空のデータベースのみを削除できます。

Iceberg データベースを削除すると、そのデータベースのファイルパスは HDFS クラスターまたはクラウドストレージ上で削除されません。

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、その catalog 内で Iceberg データベースを削除するために次のステートメントを使用します。

```SQL
DROP DATABASE <database_name>;
```

---

### Iceberg テーブルを作成する

StarRocks の内部データベースと同様に、Iceberg データベースに対して [CREATE TABLE](../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、その Iceberg データベース内でテーブルを作成するために [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) または [CREATE TABLE AS SELECT](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベース内で Iceberg テーブルを作成するために次の構文を使用します。

#### 構文

```SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...
partition_column_definition1,partition_column_definition2...])
[partition_desc]
[PROPERTIES ("key" = "value", ...)]
[AS SELECT query]
```

#### パラメータ

##### column_definition

`column_definition` の構文は次のとおりです。

```SQL
col_name col_type [COMMENT 'comment']
```

:::note

すべての非パーティション列はデフォルト値として `NULL` を使用する必要があります。つまり、テーブル作成ステートメントで各非パーティション列に対して `DEFAULT "NULL"` を指定する必要があります。さらに、パーティション列は非パーティション列の後に定義され、デフォルト値として `NULL` を使用することはできません。

:::

##### partition_desc

`partition_desc` の構文は次のとおりです。

```SQL
PARTITION BY (par_col1[, par_col2...])
```

現在、StarRocks は [identity transforms](https://iceberg.apache.org/spec/#partitioning) のみをサポートしており、StarRocks は各ユニークなパーティション値に対してパーティションを作成します。

:::note

パーティション列は非パーティション列の後に定義される必要があります。パーティション列は FLOAT、DOUBLE、DECIMAL、および DATETIME を除くすべてのデータ型をサポートし、デフォルト値として `NULL` を使用することはできません。

:::

##### PROPERTIES

`PROPERTIES` で `"key" = "value"` 形式でテーブル属性を指定できます。[Iceberg テーブル属性](https://iceberg.apache.org/docs/latest/configuration/) を参照してください。

次の表は、いくつかの主要なプロパティを説明しています。

###### location

説明: Iceberg テーブルを作成するファイルパス。HMS をメタストアとして使用する場合、`location` パラメータを指定する必要はありません。StarRocks は現在の Iceberg catalog のデフォルトのファイルパスにテーブルを作成します。AWS Glue をメタストアとして使用する場合:

- テーブルを作成するデータベースに `location` パラメータを指定した場合、テーブルに対して `location` パラメータを指定する必要はありません。この場合、テーブルは所属するデータベースのファイルパスにデフォルトで設定されます。
- テーブルを作成するデータベースに `location` を指定していない場合、テーブルに対して `location` パラメータを指定する必要があります。

###### file_format

説明: Iceberg テーブルのファイル形式。Parquet 形式のみがサポートされています。デフォルト値: `parquet`。

###### compression_codec

説明: Iceberg テーブルに使用される圧縮アルゴリズム。サポートされている圧縮アルゴリズムは SNAPPY、GZIP、ZSTD、および LZ4 です。デフォルト値: `gzip`。このプロパティは v3.2.3 で非推奨となり、それ以降のバージョンでは Iceberg テーブルにデータをシンクするために使用される圧縮アルゴリズムはセッション変数 [connector_sink_compression_codec](../../sql-reference/System_variable.md#connector_sink_compression_codec) によって一元的に制御されます。

---

### 例

1. `unpartition_tbl` という名前の非パーティションテーブルを作成します。このテーブルは `id` と `score` の2つの列で構成されています。

   ```SQL
   CREATE TABLE unpartition_tbl
   (
       id int,
       score double
   );
   ```

2. `partition_tbl_1` という名前のパーティションテーブルを作成します。このテーブルは `action`、`id`、および `dt` の3つの列で構成されており、そのうち `id` と `dt` はパーティション列として定義されています。

   ```SQL
   CREATE TABLE partition_tbl_1
   (
       action varchar(20),
       id int,
       dt date
   )
   PARTITION BY (id,dt);
   ```

3. 既存のテーブル `partition_tbl_1` をクエリし、そのクエリ結果に基づいて `partition_tbl_2` という名前のパーティションテーブルを作成します。`partition_tbl_2` では、`id` と `dt` がパーティション列として定義されています。

   ```SQL
   CREATE TABLE partition_tbl_2
   PARTITION BY (id, dt)
   AS SELECT * from employee;
   ```

---

### Iceberg テーブルにデータをシンクする

StarRocks の内部テーブルと同様に、Iceberg テーブルに対して [INSERT](../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、StarRocks テーブルのデータをその Iceberg テーブルにシンクするために [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用できます（現在、Parquet 形式の Iceberg テーブルのみがサポートされています）。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベース内の Parquet 形式の Iceberg テーブルに StarRocks テーブルのデータをシンクするために次の構文を使用します。

#### 構文

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 指定されたパーティションにデータをシンクする場合、次の構文を使用します。
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

:::note

パーティション列は `NULL` 値を許可しません。したがって、Iceberg テーブルのパーティション列に空の値がロードされないようにする必要があります。

:::

#### パラメータ

##### INTO

StarRocks テーブルのデータを Iceberg テーブルに追加します。

##### OVERWRITE

StarRocks テーブルのデータで Iceberg テーブルの既存のデータを上書きします。

##### column_name

データをロードしたい宛先列の名前。1つ以上の列を指定できます。複数の列を指定する場合、カンマ (`,`) で区切ります。Iceberg テーブルに実際に存在する列のみを指定でき、指定する宛先列には Iceberg テーブルのパーティション列が含まれている必要があります。指定する宛先列は、StarRocks テーブルの列と順番に1対1でマッピングされます。宛先列名が何であっても関係ありません。宛先列が指定されていない場合、データは Iceberg テーブルのすべての列にロードされます。StarRocks テーブルの非パーティション列が Iceberg テーブルのどの列にもマッピングできない場合、StarRocks は Iceberg テーブル列にデフォルト値 `NULL` を書き込みます。INSERT ステートメントにクエリステートメントが含まれており、その返された列タイプが宛先列のデータタイプと異なる場合、StarRocks は不一致の列に対して暗黙の変換を行います。変換に失敗した場合、構文解析エラーが返されます。

##### expression

宛先列に値を割り当てる式。

##### DEFAULT

宛先列にデフォルト値を割り当てます。

##### query

クエリステートメントの結果を Iceberg テーブルにロードします。StarRocks がサポートする任意の SQL ステートメントを使用できます。

##### PARTITION

データをロードしたいパーティション。Iceberg テーブルのすべてのパーティション列をこのプロパティで指定する必要があります。このプロパティで指定するパーティション列は、テーブル作成ステートメントで定義したパーティション列の順序と異なる順序で指定できます。このプロパティを指定する場合、`column_name` プロパティを指定することはできません。

#### 例

1. `partition_tbl_1` テーブルに3つのデータ行を挿入します。

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

4. `partition_tbl_2` テーブルの2つの条件、`dt='2023-09-01'` および `id=1` を満たすパーティションに SELECT クエリの結果を挿入します。

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. `partition_tbl_1` テーブルの2つの条件、`dt='2023-09-01'` および `id=1` を満たすパーティションのすべての `action` 列の値を `close` で上書きします。

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

---

### Iceberg テーブルを削除する

StarRocks の内部テーブルと同様に、Iceberg テーブルに対して [DROP](../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、その Iceberg テーブルを削除するために [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

Iceberg テーブルを削除すると、そのテーブルのファイルパスとデータは HDFS クラスターまたはクラウドストレージ上で削除されません。

Iceberg テーブルを強制的に削除する場合（つまり、DROP TABLE ステートメントで `FORCE` キーワードを指定した場合）、そのテーブルのデータは HDFS クラスターまたはクラウドストレージ上でテーブルと共に削除されますが、テーブルのファイルパスは保持されます。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベース内で Iceberg テーブルを削除するために次のステートメントを使用します。

```SQL
DROP TABLE <table_name> [FORCE];
```

---

### メタデータキャッシュの設定

Iceberg クラスターのメタデータファイルは、AWS S3 や HDFS などのリモートストレージに保存されている場合があります。デフォルトでは、StarRocks は Iceberg メタデータをメモリにキャッシュします。クエリを高速化するために、StarRocks はメモリとディスクの両方にメタデータをキャッシュできる2レベルのメタデータキャッシングメカニズムを採用しています。各初回クエリに対して、StarRocks はその計算結果をキャッシュします。以前のクエリと意味的に等価な後続のクエリが発行された場合、StarRocks は最初にそのキャッシュから要求されたメタデータを取得し、キャッシュでメタデータがヒットしない場合にのみリモートストレージからメタデータを取得します。

StarRocks は、最も最近使用されたものを最初に削除する（LRU）アルゴリズムを使用してデータをキャッシュおよび削除します。基本的なルールは次のとおりです。

- StarRocks は最初にメモリから要求されたメタデータを取得しようとします。メモリでメタデータがヒットしない場合、StarRocks はディスクからメタデータを取得しようとします。StarRocks がディスクから取得したメタデータはメモリにロードされます。ディスクでもメタデータがヒットしない場合、StarRocks はリモートストレージからメタデータを取得し、取得したメタデータをメモリにキャッシュします。
- StarRocks はメモリから削除されたメタデータをディスクに書き込みますが、ディスクから削除されたメタデータは直接破棄します。

#### Iceberg メタデータキャッシングパラメータ

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
デフォルト値: `2147483648`、2 GB に相当
説明: ディスク上で許可されるキャッシュされたメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_capacity

単位: バイト
デフォルト値: `536870912`、512 MB に相当
説明: メモリ内で許可されるキャッシュされたメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_expiration_seconds

単位: 秒  
デフォルト値: `86500`
説明: メモリ内のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_disk_cache_expiration_seconds

単位: 秒  
デフォルト値: `604800`、1 週間に相当
説明: ディスク上のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_cache_max_entry_size

単位: バイト
デフォルト値: `8388608`、8 MB に相当
説明: キャッシュできるファイルの最大サイズ。このパラメータの値を超えるサイズのファイルはキャッシュできません。クエリがこれらのファイルを要求する場合、StarRocks はリモートストレージからそれらを取得します。