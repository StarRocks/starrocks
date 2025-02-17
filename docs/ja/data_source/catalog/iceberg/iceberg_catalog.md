---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg']
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import QSTip from '../../../_assets/commonMarkdown/quickstart-iceberg-tip.mdx'

# Iceberg catalog

<QSTip />

Iceberg catalog は、StarRocks が v2.4 以降でサポートする external catalog の一種です。Iceberg catalog を使用すると、以下のことが可能です：

- Iceberg に保存されたデータを直接クエリし、手動でテーブルを作成する必要がありません。
- Iceberg に保存されたデータを処理し、StarRocks にデータをロードするために [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) または非同期マテリアライズドビュー（v2.5 以降でサポート）を使用します。
- StarRocks 上で操作を行い、Iceberg データベースやテーブルを作成または削除したり、StarRocks テーブルから Parquet 形式の Iceberg テーブルにデータをシンクするために [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用します（この機能は v3.1 以降でサポートされます）。

Iceberg クラスターでの SQL ワークロードを成功させるためには、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています：

- 分散ファイルシステム（HDFS）または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム（例：MinIO）

- メタストアとして Hive metastore、AWS Glue、または Tabular

:::note

- ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。
- メタストアとして Tabular を選択した場合、Iceberg REST catalog を使用する必要があります。

:::

## 使用上の注意

Iceberg からデータをクエリする際に StarRocks を使用する場合、以下の点に注意してください：

| **ファイル形式** | **圧縮形式**                                   | **Iceberg テーブルバージョン**                                           |
| --------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| Parquet         | SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION      | <ul><li>v1 テーブル: サポートされています。 </li><li>v2 テーブル: v3.1 以降でサポートされ、これらの v2 テーブルに対するクエリは位置削除をサポートします。v3.1.10、v3.2.5、v3.3 およびそれ以降のバージョンでは、v2 テーブルに対するクエリは等価削除もサポートします。 </li></ul> |
| ORC             | ZLIB, SNAPPY, LZO, LZ4, ZSTD, NO_COMPRESSION | <ul><li>v1 テーブル: サポートされています。 </li><li>v2 テーブル: v3.0 以降でサポートされ、これらの v2 テーブルに対するクエリは位置削除をサポートします。v3.1.8、v3.2.3、v3.3 およびそれ以降のバージョンでは、v2 テーブルに対するクエリは等価削除もサポートします。 </li></ul> |

## 統合準備

Iceberg catalog を作成する前に、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアと統合できることを確認してください。

---

### ストレージ

ストレージタイプに一致するタブを選択してください：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

Iceberg クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

以下の認証方法が推奨されます：

- インスタンスプロファイル
- 想定ロール
- IAM ユーザー

上記の三つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

ストレージとして HDFS を選択した場合、StarRocks クラスターを以下のように設定してください：

- （オプション）HDFS クラスターおよび Hive metastore にアクセスするためのユーザー名を設定します。デフォルトでは、StarRocks は FE および BE または CN プロセスのユーザー名を使用して HDFS クラスターおよび Hive metastore にアクセスします。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭および各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに一つのユーザー名のみを設定できます。
- Iceberg データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。以下の状況でのみ StarRocks クラスターを設定する必要があります：

  - HDFS クラスターに高可用性（HA）が有効になっている場合：HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターに View File System (ViewFs) が有効になっている場合：HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パスおよび各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

:::tip

クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

:::

---

#### Kerberos 認証

HDFS クラスターまたは Hive metastore に Kerberos 認証が有効になっている場合、StarRocks クラスターを以下のように設定してください：

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive metastore にアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用して定期的にこのコマンドを実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイルおよび各 BE の **$BE_HOME/conf/be.conf** ファイルまたは各 CN の **$CN_HOME/conf/cn.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。

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

Iceberg catalog の名前です。命名規則は以下の通りです：

- 名前には文字、数字（0-9）、アンダースコア（_）を含めることができます。文字で始まる必要があります。
- 名前は大文字小文字を区別し、1023 文字を超えることはできません。

#### comment

Iceberg catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `iceberg` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。メタストアタイプに一致するタブを選択してください：

<Tabs groupId="metastore">
<TabItem value="HIVE" label="Hive metastore" default>

##### Hive metastore

データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を以下のように設定します：

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note

Iceberg データをクエリする前に、Hive metastore ノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive metastore にアクセスできない可能性があります。

:::

以下の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

##### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプです。値を `hive` に設定します。

##### hive.metastore.uris

必須: はい
説明: Hive metastore の URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive metastore に高可用性（HA）が有効になっている場合、複数のメタストア URI を指定し、カンマ（`,`）で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。

</TabItem>
<TabItem value="GLUE" label="AWS Glue">

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合（ストレージとして AWS S3 を選択した場合のみサポート）、以下のいずれかの操作を行います：

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を以下のように設定します：

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

AWS Glue 用の `MetastoreParams`：

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプです。値を `glue` に設定します。

###### aws.glue.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` および `false`。デフォルト値: `false`。

###### aws.glue.iam_role_arn

必須: いいえ
説明: AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。想定ロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.region

必須: はい
説明: AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。

###### aws.glue.access_key

必須: いいえ
説明: AWS IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.secret_key

必須: いいえ
説明: AWS IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

AWS Glue へのアクセス認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセス認証パラメータ](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

</TabItem>
<TabItem value="REST" label="REST">

##### REST

REST をメタストアとして使用する場合、メタストアタイプを REST として指定する必要があります（`"iceberg.catalog.type" = "rest"`）。`MetastoreParams` を以下のように設定します：

```SQL
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri" = "<rest_server_api_endpoint>",
"iceberg.catalog.security" = "oauth2",
"iceberg.catalog.oauth2.credential" = "<credential>",
"iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>"
```

REST catalog 用の `MetastoreParams`：

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプです。値を `rest` に設定します。

###### iceberg.catalog.uri

必須: はい
説明: REST サービスエンドポイントの URI です。例: `https://api.tabular.io/ws`。

###### iceberg.catalog.security

必須: いいえ

説明: 使用する認証プロトコルのタイプです。デフォルト: `NONE`。有効な値: `OAUTH2`、`token` または `credential` が必要です。

###### iceberg.catalog.oauth2.token

必須: いいえ

説明: サーバーとのやり取りに使用されるベアラートークンです。`OAUTH2` 認証プロトコルには `token` または `credential` が必要です。例: `AbCdEf123456`。

###### iceberg.catalog.oauth2.credential

必須: いいえ

説明: サーバーとの OAuth2 クライアント資格情報フローでトークンと交換するための資格情報です。`OAUTH2` 認証プロトコルには `token` または `credential` が必要です。例: `AbCdEf123456`。

###### iceberg.catalog.oauth2.scope

必須: いいえ

説明: REST Catalog と通信する際に使用されるスコープです。`credential` を使用する場合にのみ適用されます。

###### iceberg.catalog.oauth2.server-uri

必須: いいえ

説明: OAuth2 サーバーからアクセストークンを取得するためのエンドポイントです。

###### iceberg.catalog.vended-credentials-enabled

必須: いいえ

説明: ネストされた名前空間の下のオブジェクトのクエリをサポートするかどうか。デフォルト: `true`。

###### iceberg.catalog.warehouse

必須: いいえ
説明: Iceberg catalog のウェアハウスの場所または識別子です。例: `s3://my_bucket/warehouse_location` または `sandbox`。

以下の例は、Tabular をメタストアとして使用する Iceberg catalog `tabular` を作成します：

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

以下の例は、Polaris をメタストアとして使用する Iceberg catalog `smith_polaris` を作成します：

```sql
CREATE EXTERNAL CATALOG smith_polaris 
PROPERTIES (   
    "iceberg.catalog.uri"  = "http://xxx.xx.xx.xxx:8181/api/catalog", 
    "type"  =  "iceberg",   
    "iceberg.catalog.type"  =  "rest",   
    "iceberg.catalog.warehouse" = "starrocks_catalog",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL'
 );

# `ns1.ns2.tpch_namespace` はネストされた名前空間です
create table smith_polaris.`ns1.ns2.tpch_namespace`.tbl (c1 string);

mysql> select * from smith_polaris.`ns1.ns2.tpch_namespace`.tbl;
+------+
| c1   |
+------+
| 1    |
| 2    |
| 3    |
+------+
3 rows in set (0.34 sec)
```

</TabItem>

</Tabs>

---

#### `StorageCredentialParams`

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

以下の点に注意してください：

- ストレージとして HDFS を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

- メタストアとして Tabular を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。メタストアとして HMS または AWS Glue を使用する場合、`StorageCredentialParams` を設定する必要があります。

ストレージタイプに一致するタブを選択してください：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

##### AWS S3

Iceberg クラスターのストレージとして AWS S3 を選択した場合、以下のいずれかの操作を行います：

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

AWS S3 用の `StorageCredentialParams`：

###### aws.s3.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。

###### aws.s3.iam_role_arn

必須: いいえ
説明: AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。想定ロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.region

必須: はい
説明: AWS S3 バケットが存在するリージョンです。例: `us-west-1`。

###### aws.s3.access_key

必須: いいえ
説明: IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.secret_key

必須: いいえ
説明: IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

AWS S3 へのアクセス認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセス認証パラメータ](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

HDFS ストレージを使用する場合、ストレージ資格情報をスキップします。

</TabItem>

<TabItem value="MINIO" label="MinIO" >

##### S3 互換ストレージシステム

Iceberg catalog は v2.5 以降で S3 互換ストレージシステムをサポートします。

MinIO などの S3 互換ストレージシステムを選択した場合、Iceberg クラスターのストレージとして `StorageCredentialParams` を以下のように設定して、統合を成功させます：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

MinIO およびその他の S3 互換システム用の `StorageCredentialParams`：

###### aws.s3.enable_ssl

必須: はい
説明: SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。

###### aws.s3.enable_path_style_access

必須: はい
説明: パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。

###### aws.s3.endpoint

必須: はい
説明: AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。

###### aws.s3.access_key

必須: はい
説明: IAM ユーザーのアクセスキーです。

###### aws.s3.secret_key

必須: はい
説明: IAM ユーザーのシークレットキーです。

</TabItem>

<TabItem value="AZURE" label="Microsoft Azure Blob Storage" >

##### Microsoft Azure Storage

Iceberg catalog は v3.0 以降で Microsoft Azure Storage をサポートします。

###### Azure Blob Storage

Blob Storage を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの操作を行います：

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

Microsoft Azure 用の `StorageCredentialParams`：

###### azure.blob.storage_account

必須: はい
説明: Blob Storage アカウントのユーザー名です。

###### azure.blob.shared_key

必須: はい
説明: Blob Storage アカウントの共有キーです。

###### azure.blob.account_name

必須: はい
説明: Blob Storage アカウントのユーザー名です。

###### azure.blob.container

必須: はい
説明: データを保存するブロブコンテナの名前です。

###### azure.blob.sas_token

必須: はい
説明: Blob Storage アカウントにアクセスするために使用される SAS トークンです。

###### Azure Data Lake Storage Gen1

Data Lake Storage Gen1 を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの操作を行います：

- マネージドサービスアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

または：

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

###### Azure Data Lake Storage Gen2

Data Lake Storage Gen2 を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの操作を行います：

- マネージドアイデンティティ認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  または：

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  または：

- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

</TabItem>

<TabItem value="GCS" label="Google GCS" >

##### Google GCS

Iceberg catalog は v3.0 以降で Google GCS をサポートします。

Google GCS を Iceberg クラスターのストレージとして選択した場合、以下のいずれかの操作を行います：

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を以下のように設定します：

  - VM インスタンスをサービスアカウントにインパーソネートさせる場合：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

  - サービスアカウント（仮にメタサービスアカウントと呼ぶ）が別のサービスアカウント（仮にデータサービスアカウントと呼ぶ）をインパーソネートする場合：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

Google GCS 用の `StorageCredentialParams`：

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

StarRocks が Iceberg メタデータのキャッシュを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

v3.3.3 以降、StarRocks は [定期的なメタデータリフレッシュ戦略](#appendix-periodic-metadata-refresh-strategy) をサポートします。ほとんどの場合、`MetadataUpdateParams` を無視し、そのポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。

| **パラメータ**                                 | **デフォルト**           | **説明**                                              |
| :-------------------------------------------- | :-------------------- | :----------------------------------------------------------- |
| enable_iceberg_metadata_cache                 | true                  | Iceberg 関連のメタデータ（Table Cache、Partition Name Cache、Manifest 内の Data File Cache および Delete Data File Cache を含む）をキャッシュするかどうか。 |
| iceberg_manifest_cache_with_column_statistics | false                 | 列の統計情報をキャッシュするかどうか。                  |
| iceberg_manifest_cache_max_num                | 100000                | キャッシュできる Manifest ファイルの最大数。     |
| refresh_iceberg_manifest_min_length           | 2 * 1024 * 1024       | Data File Cache のリフレッシュをトリガーする最小の Manifest ファイルの長さ。 |

### 例

以下の例は、使用するメタストアのタイプに応じて、Iceberg クラスターからデータをクエリするための Iceberg catalog `iceberg_catalog_hms` または `iceberg_catalog_glue` を作成します。ストレージタイプに一致するタブを選択してください：

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

#### AWS S3

##### インスタンスプロファイルベースの資格情報を選択した場合

- Iceberg クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

##### 想定ロールベースの資格情報を選択した場合

- Iceberg クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

##### IAM ユーザーベースの資格情報を選択した場合

- Iceberg クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します：

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

- Amazon EMR Iceberg クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します：

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

ストレージとして HDFS を使用する場合、以下のようなコマンドを実行します：

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

MinIO を例として使用します。以下のようなコマンドを実行します：

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

- 共有キー認証方法を選択した場合、以下のようなコマンドを実行します：

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

- SAS トークン認証方法を選択した場合、以下のようなコマンドを実行します：

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

- マネージドサービスアイデンティティ認証方法を選択した場合、以下のようなコマンドを実行します：

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

- サービスプリンシパル認証方法を選択した場合、以下のようなコマンドを実行します：

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

- マネージドアイデンティティ認証方法を選択した場合、以下のようなコマンドを実行します：

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

- 共有キー認証方法を選択した場合、以下のようなコマンドを実行します：

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

- サービスプリンシパル認証方法を選択した場合、以下のようなコマンドを実行します：

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

- VM ベースの認証方法を選択した場合、以下のようなコマンドを実行します：

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

- サービスアカウントベースの認証方法を選択した場合、以下のようなコマンドを実行します：

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

- インパーソネーションベースの認証方法を選択した場合：

  - VM インスタンスをサービスアカウントにインパーソネートさせる場合、以下のようなコマンドを実行します：

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

  - サービスアカウントが別のサービスアカウントをインパーソネートする場合、以下のようなコマンドを実行します：

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

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します：

```SQL
SHOW CATALOGS;
```

external catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例は、`iceberg_catalog_glue` という名前の Iceberg catalog の作成ステートメントをクエリします：

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

---

### Iceberg Catalog とその中のデータベースに切り替える

Iceberg catalog とその中のデータベースに切り替えるには、以下の方法のいずれかを使用します：

- 現在のセッションで Iceberg catalog を指定するには、[SET CATALOG](../../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後、アクティブなデータベースを指定するには [USE](../../../sql-reference/sql-statements/Database/USE.md) を使用します：

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える：
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する：
  USE <db_name>
  ```

- 直接 [USE](../../../sql-reference/sql-statements/Database/USE.md) を使用して、Iceberg catalog とその中のデータベースに切り替えます：

  ```SQL
  USE <catalog_name>.<db_name>
  ```
 
---

### Iceberg catalog の削除

external catalog を削除するには、[DROP CATALOG](../../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例は、`iceberg_catalog_glue` という名前の Iceberg catalog を削除します：

```SQL
DROP Catalog iceberg_catalog_glue;
```

---

### Iceberg テーブルのスキーマを表示する

Iceberg テーブルのスキーマを表示するには、以下の構文のいずれかを使用します：

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

1. Iceberg クラスター内のデータベースを表示するには、[SHOW DATABASES](../../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

---

### Iceberg データベースの作成

StarRocks の internal catalog と同様に、Iceberg catalog に対する [CREATE DATABASE](../../../administration/user_privs/privilege_item.md#catalog) 権限を持っている場合、[CREATE DATABASE](../../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) ステートメントを使用して、その Iceberg catalog にデータベースを作成できます。この機能は v3.1 以降でサポートされています。

:::tip

[GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

:::

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、その catalog に Iceberg データベースを作成するために以下のステートメントを使用します：

```SQL
CREATE DATABASE <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

`location` パラメータを使用して、データベースを作成したいファイルパスを指定できます。HDFS とクラウドストレージの両方がサポートされています。`location` パラメータを指定しない場合、StarRocks は Iceberg catalog のデフォルトファイルパスにデータベースを作成します。

`prefix` は使用するストレージシステムに基づいて異なります：

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

#### AWS S3 またはその他の S3 互換ストレージ（例：MinIO）

`Prefix` 値: `s3`

---

### Iceberg データベースの削除

StarRocks の internal データベースと同様に、Iceberg データベースに対する [DROP](../../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、[DROP DATABASE](../../../sql-reference/sql-statements/Database/DROP_DATABASE.md) ステートメントを使用して、その Iceberg データベースを削除できます。この機能は v3.1 以降でサポートされています。空のデータベースのみを削除できます。

Iceberg データベースを削除すると、HDFS クラスターまたはクラウドストレージ上のデータベースのファイルパスはデータベースと共に削除されません。

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、その catalog に Iceberg データベースを削除するために以下のステートメントを使用します：

```SQL
DROP DATABASE <database_name>;
```

---

### Iceberg テーブルの作成

StarRocks の internal データベースと同様に、Iceberg データベースに対する [CREATE TABLE](../../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、[CREATE TABLE](../../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) または [CREATE TABLE AS SELECT ../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.mdELECT.md) ステートメントを使用して、その Iceberg データベースにテーブルを作成できます。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベースに Iceberg テーブルを作成するために以下の構文を使用します。

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

`column_definition` の構文は以下の通りです：

```SQL
col_name col_type [COMMENT 'comment']
```

:::note

すべての非パーティション列は `NULL` をデフォルト値として使用する必要があります。つまり、テーブル作成ステートメントで各非パーティション列に対して `DEFAULT "NULL"` を指定する必要があります。さらに、パーティション列は非パーティション列の後に定義され、デフォルト値として `NULL` を使用することはできません。

:::

##### partition_desc

`partition_desc` の構文は以下の通りです：

```SQL
PARTITION BY (par_col1[, par_col2...])
```

現在、StarRocks は [identity transforms](https://iceberg.apache.org/spec/#partitioning) のみをサポートしており、これは StarRocks が各ユニークなパーティション値に対してパーティションを作成することを意味します。

:::note

パーティション列は非パーティション列の後に定義される必要があります。パーティション列は FLOAT、DOUBLE、DECIMAL、および DATETIME を除くすべてのデータ型をサポートし、デフォルト値として `NULL` を使用することはできません。

:::

##### PROPERTIES

`PROPERTIES` で `"key" = "value"` 形式でテーブル属性を指定できます。[Iceberg テーブル属性](https://iceberg.apache.org/docs/latest/configuration/) を参照してください。

以下の表は、いくつかの主要なプロパティを説明しています。

###### location

説明: Iceberg テーブルを作成したいファイルパスです。メタストアとして HMS を使用する場合、`location` パラメータを指定する必要はありません。StarRocks は現在の Iceberg catalog のデフォルトファイルパスにテーブルを作成します。メタストアとして AWS Glue を使用する場合：

- テーブルを作成したいデータベースに対して `location` パラメータを指定した場合、テーブルに対して `location` パラメータを指定する必要はありません。このようにして、テーブルは所属するデータベースのファイルパスをデフォルトとします。
- テーブルを作成したいデータベースに対して `location` を指定していない場合、テーブルに対して `location` パラメータを指定する必要があります。

###### file_format

説明: Iceberg テーブルのファイル形式です。Parquet 形式のみがサポートされています。デフォルト値: `parquet`。

###### compression_codec

説明: Iceberg テーブルに使用される圧縮アルゴリズムです。サポートされている圧縮アルゴリズムは SNAPPY、GZIP、ZSTD、および LZ4 です。デフォルト値: `gzip`。このプロパティは v3.2.3 で非推奨となり、それ以降のバージョンでは Iceberg テーブルへのデータシンクに使用される圧縮アルゴリズムはセッション変数 [connector_sink_compression_codec](../../../sql-reference/System_variable.md#connector_sink_compression_codec) によって一元的に制御されます。

---

### 例

1. `unpartition_tbl` という名前の非パーティションテーブルを作成します。このテーブルは `id` と `score` の二つの列で構成されています：

   ```SQL
   CREATE TABLE unpartition_tbl
   (
       id int,
       score double
   );
   ```

2. `partition_tbl_1` という名前のパーティションテーブルを作成します。このテーブルは `action`、`id`、および `dt` の三つの列で構成されており、`id` と `dt` がパーティション列として定義されています：

   ```SQL
   CREATE TABLE partition_tbl_1
   (
       action varchar(20),
       id int,
       dt date
   )
   PARTITION BY (id,dt);
   ```

3. 既存のテーブル `partition_tbl_1` をクエリし、そのクエリ結果に基づいて `partition_tbl_2` という名前のパーティションテーブルを作成します。`partition_tbl_2` では、`id` と `dt` がパーティション列として定義されています：

   ```SQL
   CREATE TABLE partition_tbl_2
   PARTITION BY (id, dt)
   AS SELECT * from employee;
   ```

---

### Iceberg テーブルへのデータシンク

StarRocks の internal テーブルと同様に、Iceberg テーブルに対する [INSERT](../../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、[INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用して、StarRocks テーブルのデータをその Iceberg テーブルにシンクできます（現在は Parquet 形式の Iceberg テーブルのみがサポートされています）。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベースにある Parquet 形式の Iceberg テーブルに StarRocks テーブルのデータをシンクするために以下の構文を使用します。

#### 構文

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 指定されたパーティションにデータをシンクしたい場合、以下の構文を使用します：
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

Iceberg テーブルの既存のデータを StarRocks テーブルのデータで上書きします。

##### column_name

データをロードしたい宛先列の名前です。一つ以上の列を指定できます。複数の列を指定する場合、カンマ（`,`）で区切ります。Iceberg テーブルに実際に存在する列のみを指定でき、指定した宛先列は Iceberg テーブルのパーティション列を含む必要があります。指定した宛先列は、StarRocks テーブルの列と順番に一対一でマッピングされます。宛先列名が何であっても関係ありません。宛先列が指定されていない場合、データは Iceberg テーブルのすべての列にロードされます。StarRocks テーブルの非パーティション列が Iceberg テーブルのいずれの列にもマッピングできない場合、StarRocks は Iceberg テーブル列にデフォルト値 `NULL` を書き込みます。INSERT ステートメントに含まれるクエリステートメントの返された列タイプが宛先列のデータタイプと異なる場合、StarRocks は不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。

##### expression

宛先列に値を割り当てる式です。

##### DEFAULT

宛先列にデフォルト値を割り当てます。

##### query

Iceberg テーブルにロードされるクエリステートメントの結果です。StarRocks がサポートする任意の SQL ステートメントを使用できます。

##### PARTITION

データをロードしたいパーティションです。このプロパティには Iceberg テーブルのすべてのパーティション列を指定する必要があります。このプロパティで指定するパーティション列は、テーブル作成ステートメントで定義したパーティション列と異なる順序で指定できます。このプロパティを指定する場合、`column_name` プロパティを指定することはできません。

#### 例

1. `partition_tbl_1` テーブルに三つのデータ行を挿入します：

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. 簡単な計算を含む SELECT クエリの結果を `partition_tbl_1` テーブルに挿入します：

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. `partition_tbl_1` テーブルからデータを読み取る SELECT クエリの結果を同じテーブルに挿入します：

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. `partition_tbl_2` テーブルの二つの条件 `dt='2023-09-01'` および `id=1` を満たすパーティションに SELECT クエリの結果を挿入します：

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. `partition_tbl_1` テーブルの二つの条件 `dt='2023-09-01'` および `id=1` を満たすパーティションのすべての `action` 列の値を `close` に上書きします：

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

---

### Iceberg テーブルの削除

StarRocks の internal テーブルと同様に、Iceberg テーブルに対する [DROP](../../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、[DROP TABLE](../../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) ステートメントを使用して、その Iceberg テーブルを削除できます。この機能は v3.1 以降でサポートされています。

Iceberg テーブルを削除すると、HDFS クラスターまたはクラウドストレージ上のテーブルのファイルパスとデータはテーブルと共に削除されません。

Iceberg テーブルを強制的に削除する場合（つまり、DROP TABLE ステートメントで `FORCE` キーワードを指定した場合）、HDFS クラスターまたはクラウドストレージ上のテーブルのデータはテーブルと共に削除されますが、テーブルのファイルパスは保持されます。

[Iceberg catalog とその中のデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) し、そのデータベースに Iceberg テーブルを削除するために以下のステートメントを使用します。

```SQL
DROP TABLE <table_name> [FORCE];
```

---

### メタデータキャッシングの設定

Iceberg クラスターのメタデータファイルは、AWS S3 や HDFS などのリモートストレージに保存されている場合があります。デフォルトでは、StarRocks は Iceberg メタデータをメモリにキャッシュします。クエリを高速化するために、StarRocks はメモリとディスクの両方にメタデータをキャッシュできる二層のメタデータキャッシングメカニズムを採用しています。最初のクエリごとに、StarRocks はその計算結果をキャッシュします。以前のクエリと意味的に同等の後続のクエリが発行された場合、StarRocks は最初にキャッシュから要求されたメタデータを取得し、キャッシュでメタデータがヒットしない場合にのみリモートストレージからメタデータを取得します。

StarRocks は、Least Recently Used (LRU) アルゴリズムを使用してデータをキャッシュおよび削除します。基本的なルールは次のとおりです：

- StarRocks は最初にメモリから要求されたメタデータを取得しようとします。メモリでメタデータがヒットしない場合、StarRocks はディスクからメタデータを取得しようとします。ディスクから取得したメタデータはメモリにロードされます。ディスクでもメタデータがヒットしない場合、StarRocks はリモートストレージからメタデータを取得し、取得したメタデータをメモリにキャッシュします。
- StarRocks はメモリから削除されたメタデータをディスクに書き込みますが、ディスクから削除されたメタデータは直接破棄します。

v3.3.3 以降、StarRocks は [定期的なメタデータリフレッシュ戦略](#appendix-periodic-metadata-refresh-strategy) をサポートします。システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。

#### Iceberg メタデータキャッシングに関する FE 設定

##### enable_iceberg_metadata_disk_cache

- 単位: N/A
- デフォルト値: `false`
- 説明: ディスクキャッシュを有効にするかどうかを指定します。

##### iceberg_metadata_cache_disk_path

- 単位: N/A
- デフォルト値: `StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"`
- 説明: ディスク上のキャッシュされたメタデータファイルの保存パスです。

##### iceberg_metadata_disk_cache_capacity

- 単位: バイト
- デフォルト値: `2147483648`、2 GB に相当
- 説明: ディスク上にキャッシュできるメタデータの最大サイズです。

##### iceberg_metadata_memory_cache_capacity

- 単位: バイト
- デフォルト値: `536870912`、512 MB に相当
- 説明: メモリにキャッシュできるメタデータの最大サイズです。

##### iceberg_metadata_memory_cache_expiration_seconds

- 単位: 秒  
- デフォルト値: `86500`
- 説明: メモリ内のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間です。

##### iceberg_metadata_disk_cache_expiration_seconds

- 単位: 秒  
- デフォルト値: `604800`、1 週間に相当
- 説明: ディスク上のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間です。

##### iceberg_metadata_cache_max_entry_size

- 単位: バイト
- デフォルト値: `8388608`、8 MB に相当
- 説明: キャッシュできるファイルの最大サイズです。このパラメータの値を超えるサイズのファイルはキャッシュできません。クエリがこれらのファイルを要求する場合、StarRocks はリモートストレージからそれらを取得します。

##### enable_background_refresh_connector_metadata

- 単位: -
- デフォルト値: true
- 説明: Iceberg メタデータキャッシュの定期的なリフレッシュを有効にするかどうか。これが有効になると、StarRocks は Iceberg クラスターのメタストア（Hive Metastore または AWS Glue）をポーリングし、頻繁にアクセスされる Iceberg catalog のキャッシュされたメタデータをリフレッシュしてデータの変更を認識します。`true` は Iceberg メタデータキャッシュのリフレッシュを有効にし、`false` はそれを無効にします。

##### background_refresh_metadata_interval_millis

- 単位: ミリ秒
- デフォルト値: 600000
- 説明: 二つの連続した Iceberg メタデータキャッシュリフレッシュの間隔です。

##### background_refresh_metadata_time_secs_since_last_access_sec

- 単位: 秒
- デフォルト値: 86400
- 説明: Iceberg メタデータキャッシュリフレッシュタスクの有効期限です。アクセスされた Iceberg catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータのリフレッシュを停止します。アクセスされていない Iceberg catalog に対して、StarRocks はそのキャッシュされたメタデータをリフレッシュしません。

## 付録: 定期的なメタデータリフレッシュ戦略

- **大量のメタデータに対する分散プラン**

  大量のメタデータを効果的に処理するために、StarRocks は複数の BE および CN ノードを使用した分散アプローチを採用しています。この方法は、現代のクエリエンジンの並列計算能力を活用し、マニフェストファイルの読み取り、解凍、フィルタリングなどのタスクを複数のノードに分散させます。これらのマニフェストファイルを並行して処理することで、メタデータの取得にかかる時間が大幅に短縮され、ジョブの計画が迅速になります。これは、特に多数のマニフェストファイルを含む大規模なクエリに対して有益であり、単一のボトルネックを排除し、全体的なクエリ実行効率を向上させます。

- **少量のメタデータに対するローカルプラン**

  小規模なクエリでは、マニフェストファイルの繰り返し解凍と解析が不要な遅延を引き起こす可能性があります。この問題に対処するために、StarRocks はデシリアライズされたメモリオブジェクト、特に Avro ファイルをキャッシュします。これらのデシリアライズされたファイルをメモリに保存することで、後続のクエリに対して解凍と解析の段階をスキップできます。このキャッシングメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ需要やマテリアライズドビューの書き換えニーズに対応しやすくなります。

- **適応型メタデータ取得戦略**（デフォルト）

  StarRocks は、FE および BE/CN ノードの数、CPU コア数、現在のクエリに必要なマニフェストファイルの数など、さまざまな要因に基づいて適切なメタデータ取得方法を自動的に選択するように設計されています。この適応型アプローチにより、システムはメタデータ関連のパラメータを手動で調整することなく、動的にメタデータ取得を最適化します。これにより、StarRocks はシームレスな体験を提供し、分散プランとローカルプランのバランスを取り、異なる条件下で最適なクエリパフォーマンスを実現します。

システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。
