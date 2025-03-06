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

Iceberg catalog は、StarRocks が v2.4 以降でサポートする外部 catalog の一種です。Iceberg catalog を使用すると、以下のことが可能です。

- Iceberg に保存されたデータを直接クエリし、手動でテーブルを作成する必要がありません。
- [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) または非同期マテリアライズドビュー（v2.5 以降でサポート）を使用して、Iceberg に保存されたデータを処理し、StarRocks にデータをロードします。
- StarRocks 上で Iceberg データベースやテーブルを作成または削除したり、StarRocks テーブルから Parquet 形式の Iceberg テーブルにデータをシンクする操作を [INSERT INTO](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して行います（この機能は v3.1 以降でサポートされています）。

Iceberg クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム（例：MinIO）などのオブジェクトストレージ

- Hive metastore、AWS Glue、または Tabular などのメタストア

:::note

- ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。
- メタストアとして Tabular を選択した場合、Iceberg REST catalog を使用する必要があります。

:::

## 使用上の注意

StarRocks を使用して Iceberg からデータをクエリする際には、以下の点に注意してください。

| **ファイル形式** | **圧縮形式**                                   | **Iceberg テーブルバージョン**                                           |
| --------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| Parquet         | SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION      | <ul><li>v1 テーブル: サポートされています。 </li><li>v2 テーブル: StarRocks v3.1 以降でサポートされており、これらの v2 テーブルに対するクエリは位置削除をサポートしています。v3.1.10、v3.2.5、v3.3 およびそれ以降のバージョンでは、v2 テーブルに対するクエリは等価削除もサポートしています。 </li></ul> |
| ORC             | ZLIB, SNAPPY, LZO, LZ4, ZSTD, NO_COMPRESSION | <ul><li>v1 テーブル: サポートされています。 </li><li>v2 テーブル: StarRocks v3.0 以降でサポートされており、これらの v2 テーブルに対するクエリは位置削除をサポートしています。v3.1.8、v3.2.3、v3.3 およびそれ以降のバージョンでは、v2 テーブルに対するクエリは等価削除もサポートしています。 </li></ul> |

## 統合準備

Iceberg catalog を作成する前に、StarRocks クラスターが Iceberg クラスターのストレージシステムとメタストアと統合できることを確認してください。

---

### ストレージ

ストレージタイプに合ったタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

Iceberg クラスターが AWS S3 をストレージとして使用する場合、または AWS Glue をメタストアとして使用する場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。

- インスタンスプロファイル
- 想定ロール
- IAM ユーザー

上記の3つの認証方法の中で、インスタンスプロファイルが最も広く使用されています。

詳細については、[AWS IAM での認証準備](../../../integrations/authenticate_to_aws_resources.md#preparations)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

HDFS をストレージとして選択した場合、StarRocks クラスターを次のように設定します。

- （オプション）HDFS クラスターおよび Hive metastore にアクセスするためのユーザー名を設定します。デフォルトでは、StarRocks は FE および BE または CN プロセスのユーザー名を使用して HDFS クラスターおよび Hive metastore にアクセスします。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭、および各 BE の **be/conf/hadoop_env.sh** ファイルまたは各 CN の **cn/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに1つのユーザー名しか設定できません。
- Iceberg データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを設定する必要があります。

  - HDFS クラスターに高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。
  - HDFS クラスターに View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE の **$BE_HOME/conf** パスまたは各 CN の **$CN_HOME/conf** パスに追加します。

:::tip

クエリを送信した際に不明なホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

:::

---

#### Kerberos 認証

HDFS クラスターまたは Hive metastore に Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive metastore へのアクセス権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
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

Iceberg catalog の名前。命名規則は次の通りです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### comment

Iceberg catalog の説明。このパラメータはオプションです。

#### type

データソースのタイプ。値を `iceberg` に設定します。

#### MetastoreParams

データソースのメタストアと StarRocks がどのように統合するかに関する一連のパラメータ。メタストアタイプに合ったタブを選択してください。

<Tabs groupId="metastore">
<TabItem value="HIVE" label="Hive metastore" default>

##### Hive metastore

データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note

Iceberg データをクエリする前に、Hive metastore ノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始した際に StarRocks が Hive metastore にアクセスできない可能性があります。

:::

次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

##### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `hive` に設定します。

##### hive.metastore.uris

必須: はい
説明: Hive metastore の URI。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive metastore に高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。

</TabItem>
<TabItem value="GLUE" label="AWS Glue">

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合（AWS S3 をストレージとして選択した場合のみサポート）、次のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。

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

AWS Glue 用の `MetastoreParams`:

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `glue` に設定します。

###### aws.glue.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` および `false`。デフォルト値: `false`。

###### aws.glue.iam_role_arn

必須: いいえ
説明: AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。想定ロールベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.region

必須: はい
説明: AWS Glue Data Catalog が存在するリージョン。例: `us-west-1`。

###### aws.glue.access_key

必須: いいえ
説明: AWS IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

###### aws.glue.secret_key

必須: いいえ
説明: AWS IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS Glue にアクセスする場合、このパラメータを指定する必要があります。

AWS Glue へのアクセス認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセス認証パラメータ](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)を参照してください。

</TabItem>
<TabItem value="REST" label="REST">

##### REST

メタストアとして REST を使用する場合、メタストアのタイプを REST (`"iceberg.catalog.type" = "rest"`) として指定する必要があります。`MetastoreParams` を次のように設定します。

```SQL
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri" = "<rest_server_api_endpoint>",
"iceberg.catalog.security" = "oauth2",
"iceberg.catalog.oauth2.credential" = "<credential>",
"iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>"
```

REST catalog 用の `MetastoreParams`:

###### iceberg.catalog.type

必須: はい
説明: Iceberg クラスターで使用するメタストアのタイプ。値を `rest` に設定します。

###### iceberg.catalog.uri

必須: はい
説明: REST サービスエンドポイントの URI。例: `https://api.tabular.io/ws`。

###### iceberg.catalog.security

必須: いいえ

説明: 使用する認証プロトコルのタイプ。デフォルト: `NONE`。有効な値: `OAUTH2`。`OAUTH2` 認証プロトコルには `token` または `credential` が必要です。

###### iceberg.catalog.oauth2.token

必須: いいえ

説明: サーバーとのやり取りに使用されるベアラートークン。`OAUTH2` 認証プロトコルには `token` または `credential` が必要です。例: `AbCdEf123456`。

###### iceberg.catalog.oauth2.credential

必須: いいえ

説明: サーバーとの OAuth2 クライアント資格情報フローでトークンと交換するための資格情報。`OAUTH2` 認証プロトコルには `token` または `credential` が必要です。例: `AbCdEf123456`。

###### iceberg.catalog.oauth2.scope

必須: いいえ

説明: REST Catalog と通信する際に使用するスコープ。`credential` を使用する場合にのみ適用されます。

###### iceberg.catalog.oauth2.server-uri

必須: いいえ

説明: OAuth2 サーバーからアクセストークンを取得するためのエンドポイント。

###### iceberg.catalog.vended-credentials-enabled

必須: いいえ

説明: ファイルシステムアクセスのために REST バックエンドによって提供される資格情報を使用するかどうか。デフォルト: `true`。

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
次の例は、メタストアとして Polaris を使用する Iceberg catalog `smith_polaris` を作成します。

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

<TabItem value="JDBC" label="JDBC">

##### JDBC

データソースのメタストアとして JDBC を選択した場合は、`MetastoreParams` を以下のように設定する：

```SQL
"iceberg.catalog.type" = "jdbc",
"iceberg.catalog.uri" = "<jdbc_uri>",
"iceberg.catalog.warehouse" = "<warehouse_location>"
```

以下に `MetastoreParams` で設定する必要のあるパラメータを示す。

##### iceberg.catalog.type

必須：はい

説明：Icebergクラスタで使用するメタストアのタイプ。値を `jdbc` に設定します。

##### iceberg.catalog.uri

必須：はい

説明：データベースのURI。フォーマット：`jdbc:[mysql\|postgresql]://<DB_IP_address>:<DB_PORT>/<DB_NAME>`。


###### iceberg.catalog.warehouse

必須：はい

説明：Iceberg カタログの Warehouse の場所または識別子。例: `s3://my_bucket/warehouse_location`。


##### iceberg.catalog.jdbc.user

必須：いいえ

説明：データベースのユーザー名。


##### iceberg.catalog.jdbc.password

必須：いいえ

説明：データベースのパスワード。

次の例では、メタストアとして `iceberg_jdbc` という名前の Iceberg カタログを作成します：

```SQL
CREATE EXTERNAL CATALOG iceberg_jdbc
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "jdbc",
    "iceberg.catalog.warehouse" = "hdfs:///jdbc_iceberg/warehouse/ ",
    "iceberg.catalog.uri" = "jdbc:mysql://ip:port/db_name",
    "iceberg.catalog.jdbc.user" = "username",
    "iceberg.catalog.jdbc.password" = "password"
);
```

</TabItem>

</Tabs>

---

#### `StorageCredentialParams`

StarRocks がストレージシステムとどのように統合するかに関する一連のパラメータ。このパラメータセットはオプションです。

次の点に注意してください。

- ストレージとして HDFS を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

- メタストアとして Tabular を使用する場合、`StorageCredentialParams` を設定する必要はなく、このセクションをスキップできます。メタストアとして HMS または AWS Glue を使用する場合、`StorageCredentialParams` を設定する必要があります。

ストレージタイプに合ったタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

##### AWS S3

Iceberg クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 想定ロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

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

AWS S3 用の `StorageCredentialParams`:

###### aws.s3.use_instance_profile

必須: はい
説明: インスタンスプロファイルベースの認証方法と想定ロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` および `false`。デフォルト値: `false`。

###### aws.s3.iam_role_arn

必須: いいえ
説明: AWS S3 バケットに対する権限を持つ IAM ロールの ARN。想定ロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.region

必須: はい
説明: AWS S3 バケットが存在するリージョン。例: `us-west-1`。

###### aws.s3.access_key

必須: いいえ
説明: IAM ユーザーのアクセスキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

###### aws.s3.secret_key

必須: いいえ
説明: IAM ユーザーのシークレットキー。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。

AWS S3 へのアクセス認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセス認証パラメータ](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

</TabItem>

<TabItem value="HDFS" label="HDFS" >

HDFS ストレージを使用する場合、ストレージ資格情報をスキップします。

</TabItem>

<TabItem value="MINIO" label="MinIO" >

##### S3 互換ストレージシステム

Iceberg catalog は v2.5 以降で S3 互換ストレージシステムをサポートしています。

S3 互換ストレージシステム（例: MinIO）を Iceberg クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように設定して、統合を成功させます。

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

MinIO およびその他の S3 互換システム用の `StorageCredentialParams`:

###### aws.s3.enable_ssl

必須: はい
説明: SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `true`。

###### aws.s3.enable_path_style_access

必須: はい
説明: パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイルの URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例: US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスする場合、次のパススタイルの URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。

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

Iceberg catalog は v3.0 以降で Microsoft Azure Storage をサポートしています。

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

Microsoft Azure 用の `StorageCredentialParams`:

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

Iceberg catalog は v3.0 以降で Google GCS をサポートしています。

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

  - サービスアカウント（仮にメタサービスアカウントと呼ぶ）を別のサービスアカウント（仮にデータサービスアカウントと呼ぶ）にインパーソネートさせる場合:

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

Google GCS 用の `StorageCredentialParams`:

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

Iceberg メタデータのキャッシュを StarRocks がどのように更新するかに関する一連のパラメータ。このパラメータセットはオプションです。

v3.3.3 以降、StarRocks は[定期的なメタデータ更新戦略](#appendix-periodic-metadata-refresh-strategy)をサポートしています。ほとんどの場合、`MetadataUpdateParams` を無視し、その中のポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使えるパフォーマンスを提供します。システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。

| **パラメータ**                                 | **デフォルト**           | **説明**                                              |
| :-------------------------------------------- | :-------------------- | :----------------------------------------------------------- |
| enable_iceberg_metadata_cache                 | true                  | Table Cache、Partition Name Cache、および Manifest 内の Data File Cache と Delete Data File Cache を含む Iceberg 関連のメタデータをキャッシュするかどうか。 |
| iceberg_manifest_cache_with_column_statistics | false                 | 列の統計情報をキャッシュするかどうか。                  |
| iceberg_manifest_cache_max_num                | 100000                | キャッシュできる Manifest ファイルの最大数。     |
| refresh_iceberg_manifest_min_length           | 2 * 1024 * 1024       | Data File Cache の更新をトリガーする最小の Manifest ファイルの長さ。 |

### 例

次の例は、使用するメタストアのタイプに応じて、Iceberg クラスターからデータをクエリするための Iceberg catalog `iceberg_catalog_hms` または `iceberg_catalog_glue` を作成します。ストレージタイプに合ったタブを選択してください。

<Tabs groupId="storage">
<TabItem value="AWS" label="AWS S3" default>

#### AWS S3

##### インスタンスプロファイルベースの資格情報を選択した場合

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

##### 想定ロールベースの資格情報を選択した場合

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

##### IAM ユーザーベースの資格情報を選択した場合

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

MinIO を例にとります。次のようなコマンドを実行します。

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

  - サービスアカウントを別のサービスアカウントにインパーソネートさせる場合、次のようなコマンドを実行します。

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

## catalog の使用

### Iceberg catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。次の例では、`iceberg_catalog_glue` という Iceberg catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

---

### Iceberg Catalog とそのデータベースに切り替える

Iceberg catalog とそのデータベースに切り替えるには、次のいずれかの方法を使用します。

- 現在のセッションで Iceberg catalog を指定するには [SET CATALOG](../../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、アクティブなデータベースを指定するには [USE](../../../sql-reference/sql-statements/Database/USE.md) を使用します。

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

- 直接 [USE](../../../sql-reference/sql-statements/Database/USE.md) を使用して Iceberg catalog とそのデータベースに切り替えます。

  ```SQL
  USE <catalog_name>.<db_name>
  ```
 
---

### Iceberg catalog の削除

外部 catalog を削除するには、[DROP CATALOG](../../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

次の例では、`iceberg_catalog_glue` という Iceberg catalog を削除します。

```SQL
DROP Catalog iceberg_catalog_glue;
```

---

### Iceberg テーブルのスキーマを表示する

Iceberg テーブルのスキーマを表示するには、次のいずれかの構文を使用します。

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

1. Iceberg クラスター内のデータベースを表示するには、[SHOW DATABASES](../../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Iceberg catalog とそのデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it)。

3. 指定されたデータベース内の宛先テーブルをクエリするには、[SELECT](../../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

---

### Iceberg データベースの作成

StarRocks の内部 catalog と同様に、Iceberg catalog に対して [CREATE DATABASE](../../../administration/user_privs/privilege_item.md#catalog) 権限を持っている場合、その Iceberg catalog 内でデータベースを作成するために [CREATE DATABASE](../../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

:::tip

[GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) および [REVOKE](../../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して権限を付与および取り消すことができます。

:::

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) してから、次のステートメントを使用してその catalog 内で Iceberg データベースを作成します。

```SQL
CREATE DATABASE <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>/")]
```

`location` パラメータを使用して、データベースを作成するファイルパスを指定できます。HDFS とクラウドストレージの両方がサポートされています。`location` パラメータを指定しない場合、StarRocks は Iceberg catalog のデフォルトファイルパスにデータベースを作成します。

使用するストレージシステムに基づいて `prefix` が異なります。

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

#### AWS S3 またはその他の S3 互換ストレージ（例: MinIO）

`Prefix` 値: `s3`

---

### Iceberg データベースの削除

StarRocks の内部データベースと同様に、Iceberg データベースに対して [DROP](../../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、その Iceberg データベースを削除するために [DROP DATABASE](../../../sql-reference/sql-statements/Database/DROP_DATABASE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。空のデータベースのみを削除できます。

Iceberg データベースを削除すると、HDFS クラスターまたはクラウドストレージ上のデータベースのファイルパスはデータベースと共に削除されません。

[Iceberg catalog に切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) してから、次のステートメントを使用してその catalog 内で Iceberg データベースを削除します。

```SQL
DROP DATABASE <database_name>;
```

---

### Iceberg テーブルの作成

StarRocks の内部データベースと同様に、Iceberg データベースに対して [CREATE TABLE](../../../administration/user_privs/privilege_item.md#database) 権限を持っている場合、その Iceberg データベース内でテーブルを作成するために [CREATE TABLE](../../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) または [CREATE TABLE AS SELECT ../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.mdELECT.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とそのデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) してから、そのデータベース内で Iceberg テーブルを作成するための次の構文を使用します。

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

`column_definition` の構文は次の通りです。

```SQL
col_name col_type [COMMENT 'comment']
```

:::note

すべての非パーティション列は `NULL` をデフォルト値として使用する必要があります。つまり、テーブル作成ステートメントで各非パーティション列に対して `DEFAULT "NULL"` を指定する必要があります。さらに、パーティション列は非パーティション列の後に定義され、デフォルト値として `NULL` を使用することはできません。

:::

##### partition_desc

`partition_desc` の構文は次の通りです。

```SQL
PARTITION BY (par_col1[, par_col2...])
```

現在、StarRocks は [identity transforms](https://iceberg.apache.org/spec/#partitioning) のみをサポートしており、これは StarRocks が各ユニークなパーティション値に対してパーティションを作成することを意味します。

:::note

パーティション列は非パーティション列の後に定義される必要があります。パーティション列は FLOAT、DOUBLE、DECIMAL、および DATETIME を除くすべてのデータ型をサポートし、デフォルト値として `NULL` を使用することはできません。

:::

##### PROPERTIES

`PROPERTIES` で `"key" = "value"` 形式でテーブル属性を指定できます。[Iceberg テーブル属性](https://iceberg.apache.org/docs/latest/configuration/) を参照してください。

次の表は、いくつかの主要なプロパティを説明しています。

###### location

説明: Iceberg テーブルを作成するファイルパス。メタストアとして HMS を使用する場合、`location` パラメータを指定する必要はありません。StarRocks は現在の Iceberg catalog のデフォルトファイルパスにテーブルを作成します。AWS Glue をメタストアとして使用する場合:

- テーブルを作成するデータベースに対して `location` パラメータを指定している場合、テーブルに対して `location` パラメータを指定する必要はありません。その場合、テーブルは所属するデータベースのファイルパスにデフォルトで設定されます。
- テーブルを作成するデータベースに対して `location` を指定していない場合、テーブルに対して `location` パラメータを指定する必要があります。

###### file_format

説明: Iceberg テーブルのファイル形式。Parquet 形式のみがサポートされています。デフォルト値: `parquet`。

###### compression_codec

説明: Iceberg テーブルに使用される圧縮アルゴリズム。サポートされている圧縮アルゴリズムは SNAPPY、GZIP、ZSTD、および LZ4 です。デフォルト値: `gzip`。このプロパティは v3.2.3 で非推奨となり、それ以降のバージョンでは Iceberg テーブルへのデータシンクに使用される圧縮アルゴリズムはセッション変数 [connector_sink_compression_codec](../../../sql-reference/System_variable.md#connector_sink_compression_codec) によって一元的に制御されます。

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

2. `partition_tbl_1` という名前のパーティションテーブルを作成します。このテーブルは `action`、`id`、および `dt` の3つの列で構成されており、`id` と `dt` がパーティション列として定義されています。

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

### Iceberg テーブルへのデータシンク

StarRocks の内部テーブルと同様に、Iceberg テーブルに対して [INSERT](../../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、StarRocks テーブルのデータをその Iceberg テーブルにシンクするために [INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用できます（現在は Parquet 形式の Iceberg テーブルのみがサポートされています）。この機能は v3.1 以降でサポートされています。

[Iceberg catalog とそのデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) してから、次の構文を使用して StarRocks テーブルのデータをそのデータベース内の Parquet 形式の Iceberg テーブルにシンクします。

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

StarRocks テーブルのデータで Iceberg テーブルの既存データを上書きします。

##### column_name

データをロードしたい宛先列の名前。1つ以上の列を指定できます。複数の列を指定する場合、カンマ (`,`) で区切ります。実際に Iceberg テーブルに存在する列のみを指定でき、指定する宛先列には Iceberg テーブルのパーティション列を含める必要があります。指定する宛先列は、StarRocks テーブルの列と順番に1対1でマッピングされ、宛先列名が何であっても関係ありません。宛先列が指定されていない場合、データは Iceberg テーブルのすべての列にロードされます。StarRocks テーブルの非パーティション列が Iceberg テーブルの列にマッピングできない場合、StarRocks は Iceberg テーブル列にデフォルト値 `NULL` を書き込みます。INSERT ステートメントに返される列タイプが宛先列のデータ型と異なるクエリステートメントが含まれている場合、StarRocks は不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。

##### expression

宛先列に値を割り当てる式。

##### DEFAULT

宛先列にデフォルト値を割り当てます。

##### query

Iceberg テーブルにロードされるクエリステートメントの結果。StarRocks がサポートする任意の SQL ステートメントを使用できます。

##### PARTITION

データをロードしたいパーティション。Iceberg テーブルのすべてのパーティション列をこのプロパティで指定する必要があります。このプロパティで指定するパーティション列は、テーブル作成ステートメントで定義したパーティション列と異なる順序で指定できます。このプロパティを指定する場合、`column_name` プロパティを指定することはできません。

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

4. `partition_tbl_2` テーブルの `dt='2023-09-01'` および `id=1` の2つの条件を満たすパーティションに SELECT クエリの結果を挿入します。

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. `partition_tbl_1` テーブルの `dt='2023-09-01'` および `id=1` の2つの条件を満たすパーティションのすべての `action` 列の値を `close` で上書きします。

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

---

### Iceberg テーブルの削除

StarRocks の内部テーブルと同様に、Iceberg テーブルに対して [DROP](../../../administration/user_privs/privilege_item.md#table) 権限を持っている場合、その Iceberg テーブルを削除するために [DROP TABLE](../../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) ステートメントを使用できます。この機能は v3.1 以降でサポートされています。

Iceberg テーブルを削除すると、HDFS クラスターまたはクラウドストレージ上のテーブルのファイルパスとデータはテーブルと共に削除されません。

Iceberg テーブルを強制的に削除する場合（つまり、DROP TABLE ステートメントで `FORCE` キーワードを指定した場合）、HDFS クラスターまたはクラウドストレージ上のテーブルのデータはテーブルと共に削除されますが、テーブルのファイルパスは保持されます。

[Iceberg catalog とそのデータベースに切り替える](#switch-to-an-iceberg-catalog-and-a-database-in-it) してから、そのデータベース内で Iceberg テーブルを削除するための次のステートメントを使用します。

```SQL
DROP TABLE <table_name> [FORCE];
```

---

### メタデータキャッシュの設定

Iceberg クラスターのメタデータファイルは、AWS S3 や HDFS などのリモートストレージに保存されている場合があります。デフォルトでは、StarRocks は Iceberg メタデータをメモリにキャッシュします。クエリを高速化するために、StarRocks はメモリとディスクの両方にメタデータをキャッシュできる2レベルのメタデータキャッシングメカニズムを採用しています。各初回クエリに対して、StarRocks はその計算結果をキャッシュします。以前のクエリと意味的に同等の後続のクエリが発行された場合、StarRocks は最初にキャッシュから要求されたメタデータを取得し、キャッシュでメタデータがヒットしない場合にのみリモートストレージからメタデータを取得します。

StarRocks は、最も最近使用されたものを優先してキャッシュおよびデータを削除するアルゴリズム（LRU）を使用します。基本的なルールは次の通りです。

- StarRocks は最初にメモリから要求されたメタデータを取得しようとします。メモリでメタデータがヒットしない場合、StarRocks はディスクからメタデータを取得しようとします。ディスクから取得したメタデータはメモリにロードされます。ディスクでもメタデータがヒットしない場合、StarRocks はリモートストレージからメタデータを取得し、取得したメタデータをメモリにキャッシュします。
- StarRocks はメモリから削除されたメタデータをディスクに書き込みますが、ディスクから削除されたメタデータは直接破棄します。

v3.3.3 以降、StarRocks は[定期的なメタデータ更新戦略](#appendix-periodic-metadata-refresh-strategy)をサポートしています。システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。

#### Iceberg メタデータキャッシングに関する FE 設定

##### enable_iceberg_metadata_disk_cache

- 単位: N/A
- デフォルト値: `false`
- 説明: ディスクキャッシュを有効にするかどうかを指定します。

##### iceberg_metadata_cache_disk_path

- 単位: N/A
- デフォルト値: `StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"`
- 説明: ディスク上のキャッシュされたメタデータファイルの保存パス。

##### iceberg_metadata_disk_cache_capacity

- 単位: バイト
- デフォルト値: `2147483648`、2 GB に相当
- 説明: ディスク上にキャッシュできるメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_capacity

- 単位: バイト
- デフォルト値: `536870912`、512 MB に相当
- 説明: メモリにキャッシュできるメタデータの最大サイズ。

##### iceberg_metadata_memory_cache_expiration_seconds

- 単位: 秒  
- デフォルト値: `86500`
- 説明: メモリ内のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_disk_cache_expiration_seconds

- 単位: 秒  
- デフォルト値: `604800`、1 週間に相当
- 説明: ディスク上のキャッシュエントリが最後にアクセスされてから期限切れになるまでの時間。

##### iceberg_metadata_cache_max_entry_size

- 単位: バイト
- デフォルト値: `8388608`、8 MB に相当
- 説明: キャッシュ可能なファイルの最大サイズ。このパラメータの値を超えるサイズのファイルはキャッシュできません。クエリがこれらのファイルを要求する場合、StarRocks はリモートストレージからそれらを取得します。

##### enable_background_refresh_connector_metadata

- 単位: -
- デフォルト値: true
- 説明: 定期的な Iceberg メタデータキャッシュの更新を有効にするかどうか。これを有効にすると、StarRocks は Iceberg クラスターのメタストア（Hive Metastore または AWS Glue）をポーリングし、頻繁にアクセスされる Iceberg catalog のキャッシュされたメタデータを更新してデータの変更を認識します。`true` は Iceberg メタデータキャッシュの更新を有効にし、`false` は無効にします。

##### background_refresh_metadata_interval_millis

- 単位: ミリ秒
- デフォルト値: 600000
- 説明: 2 回の連続した Iceberg メタデータキャッシュ更新の間隔。- 単位: ミリ秒。

##### background_refresh_metadata_time_secs_since_last_access_sec

- 単位: 秒
- デフォルト値: 86400
- 説明: Iceberg メタデータキャッシュ更新タスクの有効期限。アクセスされた Iceberg catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Iceberg catalog に対して、StarRocks はそのキャッシュされたメタデータを更新しません。

## 付録: 定期的なメタデータ更新戦略

- **大量のメタデータに対する分散プラン**

  大量のメタデータを効果的に処理するために、StarRocks は複数の BE および CN ノードを使用した分散アプローチを採用しています。この方法は、現代のクエリエンジンの並列計算能力を活用し、マニフェストファイルの読み取り、解凍、フィルタリングなどのタスクを複数のノードに分散します。これにより、これらのマニフェストファイルを並行して処理することで、メタデータの取得にかかる時間が大幅に短縮され、ジョブの計画が迅速になります。特に多数のマニフェストファイルを含む大規模なクエリに対しては、単一ポイントのボトルネックを排除し、全体的なクエリ実行効率を向上させます。

- **少量のメタデータに対するローカルプラン**

  マニフェストファイルの繰り返しの解凍と解析が不要な遅延を引き起こす小規模なクエリに対しては、異なる戦略が採用されます。StarRocks は、特に Avro ファイルをメモリにキャッシュすることで、この問題に対処します。これらのデシリアライズされたファイルをメモリに保存することで、後続のクエリに対して解凍と解析の段階をスキップできます。このキャッシングメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ要求やマテリアライズドビューの書き換えニーズに対応しやすくなります。

- **適応型メタデータ取得戦略**（デフォルト）

  StarRocks は、FE および BE/CN ノードの数、CPU コア数、現在のクエリに必要なマニフェストファイルの数など、さまざまな要因に基づいて適切なメタデータ取得方法を自動的に選択するように設計されています。この適応型アプローチにより、メタデータ関連のパラメータを手動で調整する必要なく、システムは動的にメタデータ取得を最適化します。これにより、StarRocks はシームレスなエクスペリエンスを提供し、さまざまな条件下で最適なクエリパフォーマンスを達成するために分散プランとローカルプランのバランスを取ります。

システム変数 [`plan_mode`](../../../sql-reference/System_variable.md#plan_mode) を使用して Iceberg メタデータキャッシングプランを調整できます。
