---
description: Delta Lake から直接データをクエリする
displayed_sidebar: docs
---
import Intro from '../../_assets/catalog/_deltalake_intro.mdx'
import DatabricksParams from '../../_assets/catalog/_databricks_params.mdx'

# Delta Lake catalog

<<<<<<< HEAD
Delta Lake catalog は、Delta Lake からデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、Delta Lake catalog を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、Delta Lake から直接データを変換してロードすることもできます。StarRocks は v2.5 以降で Delta Lake catalog をサポートしています。

Delta Lake クラスターで SQL ワークロードを成功させるためには、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアにアクセスできる必要があります。StarRocks は以下のストレージシステムとメタストアをサポートしています。

- 分散ファイルシステム (HDFS) または AWS S3、Microsoft Azure Storage、Google GCS、その他の S3 互換ストレージシステム (例: MinIO) のようなオブジェクトストレージ

- Hive metastore や AWS Glue のようなメタストア

  > **NOTE**
  >
  > ストレージとして AWS S3 を選択した場合、メタストアとして HMS または AWS Glue を使用できます。他のストレージシステムを選択した場合、メタストアとして HMS のみを使用できます。

## 使用上の注意

- StarRocks がサポートする Delta Lake のファイル形式は Parquet です。Parquet ファイルは、SNAPPY、LZ4、ZSTD、GZIP、NO_COMPRESSION の圧縮形式をサポートしています。
=======
<Intro />

## 使用上の注意

- StarRocks がサポートする Delta Lake のファイル形式は Parquet です。Parquet ファイルは次の圧縮形式をサポートしています: SNAPPY, LZ4, ZSTD, GZIP, NO_COMPRESSION。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))
- StarRocks がサポートしていない Delta Lake のデータ型は MAP と STRUCT です。

## 統合準備

Delta Lake catalog を作成する前に、StarRocks クラスターが Delta Lake クラスターのストレージシステムとメタストアと統合できることを確認してください。

### AWS IAM

<<<<<<< HEAD
Delta Lake クラスターが AWS S3 をストレージとして使用している場合、または AWS Glue をメタストアとして使用している場合、適切な認証方法を選択し、StarRocks クラスターが関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです。
=======
Delta Lake クラスターが AWS S3 をストレージとして使用している場合、または AWS Glue をメタストアとして使用している場合、適切な認証方法を選択し、関連する AWS クラウドリソースにアクセスできるように必要な準備を行ってください。

推奨される認証方法は以下の通りです:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

- インスタンスプロファイル
- アサインされたロール
- IAM ユーザー

上記の認証方法の中で、インスタンスプロファイルが最も広く使用されています。

<<<<<<< HEAD
詳細については、[AWS IAM での認証準備](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication) を参照してください。

### HDFS

ストレージとして HDFS を選択した場合、StarRocks クラスターを次のように設定します。

- (オプション) HDFS クラスターおよび Hive metastore にアクセスするために使用されるユーザー名を設定します。デフォルトでは、StarRocks は HDFS クラスターおよび Hive metastore にアクセスするために FE および BE または CN プロセスのユーザー名を使用します。また、各 FE の **fe/conf/hadoop_env.sh** ファイルの先頭、および各 BE または各 CN の **be/conf/hadoop_env.sh** ファイルの先頭に `export HADOOP_USER_NAME="<user_name>"` を追加することでユーザー名を設定することもできます。これらのファイルでユーザー名を設定した後、各 FE および各 BE または CN を再起動してパラメータ設定を有効にします。StarRocks クラスターごとに 1 つのユーザー名のみを設定できます。
- Delta Lake データをクエリする際、StarRocks クラスターの FEs および BEs または CNs は HDFS クライアントを使用して HDFS クラスターにアクセスします。ほとんどの場合、その目的を達成するために StarRocks クラスターを設定する必要はなく、StarRocks はデフォルトの設定を使用して HDFS クライアントを起動します。次の状況でのみ StarRocks クラスターを設定する必要があります。

  - HDFS クラスターで高可用性 (HA) が有効になっている場合: HDFS クラスターの **hdfs-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE または各 CN の **$BE_HOME/conf** パスに追加します。
  - HDFS クラスターで View File System (ViewFs) が有効になっている場合: HDFS クラスターの **core-site.xml** ファイルを各 FE の **$FE_HOME/conf** パス、および各 BE または各 CN の **$BE_HOME/conf** パスに追加します。

> **NOTE**
>
> クエリを送信した際に未知のホストを示すエラーが返された場合、HDFS クラスターのノードのホスト名と IP アドレスのマッピングを **/etc/hosts** パスに追加する必要があります。

### Kerberos 認証

HDFS クラスターまたは Hive metastore で Kerberos 認証が有効になっている場合、StarRocks クラスターを次のように設定します。

- 各 FE および各 BE または CN で `kinit -kt keytab_path principal` コマンドを実行して、Key Distribution Center (KDC) から Ticket Granting Ticket (TGT) を取得します。このコマンドを実行するには、HDFS クラスターおよび Hive metastore にアクセスする権限が必要です。このコマンドを使用して KDC にアクセスすることは時間に敏感であるため、cron を使用してこのコマンドを定期的に実行する必要があります。
- 各 FE の **$FE_HOME/conf/fe.conf** ファイル、および各 BE または各 CN の **$BE_HOME/conf/be.conf** ファイルに `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。この例では、`/etc/krb5.conf` は **krb5.conf** ファイルの保存パスです。必要に応じてパスを変更できます。
=======
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
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
Delta Lake catalog の名前です。命名規則は以下の通りです。
=======
Delta Lake catalog の名前です。命名規則は次の通りです:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字と小文字を区別し、1023 文字を超えることはできません。

#### comment

Delta Lake catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `deltalake` に設定します。

#### MetastoreParams

StarRocks がデータソースのメタストアと統合する方法に関する一連のパラメータです。

##### Hive metastore

<<<<<<< HEAD
データソースのメタストアとして Hive metastore を選択した場合、`MetastoreParams` を次のように設定します。
=======
データソースのメタストアとして Hive メタストアを選択した場合、`MetastoreParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

<<<<<<< HEAD
> **NOTE**
>
> Delta Lake データをクエリする前に、Hive metastore ノードのホスト名と IP アドレスのマッピングを `/etc/hosts` パスに追加する必要があります。そうしないと、クエリを開始する際に StarRocks が Hive metastore にアクセスできない可能性があります。

次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | Delta Lake クラスターに使用するメタストアのタイプです。値を `hive` に設定します。 |
| hive.metastore.uris | Yes      | Hive metastore の URI です。形式: `thrift://<metastore_IP_address>:<metastore_port>`。<br />Hive metastore で高可用性 (HA) が有効になっている場合、複数のメタストア URI を指定し、カンマ (`,`) で区切ることができます。例: `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

データソースのメタストアとして AWS Glue を選択した場合、これは AWS S3 をストレージとして選択した場合にのみサポートされます。次のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。
=======
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
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

<<<<<<< HEAD
- 想定ロールベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。
=======
- アサインされたロールベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

<<<<<<< HEAD
- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように設定します。
=======
- IAM ユーザーベースの認証方法を選択する場合、`MetastoreParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

<<<<<<< HEAD
次の表は、`MetastoreParams` で設定する必要があるパラメータを説明しています。

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | Delta Lake クラスターに使用するメタストアのタイプです。値を `glue` に設定します。 |
| aws.glue.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法および想定ロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | No       | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN です。AWS Glue にアクセスするために想定ロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.region               | Yes      | AWS Glue Data Catalog が存在するリージョンです。例: `us-west-1`。 |
| aws.glue.access_key           | No       | AWS IAM ユーザーのアクセスキーです。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | No       | AWS IAM ユーザーのシークレットキーです。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |

AWS Glue へのアクセスのための認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS Glue へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue) を参照してください。
=======
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
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

#### StorageCredentialParams

StarRocks がストレージシステムと統合する方法に関する一連のパラメータです。このパラメータセットはオプションです。

HDFS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要はありません。

AWS S3、その他の S3 互換ストレージシステム、Microsoft Azure Storage、または Google GCS をストレージとして使用する場合、`StorageCredentialParams` を設定する必要があります。

##### AWS S3

<<<<<<< HEAD
Delta Lake クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います。

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
Delta Lake クラスターのストレージとして AWS S3 を選択した場合、次のいずれかの操作を行います:

- インスタンスプロファイルベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

<<<<<<< HEAD
- 想定ロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- アサインされたロールベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

<<<<<<< HEAD
- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- IAM ユーザーベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

<<<<<<< HEAD
次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法および想定ロールベースの認証方法を有効にするかどうかを指定します。 有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。AWS S3 にアクセスするために想定ロールベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。AWS S3 にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。AWS S3 にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |

AWS S3 へのアクセスのための認証方法の選択方法および AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[AWS S3 へのアクセスのための認証パラメータ](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。
=======
次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | インスタンスプロファイルベースの認証方法とアサインされたロールベースの認証方法を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN です。アサインされたロールベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョンです。例: `us-west-1`。 |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキーです。IAM ユーザーベースの認証方法を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要があります。 |

AWS S3 にアクセスするための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの構成方法については、 [Authentication parameters for accessing AWS S3](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

##### S3 互換ストレージシステム

Delta Lake catalogs は v2.5 以降、S3 互換ストレージシステムをサポートしています。

<<<<<<< HEAD
S3 互換ストレージシステム (例: MinIO) を Delta Lake クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように設定して、統合を成功させます。
=======
S3 互換ストレージシステム (例: MinIO) を Delta Lake クラスターのストレージとして選択した場合、`StorageCredentialParams` を次のように構成して、統合を成功させます:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

<<<<<<< HEAD
次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `true`。 |
<<<<<<< HEAD
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (オレゴン) リージョンで `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
=======
| aws.s3.enable_path_style_access  | Yes      | パススタイルアクセスを有効にするかどうかを指定します。<br />有効な値: `true` と `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。<br />パススタイル URL は次の形式を使用します: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例えば、US West (Oregon) リージョンに `DOC-EXAMPLE-BUCKET1` というバケットを作成し、そのバケット内の `alice.jpg` オブジェクトにアクセスしたい場合、次のパススタイル URL を使用できます: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))
| aws.s3.endpoint                  | Yes      | AWS S3 の代わりに S3 互換ストレージシステムに接続するために使用されるエンドポイントです。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキーです。 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキーです。 |

##### Microsoft Azure Storage

Delta Lake catalogs は v3.0 以降、Microsoft Azure Storage をサポートしています。

###### Azure Blob Storage

<<<<<<< HEAD
Delta Lake クラスターのストレージとして Blob Storage を選択した場合、次のいずれかの操作を行います。

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
Blob Storage を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

| **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名です。   |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キーです。 |

<<<<<<< HEAD
- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- SAS トークン認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名です。                   |
  | azure.blob.container      | Yes          | データを格納する Blob コンテナの名前です。                  |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークンです。 |

###### Azure Data Lake Storage Gen2

<<<<<<< HEAD
Delta Lake クラスターのストレージとして Data Lake Storage Gen2 を選択した場合、次のいずれかの操作を行います。

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
Data Lake Storage Gen2 を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- マネージド ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
<<<<<<< HEAD
  | azure.adls2.oauth2_tenant_id            | Yes          | データにアクセスしたいテナントの ID です。          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。         |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID です。                    |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント (アプリケーション) ID です。   |

- 共有キー認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名です。 |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キーです。 |

<<<<<<< HEAD
- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

###### Azure Data Lake Storage Gen1

<<<<<<< HEAD
Delta Lake クラスターのストレージとして Data Lake Storage Gen1 を選択した場合、次のいずれかの操作を行います。

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
Data Lake Storage Gen1 を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- マネージドサービス ID 認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

<<<<<<< HEAD
- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- サービスプリンシパル認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | サービスプリンシパルのクライアント (アプリケーション) ID です。 |
  | azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント (アプリケーション) シークレットの値です。 |
  | azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント (v1) です。 |

##### Google GCS

Delta Lake catalogs は v3.0 以降、Google GCS をサポートしています。

<<<<<<< HEAD
Delta Lake クラスターのストレージとして Google GCS を選択した場合、次のいずれかの操作を行います。

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
Google GCS を Delta Lake クラスターのストレージとして選択した場合、次のいずれかの操作を行います:

- VM ベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

<<<<<<< HEAD
- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。
=======
- サービスアカウントベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>",
  ```

<<<<<<< HEAD
  次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
  次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレスです。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID です。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキーです。 |

<<<<<<< HEAD
- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように設定します。

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、次のように設定します。
=======
- インパーソネーションベースの認証方法を選択する場合、`StorageCredentialParams` を次のように構成します:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

<<<<<<< HEAD
    次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
    次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウントです。              |

<<<<<<< HEAD
  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合、次のように設定します。
=======
  - サービスアカウント (一時的にメタサービスアカウントと呼ばれる) に別のサービスアカウント (一時的にデータサービスアカウントと呼ばれる) をインパーソネートさせる場合:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

<<<<<<< HEAD
    次の表は、`StorageCredentialParams` で設定する必要があるパラメータを説明しています。
=======
    次の表は、`StorageCredentialParams` で構成する必要があるパラメータを説明しています。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレスです。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID です。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキーです。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウントです。       |

#### MetadataUpdateParams

StarRocks が Delta Lake のキャッシュされたメタデータを更新する方法に関する一連のパラメータです。このパラメータセットはオプションです。

<<<<<<< HEAD
v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。ほとんどの場合、`MetadataUpdateParams` を無視し、ポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使用できるパフォーマンスを提供します。
=======
v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。ほとんどの場合、`MetadataUpdateParams` を無視しても問題なく、ポリシーパラメータを調整する必要はありません。これらのパラメータのデフォルト値は、すぐに使えるパフォーマンスを提供します。
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

ただし、Delta Lake のデータ更新頻度が高い場合、これらのパラメータを調整して自動非同期更新のパフォーマンスをさらに最適化することができます。

:::note
ほとんどの場合、Delta Lake データが1時間以下の粒度で更新される場合、データ更新頻度は高いと見なされます。
:::

| **Parameter**                                      | **Unit** | **Default** | **Description**                                |
|----------------------------------------------------| -------- | ------------------------------------------------------------ |
| enable_deltalake_table_cache                       | -        | true         | Delta Lake のメタデータキャッシュで Table Cache を有効にするかどうか。 |
| enable_deltalake_json_meta_cache                   | -        | true         | Delta Log JSON ファイルのキャッシュを有効にするかどうか。 |
| deltalake_json_meta_cache_ttl_sec                  | Second   | 48 * 60 * 60 | Delta Log JSON ファイルキャッシュの有効期間 (TTL)。 |
| deltalake_json_meta_cache_memory_usage_ratio       | -        | 0.1          | Delta Log JSON ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |
| enable_deltalake_checkpoint_meta_cache             | -        | true         | Delta Log Checkpoint ファイルのキャッシュを有効にするかどうか。 |
| deltalake_checkpoint_meta_cache_ttl_sec            | Second   | 48 * 60 * 60 | Delta Log Checkpoint ファイルキャッシュの有効期間 (TTL)。  |
| deltalake_checkpoint_meta_cache_memory_usage_ratio | -        | 0.1          | Delta Log Checkpoint ファイルキャッシュが占める JVM ヒープサイズの最大比率。 |

### 例

<<<<<<< HEAD
次の例は、使用するメタストアのタイプに応じて、Delta Lake クラスターからデータをクエリするための `deltalake_catalog_hms` または `deltalake_catalog_glue` という名前の Delta Lake catalog を作成します。

#### HDFS

ストレージとして HDFS を使用する場合、次のようなコマンドを実行します。
=======
次の例では、使用するメタストアのタイプに応じて `deltalake_catalog_hms` または `deltalake_catalog_glue` という名前の Delta Lake catalog を作成し、Delta Lake クラスターからデータをクエリします。

#### HDFS

HDFS をストレージとして使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

##### インスタンスプロファイルベースのクレデンシャルを選択した場合

<<<<<<< HEAD
- Delta Lake クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。
=======
- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。
=======
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
##### 想定ロールベースのクレデンシャルを選択した場合

- Delta Lake クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。
=======
##### アサインされたロールベースのクレデンシャルを選択する場合

- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。
=======
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

##### IAM ユーザーベースのクレデンシャルを選択した場合

<<<<<<< HEAD
- Delta Lake クラスターで Hive metastore を使用する場合、次のようなコマンドを実行します。
=======
- Delta Lake クラスターで Hive メタストアを使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します。
=======
- Amazon EMR Delta Lake クラスターで AWS Glue を使用する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
MinIO を例にとります。次のようなコマンドを実行します。
=======
MinIO を例にとります。次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- 共有キー認証方法を選択した場合、次のようなコマンドを実行します。
=======
- 共有キー認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- SAS トークン認証方法を選択した場合、次のようなコマンドを実行します。
=======
- SAS トークン認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- マネージドサービス ID 認証方法を選択した場合、次のようなコマンドを実行します。
=======
- マネージドサービス ID 認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- サービスプリンシパル認証方法を選択した場合、次のようなコマンドを実行します。
=======
- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- マネージド ID 認証方法を選択した場合、次のようなコマンドを実行します。
=======
- マネージド ID 認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- 共有キー認証方法を選択した場合、次のようなコマンドを実行します。
=======
- 共有キー認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- サービスプリンシパル認証方法を選択した場合、次のようなコマンドを実行します。
=======
- サービスプリンシパル認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- VM ベースの認証方法を選択した場合、次のようなコマンドを実行します。
=======
- VM ベースの認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- サービスアカウントベースの認証方法を選択した場合、次のようなコマンドを実行します。
=======
- サービスアカウントベースの認証方法を選択する場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
- インパーソネーションベースの認証方法を選択した場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します。
=======
- インパーソネーションベースの認証方法を選択する場合:

  - VM インスタンスにサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します。
=======
  - サービスアカウントに別のサービスアカウントをインパーソネートさせる場合、次のようなコマンドを実行します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

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

<<<<<<< HEAD
## Delta Lake catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。
=======
## Delta Lake catalogs の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
SHOW CATALOGS;
```

<<<<<<< HEAD
また、外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。次の例は、`deltalake_catalog_glue` という名前の Delta Lake catalog の作成ステートメントをクエリします。
=======
外部 catalog の作成ステートメントをクエリするには、 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。次の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog の作成ステートメントをクエリします:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
SHOW CREATE CATALOG deltalake_catalog_glue;
```

## Delta Lake Catalog とその中のデータベースへの切り替え

<<<<<<< HEAD
Delta Lake catalog とその中のデータベースに切り替えるには、次の方法のいずれかを使用できます。

- 現在のセッションで Delta Lake catalog を指定するには [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、その後 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します。
=======
Delta Lake catalog とその中のデータベースに切り替えるには、次のいずれかの方法を使用できます:

- 現在のセッションで Delta Lake catalog を指定するには、 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用し、次に [USE](../../sql-reference/sql-statements/Database/USE.md) を使用してアクティブなデータベースを指定します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  -- 現在のセッションで指定された catalog に切り替える:
  SET CATALOG <catalog_name>
  -- 現在のセッションでアクティブなデータベースを指定する:
  USE <db_name>
  ```

<<<<<<< HEAD
- [USE](../../sql-reference/sql-statements/Database/USE.md) を直接使用して、Delta Lake catalog とその中のデータベースに切り替えます。
=======
- 直接 [USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、Delta Lake catalog とその中のデータベースに切り替えます:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Delta Lake catalog の削除

外部 catalog を削除するには、 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

<<<<<<< HEAD
次の例は、`deltalake_catalog_glue` という名前の Delta Lake catalog を削除します。
=======
次の例では、`deltalake_catalog_glue` という名前の Delta Lake catalog を削除します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
DROP Catalog deltalake_catalog_glue;
```

## Delta Lake テーブルのスキーマを表示

<<<<<<< HEAD
Delta Lake テーブルのスキーマを表示するには、次の構文のいずれかを使用できます。
=======
Delta Lake テーブルのスキーマを表示するには、次のいずれかの構文を使用できます:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Delta Lake テーブルをクエリ

<<<<<<< HEAD
1. Delta Lake クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。
=======
1. Delta Lake クラスター内のデータベースを表示するには、 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Delta Lake Catalog とその中のデータベースへの切り替え](#switch-to-a-delta-lake-catalog-and-a-database-in-it) を行います。

<<<<<<< HEAD
3. 指定されたデータベース内の宛先テーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。
=======
3. 指定されたデータベース内の宛先テーブルをクエリするには、 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Delta Lake からデータをロード

<<<<<<< HEAD
`olap_tbl` という名前の OLAP テーブルがあると仮定して、次のようにデータを変換してロードできます。
=======
`olap_tbl` という名前の OLAP テーブルがあると仮定すると、次のようにデータを変換してロードできます:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM deltalake_table
```

<<<<<<< HEAD
## メタデータキャッシュと更新戦略の設定

v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。

Delta Lake メタデータキャッシュの更新を次の FE パラメータを通じて設定できます。

| **Configuration item**                                       | **Default** | **Description**                                               |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------- |
| enable_background_refresh_connector_metadata                 | `true`      | 定期的な Delta Lake メタデータキャッシュの更新を有効にするかどうか。 有効にすると、StarRocks は Delta Lake クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Delta Lake catalog のキャッシュされたメタデータを更新してデータの変更を検知します。`true` は Delta Lake メタデータキャッシュの更新を有効にし、`false` は無効にします。 |
| background_refresh_metadata_interval_millis                  | `600000`    | 2 回の連続した Delta Lake メタデータキャッシュ更新の間隔。単位: ミリ秒。 |
=======
## メタデータキャッシュと更新戦略の構成

v3.3.3 以降、Delta Lake Catalog は [Metadata Local Cache and Retrieval](#appendix-metadata-local-cache-and-retrieval) をサポートしています。

次の FE パラメータを通じて Delta Lake メタデータキャッシュの更新を構成できます:

| **Configuration item**                                       | **Default** | **Description**                                               |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------- |
| enable_background_refresh_connector_metadata                 | `true`      | 定期的な Delta Lake メタデータキャッシュの更新を有効にするかどうか。これを有効にすると、StarRocks は Delta Lake クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Delta Lake catalog のキャッシュされたメタデータを更新してデータの変更を検知します。`true` は Delta Lake メタデータキャッシュの更新を有効にし、`false` は無効にします。 |
| background_refresh_metadata_interval_millis                  | `600000`    | 2 つの連続した Delta Lake メタデータキャッシュ更新の間隔。単位: ミリ秒。 |
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))
| background_refresh_metadata_time_secs_since_last_access_secs | `86400`     | Delta Lake メタデータキャッシュ更新タスクの有効期限。アクセスされた Delta Lake catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Delta Lake catalog に対して、StarRocks はそのキャッシュされたメタデータを更新しません。単位: 秒。 |

## 付録: メタデータローカルキャッシュと取得

<<<<<<< HEAD
メタデータファイルの繰り返しの解凍と解析は不要な遅延を引き起こす可能性があるため、StarRocks はデシリアライズされたメモリオブジェクトをキャッシュする新しいメタデータキャッシュ戦略を採用しています。これらのデシリアライズされたファイルをメモリに保存することで、後続のクエリで解凍と解析の段階をバイパスできます。このキャッシングメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ需要やマテリアライズドビューの書き換えニーズに対応しやすくなります。

この動作は、Catalog プロパティ [MetadataUpdateParams](#metadataupdateparams) と [関連する設定項目](#configure-metadata-cache-and-update-strategy) を通じて設定できます。

## 機能サポート

現在、Delta Lake catalog は次のテーブル機能をサポートしています。
=======
メタデータファイルの繰り返しの解凍と解析は不要な遅延を引き起こす可能性があるため、StarRocks はデシリアライズされたメモリオブジェクトをキャッシュする新しいメタデータキャッシュ戦略を採用しています。これらのデシリアライズされたファイルをメモリに保存することで、システムは後続のクエリで解凍と解析の段階をバイパスできます。このキャッシュメカニズムにより、必要なメタデータに直接アクセスでき、取得時間が大幅に短縮されます。その結果、システムはより応答性が高くなり、高いクエリ需要やマテリアライズドビューの書き換えニーズに対応しやすくなります。

この動作は、Catalog プロパティ [MetadataUpdateParams](#metadataupdateparams) と [関連する構成項目](#configure-metadata-cache-and-update-strategy) を通じて構成できます。

## 機能サポート

現在、Delta Lake catalogs は次のテーブル機能をサポートしています:
>>>>>>> 7cb3703511 ([Doc] move intro to snippet (#58966))

- V2 チェックポイント (v3.3.0 以降)
- タイムゾーンなしのタイムスタンプ (v3.3.1 以降)
- カラムマッピング (v3.3.6 以降)
- Deletion Vector (v3.4.0 以降)