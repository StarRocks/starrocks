---
description: AWS で認証する方法について説明します
displayed_sidebar: docs
---

# AWS リソースへの認証

StarRocks は、AWS リソースと統合するために 3 つの認証方法をサポートしています: インスタンスプロファイルベースの認証、役割引受けベースの認証、および IAM ユーザーベースの認証です。このトピックでは、これらの認証方法を使用して AWS 資格情報を設定する方法について説明します。

## 認証方法

### インスタンスプロファイルベースの認証

インスタンスプロファイルベースの認証方法では、StarRocks クラスターがクラスターが実行されている EC2 インスタンスのインスタンスプロファイルで指定された権限を継承することができます。理論的には、クラスターにログインできるクラスターのユーザーは、設定した AWS IAM ポリシーに従って AWS リソース上で許可された操作を実行できます。このユースケースの典型的なシナリオは、クラスター内の複数のクラスターユーザー間で AWS リソースアクセス制御が不要な場合です。この認証方法は、同じクラスター内での隔離が不要であることを意味します。

ただし、この認証方法は、クラスターにログインできる人がクラスター管理者によって制御されているため、クラスター レベルの安全なアクセス制御ソリューションと見なすことができます。

### 役割引受けベースの認証

インスタンスプロファイルベースの認証とは異なり、役割引受けベースの認証方法は、AWS IAM ロールを引き受けて AWS リソースにアクセスすることをサポートしています。詳細については、[Assuming a role](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-sharing-logs-assume-role.html) を参照してください。

### IAM ユーザーベースの認証

IAM ユーザーベースの認証方法は、IAM ユーザー資格情報を使用して AWS リソースにアクセスすることをサポートしています。詳細については、[IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) を参照してください。

## 準備

まず、StarRocks クラスターが実行されている EC2 インスタンスに関連付けられている IAM ロールを見つけ、そのロールの ARN を取得します。このトピックでは、以降このロールを EC2 インスタンスロールと呼びます。インスタンスプロファイルベースの認証には EC2 インスタンスロールが必要であり、役割引受けベースの認証には EC2 インスタンスロールとその ARN が必要です。

次に、アクセスしたい AWS リソースの種類と StarRocks 内での特定の操作シナリオに基づいて IAM ポリシーを作成します。AWS IAM のポリシーは、特定の AWS リソースに対する一連の権限を宣言します。ポリシーを作成した後、それを IAM ロールまたはユーザーにアタッチする必要があります。このようにして、IAM ロールまたはユーザーは、ポリシーで宣言された権限を割り当てられ、指定された AWS リソースにアクセスできます。

> **注意**
>
> これらの準備を行うには、[AWS IAM コンソール](https://us-east-1.console.aws.amazon.com/iamv2/home#/home) にサインインし、IAM ユーザーとロールを編集する権限が必要です。

特定の AWS リソースにアクセスするために必要な IAM ポリシーについては、以下のセクションを参照してください。

- [Batch load data from AWS S3](../sql-reference/aws_iam_policies.md#batch-load-data-from-aws-s3)
- [Read/write AWS S3](../sql-reference/aws_iam_policies.md#readwrite-aws-s3)
- [Integrate with AWS Glue](../sql-reference/aws_iam_policies.md#integrate-with-aws-glue)

### インスタンスプロファイルベースの認証の準備

必要な AWS リソースにアクセスするための [IAM ポリシー](../sql-reference/aws_iam_policies.md) を EC2 インスタンスロールにアタッチします。

### 役割引受けベースの認証の準備

#### IAM ロールを作成し、ポリシーをアタッチする

アクセスしたい AWS リソースに応じて、1 つ以上の IAM ロールを作成します。[Creating IAM roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html) を参照してください。その後、必要な AWS リソースにアクセスするための [IAM ポリシー](../sql-reference/aws_iam_policies.md) を作成した IAM ロールにアタッチします。

例えば、StarRocks クラスターが AWS S3 と AWS Glue にアクセスすることを望む場合、この状況では 1 つの IAM ロール（例えば `s3_assumed_role`）を作成し、AWS S3 にアクセスするためのポリシーと AWS Glue にアクセスするためのポリシーの両方をそのロールにアタッチすることができます。あるいは、2 つの異なる IAM ロール（例えば `s3_assumed_role` と `glue_assumed_role`）を作成し、これらのポリシーをそれぞれのロールにアタッチすることもできます（つまり、AWS S3 にアクセスするためのポリシーを `s3_assumed_role` に、AWS Glue にアクセスするためのポリシーを `glue_assumed_role` にアタッチします）。

作成した IAM ロールは、StarRocks クラスターの EC2 インスタンスロールによって引き受けられ、指定された AWS リソースにアクセスします。

このセクションでは、1 つの引受けロール `s3_assumed_role` のみを作成し、AWS S3 にアクセスするためのポリシーと AWS Glue にアクセスするためのポリシーの両方をそのロールに追加したと仮定します。

#### 信頼関係を設定する

引受けロールを次のように設定します。

1. [AWS IAM コンソール](https://us-east-1.console.aws.amazon.com/iamv2/home#/home) にサインインします。
2. 左側のナビゲーションペインで、**Access management** > **Roles** を選択します。
3. 引受けロール (`s3_assumed_role`) を見つけて、その名前をクリックします。
4. ロールの詳細ページで、**Trust relationships** タブをクリックし、**Trust relationships** タブで **Edit trust policy** をクリックします。
5. **Edit trust policy** ページで、既存の JSON ポリシードキュメントを削除し、以下の IAM ポリシーを貼り付けます。この際、上で取得した EC2 インスタンスロールの ARN を `<cluster_EC2_iam_role_ARN>` に置き換える必要があります。その後、**Update policy** をクリックします。

   ```JSON
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Principal": {
                   "AWS": "<cluster_EC2_iam_role_ARN>"
               },
               "Action": "sts:AssumeRole"
           }
       ]
   }
   ```

異なる AWS リソースにアクセスするために異なる引受けロールを作成した場合は、他の引受けロールを設定するために上記の手順を繰り返す必要があります。例えば、AWS S3 と AWS Glue にアクセスするために `s3_assumed_role` と `glue_assumed_role` を作成した場合、この状況では `glue_assumed_role` を設定するために上記の手順を繰り返す必要があります。

EC2 インスタンスロールを次のように設定します。

1. [AWS IAM コンソール](https://us-east-1.console.aws.amazon.com/iamv2/home#/home) にサインインします。
2. 左側のナビゲーションペインで、**Access management** > **Roles** を選択します。
3. EC2 インスタンスロールを見つけて、その名前をクリックします。
4. ロールの詳細ページの **Permissions policies** セクションで、**Add permissions** をクリックし、**Create inline policy** を選択します。
5. **Specify permissions** ステップで、**JSON** タブをクリックし、既存の JSON ポリシードキュメントを削除し、以下の IAM ポリシーを貼り付けます。この際、引受けロール `s3_assumed_role` の ARN を `<s3_assumed_role_ARN>` に置き換える必要があります。その後、**Review policy** をクリックします。

   ```JSON
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": ["sts:AssumeRole"],
               "Resource": [
                   "<s3_assumed_role_ARN>"
               ]
           }
       ]
   }
   ```

   異なる AWS リソースにアクセスするために異なる引受けロールを作成した場合は、上記の IAM ポリシーの **Resource** 要素にこれらの引受けロールの ARN をすべて記入し、カンマ (,) で区切る必要があります。例えば、AWS S3 と AWS Glue にアクセスするために `s3_assumed_role` と `glue_assumed_role` を作成した場合、この状況では `s3_assumed_role` の ARN と `glue_assumed_role` の ARN を次の形式で **Resource** 要素に記入する必要があります: `"<s3_assumed_role_ARN>","<glue_assumed_role_ARN>"`。

6. **Review Policy** ステップで、ポリシー名を入力し、**Create policy** をクリックします。

### IAM ユーザーベースの認証の準備

IAM ユーザーを作成します。[Creating an IAM user in your AWS account](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) を参照してください。

その後、必要な AWS リソースにアクセスするための [IAM ポリシー](../sql-reference/aws_iam_policies.md) を作成した IAM ユーザーにアタッチします。

## 認証方法の比較

以下の図は、StarRocks におけるインスタンスプロファイルベースの認証、役割引受けベースの認証、および IAM ユーザーベースの認証のメカニズムの違いを高レベルで説明しています。

![Comparison between authentication methods](../_assets/authenticate_s3_credential_methods.png)

## AWS リソースとの接続を構築する

### AWS S3 にアクセスするための認証パラメータ

StarRocks が AWS S3 と統合する必要があるさまざまなシナリオ、例えば、external catalogs や file external tables を作成する場合、または AWS S3 からデータを取り込んだり、バックアップしたり、復元したりする場合に、AWS S3 にアクセスするための認証パラメータを次のように設定します。

- インスタンスプロファイルベースの認証の場合、`aws.s3.use_instance_profile` を `true` に設定します。
- 役割引受けベースの認証の場合、`aws.s3.use_instance_profile` を `true` に設定し、`aws.s3.iam_role_arn` を AWS S3 にアクセスするために使用する引受けロールの ARN（例えば、上記で作成した引受けロール `s3_assumed_role` の ARN）として設定します。
- IAM ユーザーベースの認証の場合、`aws.s3.use_instance_profile` を `false` に設定し、`aws.s3.access_key` と `aws.s3.secret_key` を AWS IAM ユーザーのアクセスキーとシークレットキーとして設定します。

以下の表は、パラメータを説明しています。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.use_instance_profile | はい | インスタンスプロファイルベースの認証方法と役割引受けベースの認証方法を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | いいえ | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。AWS S3 にアクセスするために役割引受けベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.access_key           | いいえ | IAM ユーザーのアクセスキー。AWS S3 にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ | IAM ユーザーのシークレットキー。AWS S3 にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |

### AWS Glue にアクセスするための認証パラメータ

StarRocks が AWS Glue と統合する必要があるさまざまなシナリオ、例えば、external catalogs を作成する場合に、AWS Glue にアクセスするための認証パラメータを次のように設定します。

- インスタンスプロファイルベースの認証の場合、`aws.glue.use_instance_profile` を `true` に設定します。
- 役割引受けベースの認証の場合、`aws.glue.use_instance_profile` を `true` に設定し、`aws.glue.iam_role_arn` を AWS Glue にアクセスするために使用する引受けロールの ARN（例えば、上記で作成した引受けロール `glue_assumed_role` の ARN）として設定します。
- IAM ユーザーベースの認証の場合、`aws.glue.use_instance_profile` を `false` に設定し、`aws.glue.access_key` と `aws.glue.secret_key` を AWS IAM ユーザーのアクセスキーとシークレットキーとして設定します。

以下の表は、パラメータを説明しています。

| パラメータ                     | 必須 | 説明                                                  |
| ----------------------------- | ---- | ----------------------------------------------------- |
| aws.glue.use_instance_profile | はい | インスタンスプロファイルベースの認証方法と役割引受けベースの認証を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.glue.iam_role_arn         | いいえ | AWS Glue Data Catalog に対する権限を持つ IAM ロールの ARN。AWS Glue にアクセスするために役割引受けベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.access_key           | いいえ | AWS IAM ユーザーのアクセスキー。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |
| aws.glue.secret_key           | いいえ | AWS IAM ユーザーのシークレットキー。AWS Glue にアクセスするために IAM ユーザーベースの認証方法を使用する場合、このパラメータを指定する必要があります。 |

## 統合例

### external catalog

StarRocks クラスターで external catalog を作成することは、ターゲットデータレイクシステムとの統合を構築することを意味し、これは 2 つの主要なコンポーネントで構成されています。

- テーブルファイルを保存するための AWS S3 のようなファイルストレージ
- テーブルファイルのメタデータと位置を保存するための Hive metastore または AWS Glue のようなメタストア

StarRocks は次の種類の catalogs をサポートしています。

- [Hive catalog](../data_source/catalog/hive_catalog.md)
- [Iceberg catalog](../data_source/catalog/iceberg_catalog.md)
- [Hudi catalog](../data_source/catalog/hudi_catalog.md)
- [Delta Lake catalog](../data_source/catalog/deltalake_catalog.md)

以下の例では、使用するメタストアの種類に応じて `hive_catalog_hms` または `hive_catalog_glue` という名前の Hive catalog を作成し、Hive クラスターからデータをクエリします。詳細な構文とパラメータについては、[Hive catalog](../data_source/catalog/hive_catalog.md) を参照してください。

#### インスタンスプロファイルベースの認証

- Hive クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2"
  );
  ```

#### 役割引受けベースの認証

- Hive クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/s3_assumed_role",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/s3_assumed_role",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/glue_assumed_role",
      "aws.glue.region" = "us-west-2"
  );
  ```

#### IAM ユーザーベースの認証

- Hive クラスターで Hive metastore を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
  );
  ```

- Amazon EMR Hive クラスターで AWS Glue を使用する場合、以下のようなコマンドを実行します。

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2"
  );
  ```

### ファイル外部テーブル

ファイル外部テーブルは、`default_catalog` という名前の内部カタログに作成する必要があります。

以下の例では、`test_s3_db` という既存のデータベースに `file_table` という名前のファイル外部テーブルを作成します。詳細な構文とパラメータについては、[ファイル外部テーブル](../data_source/file_external_table.md) を参照してください。

#### インスタンスプロファイルベースの認証

以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-west-2"
);
```

#### 役割引受けベースの認証

以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/s3_assumed_role",
    "aws.s3.region" = "us-west-2"
);
```

#### IAM ユーザーベースの認証

以下のようなコマンドを実行します。

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>",
    "aws.s3.region" = "us-west-2"
);
```

### データの取り込み

LOAD LABEL を使用して AWS S3 からデータをロードできます。

以下の例では、`s3a://test-bucket/test_brokerload_ingestion` パスに保存されているすべての Parquet データファイルを、`test_s3_db` という既存のデータベース内の `test_ingestion_2` テーブルにロードします。詳細な構文とパラメータについては、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

#### インスタンスプロファイルベースの認証

以下のようなコマンドを実行します。

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-west-1"
)
PROPERTIES
(
    "timeout" = "1200"
);
```

#### 役割引受けベースの認証

以下のようなコマンドを実行します。

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/s3_assumed_role",
    "aws.s3.region" = "us-west-1"
)
PROPERTIES
(
    "timeout" = "1200"
);
```

#### IAM ユーザーベースの認証

以下のようなコマンドを実行します。

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>",
    "aws.s3.region" = "us-west-1"
)
PROPERTIES
(
    "timeout" = "1200"
);
```
```