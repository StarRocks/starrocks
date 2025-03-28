---
displayed_sidebar: docs
---

# CREATE REPOSITORY

データのスナップショットをバックアップおよび復元するために使用されるリモートストレージシステムにリポジトリを作成します。

:::tip
バックアップと復元の概要については、[バックアップと復元ガイド](../../../administration/management/Backup_and_restore.md)をご覧ください。
:::

> **CAUTION**
>
> ADMIN権限を持つユーザーのみがリポジトリを作成できます。

リポジトリの削除に関する詳細な手順については、[DROP REPOSITORY](./DROP_REPOSITORY.md)をご覧ください。

## 構文

```SQL
CREATE [READ ONLY] REPOSITORY <repository_name>
WITH BROKER
ON LOCATION "<repository_location>"
PROPERTIES ("key"="value", ...)
```

## パラメータ

| **パラメータ**      | **説明**                                                      |
| ------------------- | ------------------------------------------------------------ |
| READ ONLY           | 読み取り専用のリポジトリを作成します。読み取り専用のリポジトリからのみデータを復元できます。データを移行するために2つのクラスターで同じリポジトリを作成する場合、新しいクラスターに対して読み取り専用のリポジトリを作成し、RESTORE権限のみを付与できます。|
| repository_name     | リポジトリ名。命名規則については、[システム制限](../../System_limit.md)をご覧ください。                           |
| repository_location | リモートストレージシステム内のリポジトリの場所。               |
| PROPERTIES          | リモートストレージシステムにアクセスするための認証方法。      |

**PROPERTIES**:

StarRocksは、HDFS、AWS S3、およびGoogle GCSにリポジトリを作成することをサポートしています。

- HDFSの場合:
  - "username": HDFSクラスターのNameNodeにアクセスするために使用するアカウントのユーザー名。
  - "password": HDFSクラスターのNameNodeにアクセスするために使用するアカウントのパスワード。

- AWS S3の場合:
  - "aws.s3.use_instance_profile": AWS S3にアクセスするための認証方法としてインスタンスプロファイルとアサインされたロールを許可するかどうか。デフォルト: `false`。

    - IAMユーザーに基づく認証情報（アクセスキーとシークレットキー）を使用してAWS S3にアクセスする場合、このパラメータを指定する必要はなく、"aws.s3.access_key"、"aws.s3.secret_key"、および"aws.s3.region"を指定する必要があります。
    - インスタンスプロファイルを使用してAWS S3にアクセスする場合、このパラメータを`true`に設定し、"aws.s3.region"を指定する必要があります。
    - アサインされたロールを使用してAWS S3にアクセスする場合、このパラメータを`true`に設定し、"aws.s3.iam_role_arn"および"aws.s3.region"を指定する必要があります。
  
  - "aws.s3.access_key": Amazon S3バケットにアクセスするために使用できるアクセスキーID。
  - "aws.s3.secret_key": Amazon S3バケットにアクセスするために使用できるシークレットアクセスキー。
  - "aws.s3.iam_role_arn": データファイルが保存されているAWS S3バケットに対する権限を持つIAMロールのARN。AWS S3にアクセスするための認証方法としてアサインされたロールを使用する場合、このパラメータを指定する必要があります。その後、StarRocksはHive catalogを使用してHiveデータを分析する際にこのロールをアサインします。
  - "aws.s3.region": AWS S3バケットが存在するリージョン。例: `us-west-1`。

> **NOTE**
>
> StarRocksは、S3Aプロトコルに従ってのみAWS S3にリポジトリを作成することをサポートしています。したがって、AWS S3にリポジトリを作成する際には、リポジトリの場所として渡すS3 URIの`s3://`を`s3a://`に置き換える必要があります。

- Google GCSの場合:
  - "fs.s3a.access.key": Google GCSバケットにアクセスするために使用できるアクセスキー。
  - "fs.s3a.secret.key": Google GCSバケットにアクセスするために使用できるシークレットキー。
  - "fs.s3a.endpoint": Google GCSバケットにアクセスするために使用できるエンドポイント。エンドポイントアドレスに`https`を指定しないでください。

> **NOTE**
>
> StarRocksは、S3Aプロトコルに従ってのみGoogle GCSにリポジトリを作成することをサポートしています。したがって、Google GCSにリポジトリを作成する際には、リポジトリの場所として渡すGCS URIのプレフィックスを`s3a://`に置き換える必要があります。

- MinIOの場合:
  - "aws.s3.access_key": MinIOバケットにアクセスするために使用できるアクセスキー。
  - "aws.s3.secret_key": MinIOバケットにアクセスするために使用できるシークレットキー。
  - "aws.s3.endpoint": MinIOバケットにアクセスするために使用できるエンドポイント。

## 例

例1: Apache™ Hadoop®クラスターに`hdfs_repo`という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER
ON LOCATION "hdfs://x.x.x.x:yyyy/repo_dir/backup"
PROPERTIES(
    "username" = "xxxxxxxx",
    "password" = "yyyyyyyy"
);
```

例2: Amazon S3バケット`bucket_s3`に`read-only`リポジトリ`s3_repo`を作成します。

```SQL
CREATE READ ONLY REPOSITORY s3_repo
WITH BROKER
ON LOCATION "s3a://bucket_s3/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.region" = "us-east-1"
);
```

例3: Google GCSバケット`bucket_gcs`に`gcs_repo`という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY gcs_repo
WITH BROKER
ON LOCATION "s3a://bucket_gcs/backup"
PROPERTIES(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "storage.googleapis.com"
);
```

例4: MinIOバケット`bucket_minio`に`minio_repo`という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY minio_repo
WITH BROKER
ON LOCATION "s3://bucket_minio/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.endpoint" = "http://minio:9000"
);
```