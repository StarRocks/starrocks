---
displayed_sidebar: docs
---

# CREATE REPOSITORY

データのバックアップと復元のためにデータスナップショットを保存するリモートストレージシステムにリポジトリを作成します。

:::tip
バックアップと復元の概要については、[バックアップと復元ガイド](../../../administration/management/Backup_and_restore.md) を参照してください。
:::

> **CAUTION**
>
> ADMIN 権限を持つユーザーのみがリポジトリを作成できます。

リポジトリの削除に関する詳細な手順については、[DROP REPOSITORY](./DROP_REPOSITORY.md) を参照してください。

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
| READ ONLY           | 読み取り専用リポジトリを作成します。読み取り専用リポジトリからのみデータを復元できます。データを移行するために同じリポジトリを2つのクラスターで作成する場合、新しいクラスターに対して読み取り専用リポジトリを作成し、RESTORE 権限のみを付与できます。|
| repository_name     | リポジトリ名。命名規則については、[システム制限](../../System_limit.md) を参照してください。                           |
| repository_location | リモートストレージシステム内のリポジトリの場所。     |
| PROPERTIES          | リモートストレージシステムにアクセスするための認証方法。 |

**PROPERTIES**:

StarRocks は HDFS、AWS S3、Google GCS にリポジトリを作成することをサポートしています。

- HDFS の場合:
  - "username": HDFS クラスターの NameNode にアクセスするために使用するアカウントのユーザー名。
  - "password": HDFS クラスターの NameNode にアクセスするために使用するアカウントのパスワード。

- AWS S3 の場合:
  - "aws.s3.use_instance_profile": AWS S3 にアクセスするための認証方法としてインスタンスプロファイルとアサインされたロールを許可するかどうか。デフォルト: `false`。

    - IAM ユーザーに基づく認証情報（アクセスキーとシークレットキー）を使用して AWS S3 にアクセスする場合、このパラメータを指定する必要はありませんが、"aws.s3.access_key"、"aws.s3.secret_key"、および "aws.s3.region" を指定する必要があります。
    - インスタンスプロファイルを使用して AWS S3 にアクセスする場合、このパラメータを `true` に設定し、"aws.s3.region" を指定する必要があります。
    - アサインされたロールを使用して AWS S3 にアクセスする場合、このパラメータを `true` に設定し、"aws.s3.iam_role_arn" および "aws.s3.region" を指定する必要があります。
  
  - "aws.s3.access_key": Amazon S3 バケットにアクセスするために使用できるアクセスキー ID。
  - "aws.s3.secret_key": Amazon S3 バケットにアクセスするために使用できるシークレットアクセスキー。
  - "aws.s3.iam_role_arn": データファイルが保存されている AWS S3 バケットに対する権限を持つ IAM ロールの ARN。AWS S3 にアクセスするための認証方法としてアサインされたロールを使用する場合、このパラメータを指定する必要があります。その後、StarRocks は Hive catalog を使用して Hive データを分析する際にこのロールをアサインします。
  - "aws.s3.region": AWS S3 バケットが存在するリージョン。例: `us-west-1`。

> **NOTE**
>
> StarRocks は S3A プロトコルに従って AWS S3 にリポジトリを作成することのみをサポートしています。したがって、AWS S3 にリポジトリを作成する際には、`ON LOCATION` でリポジトリの場所として渡す S3 URI の `s3://` を `s3a://` に置き換える必要があります。

- Google GCS の場合:
  - "fs.s3a.access.key": Google GCS バケットにアクセスするために使用できるアクセスキー。
  - "fs.s3a.secret.key": Google GCS バケットにアクセスするために使用できるシークレットキー。
  - "fs.s3a.endpoint": Google GCS バケットにアクセスするために使用できるエンドポイント。エンドポイントアドレスに `https` を指定しないでください。

> **NOTE**
>
> StarRocks は S3A プロトコルに従って Google GCS にリポジトリを作成することのみをサポートしています。したがって、Google GCS にリポジトリを作成する際には、`ON LOCATION` でリポジトリの場所として渡す GCS URI のプレフィックスを `s3a://` に置き換える必要があります。

- MinIO の場合:
  - "aws.s3.access_key": MinIO バケットにアクセスするために使用できるアクセスキー。
  - "aws.s3.secret_key": MinIO バケットにアクセスするために使用できるシークレットキー。
  - "aws.s3.endpoint": MinIO バケットにアクセスするために使用できるエンドポイント。

## 例

例 1: Apache™ Hadoop® クラスターに `hdfs_repo` という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER
ON LOCATION "hdfs://x.x.x.x:yyyy/repo_dir/backup"
PROPERTIES(
    "username" = "xxxxxxxx",
    "password" = "yyyyyyyy"
);
```

例 2: Amazon S3 バケット `bucket_s3` に `s3_repo` という名前の読み取り専用リポジトリを作成します。

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

例 3: Google GCS バケット `bucket_gcs` に `gcs_repo` という名前のリポジトリを作成します。

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

例 4: MinIO バケット `bucket_minio` に `minio_repo` という名前のリポジトリを作成します。

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