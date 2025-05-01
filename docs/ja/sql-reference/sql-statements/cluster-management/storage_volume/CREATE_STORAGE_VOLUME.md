---
displayed_sidebar: docs
---

# CREATE STORAGE VOLUME

## 説明

リモートストレージシステム用のストレージボリュームを作成します。この機能は v3.1 からサポートされています。

ストレージボリュームは、リモートデータストレージのプロパティと認証情報で構成されます。データベースやクラウドネイティブテーブルを作成する際に、[共有データ StarRocks クラスター](../../../../deployment/shared_data/s3.md)でストレージボリュームを参照できます。

> **注意**
>
> SYSTEM レベルで CREATE STORAGE VOLUME 権限を持つユーザーのみがこの操作を実行できます。

## 構文

```SQL
CREATE STORAGE VOLUME [IF NOT EXISTS] <storage_volume_name>
TYPE = { S3 | AZBLOB }
LOCATIONS = ('<remote_storage_path>')
[ COMMENT '<comment_string>' ]
PROPERTIES
("key" = "value",...)
```

## パラメータ

| **パラメータ**       | **説明**                                              |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | ストレージボリュームの名前です。`builtin_storage_volume` という名前のストレージボリュームは作成できません。これは組み込みストレージボリュームを作成するために使用されます。 |
| TYPE                | リモートストレージシステムのタイプです。有効な値は `S3` と `AZBLOB` です。`S3` は AWS S3 または S3 互換のストレージシステムを示します。`AZBLOB` は Azure Blob Storage を示します（v3.1.1 以降でサポート）。 |
| LOCATIONS           | ストレージの場所です。形式は以下の通りです：<ul><li>AWS S3 または S3 プロトコル互換のストレージシステムの場合：`s3://<s3_path>`。`<s3_path>` は絶対パスでなければなりません。例：`s3://testbucket/subpath`。</li><li>Azure Blob Storage の場合：`azblob://<azblob_path>`。`<azblob_path>` は絶対パスでなければなりません。例：`azblob://testcontainer/subpath`。</li></ul> |
| COMMENT             | ストレージボリュームに関するコメントです。                           |
| PROPERTIES          | リモートストレージシステムにアクセスするためのプロパティと認証情報を指定する `"key" = "value"` ペアのパラメータです。詳細については [PROPERTIES](#properties) を参照してください。 |

### PROPERTIES

- AWS S3 を使用する場合：

  - AWS SDK のデフォルト認証情報を使用して S3 にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "true"
    ```

  - IAM ユーザー認証情報（アクセスキーとシークレットキー）を使用して S3 にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secrete_key>"
    ```

  - インスタンスプロファイルを使用して S3 にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true"
    ```

  - アサインされたロールを使用して S3 にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "<role_arn>"
    ```

  - 外部 AWS アカウントからアサインされたロールを使用して S3 にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "aws.s3.region" = "<region>",
    "aws.s3.endpoint" = "<endpoint_url>",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "<role_arn>",
    "aws.s3.external_id" = "<external_id>"
    ```

- GCP Cloud Storage を使用する場合、以下のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例: us-east-1
  "aws.s3.region" = "<region>",
  
  -- 例: https://storage.googleapis.com
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- MinIO を使用する場合、以下のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  
  -- 例: us-east-1
  "aws.s3.region" = "<region>",
  
  -- 例: http://172.26.xx.xxx:39000
  "aws.s3.endpoint" = "<endpoint_url>",
  
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

  | **プロパティ**                        | **説明**                                              |
  | ----------------------------------- | ------------------------------------------------------------ |
  | enabled                             | このストレージボリュームを有効にするかどうか。デフォルトは `false` です。無効なストレージボリュームは参照できません。 |
  | aws.s3.region                       | S3 バケットが存在するリージョンです。例：`us-west-2`。 |
  | aws.s3.endpoint                     | S3 バケットにアクセスするためのエンドポイント URL です。例：`https://s3.us-west-2.amazonaws.com`。 |
  | aws.s3.use_aws_sdk_default_behavior | AWS SDK のデフォルト認証情報を使用するかどうか。有効な値は `true` と `false`（デフォルト）です。 |
  | aws.s3.use_instance_profile         | インスタンスプロファイルとアサインされたロールを S3 へのアクセスのための認証方法として使用するかどうか。有効な値は `true` と `false`（デフォルト）です。<ul><li>IAM ユーザー認証情報（アクセスキーとシークレットキー）を使用して S3 にアクセスする場合、この項目を `false` に設定し、`aws.s3.access_key` と `aws.s3.secret_key` を指定する必要があります。</li><li>インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に設定する必要があります。</li><li>アサインされたロールを使用して S3 にアクセスする場合、この項目を `true` に設定し、`aws.s3.iam_role_arn` を指定する必要があります。</li><li>外部 AWS アカウントを使用する場合、この項目を `true` に設定し、`aws.s3.iam_role_arn` と `aws.s3.external_id` を指定する必要があります。</li></ul> |
  | aws.s3.access_key                   | S3 バケットにアクセスするために使用されるアクセスキー ID です。             |
  | aws.s3.secret_key                   | S3 バケットにアクセスするために使用されるシークレットアクセスキーです。         |
  | aws.s3.iam_role_arn                 | データファイルが保存されている S3 バケットに対して権限を持つ IAM ロールの ARN です。 |
  | aws.s3.external_id                  | S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID です。 |

- Azure Blob Storage を使用する場合（v3.1.1 以降でサポート）：

  - Shared Key を使用して Azure Blob Storage にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.shared_key" = "<shared_key>"
    ```

  - 共有アクセス署名（SAS）を使用して Azure Blob Storage にアクセスする場合、以下のプロパティを設定します：

    ```SQL
    "enabled" = "{ true | false }",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.sas_token" = "<sas_token>"
    ```

  > **注意**
  >
  > Azure Blob Storage アカウントを作成する際には、階層型名前空間を無効にする必要があります。

  | **プロパティ**          | **説明**                                              |
  | --------------------- | ------------------------------------------------------------ |
  | enabled               | このストレージボリュームを有効にするかどうか。デフォルトは `false` です。無効なストレージボリュームは参照できません。 |
  | azure.blob.endpoint   | Azure Blob Storage アカウントのエンドポイントです。例：`https://test.blob.core.windows.net`。 |
  | azure.blob.shared_key | Azure Blob Storage のリクエストを認証するために使用される Shared Key です。 |
  | azure.blob.sas_token  | Azure Blob Storage のリクエストを認証するために使用される共有アクセス署名（SAS）です。 |

## 例

例 1: AWS S3 バケット `defaultbucket` 用のストレージボリューム `my_s3_volume` を作成し、IAM ユーザー認証情報（アクセスキーとシークレットキー）を使用して S3 にアクセスし、有効にします。

```SQL
CREATE STORAGE VOLUME my_s3_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);
```

## 関連する SQL ステートメント

- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)