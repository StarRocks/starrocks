---
displayed_sidebar: docs
---

# 共有データに GCS を使用する

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## アーキテクチャ

![Shared-data Architecture](../../_assets/share_data_arch.png)

## 共有データ StarRocks クラスタのデプロイ

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BEs の代わりに CNs をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に **fe.conf** と **cn.conf** の設定ファイルに追加する必要がある FE と CN の追加設定項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順は、[Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

> **注意**
>
> このドキュメントの次のセクションで共有ストレージ用に設定されるまで、クラスタを開始しないでください。

## 共有データ StarRocks 用の FE ノードを設定する

クラスタを開始する前に、FEs と CNs を設定します。以下に設定例を示し、各パラメータの詳細を説明します。

### GCS 用の FE 設定例

`fe.conf` に対する共有データの追加設定は、各 FE ノードの `fe.conf` ファイルに追加できます。GCS ストレージは [Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview) を使用してアクセスされるため、パラメータは `aws_s3` プレフィックスを使用します。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
cloud_native_storage_type = S3

# 例: testbucket/subpath
aws_s3_path = <s3_path>

# 例: us-east1
aws_s3_region = <region>

# 例: https://storage.googleapis.com
aws_s3_endpoint = <endpoint_url>

aws_s3_access_key = <HMAC access_key>
aws_s3_secret_key = <HMAC secret_key>
```

### GCS に関連するすべての FE パラメータ

#### run_mode

StarRocks クラスタの実行モード。有効な値:

- `shared_data`
- `shared_nothing` (デフォルト)

> **注意**
>
> - StarRocks クラスタで `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイはサポートされていません。
> - クラスタがデプロイされた後に `run_mode` を変更しないでください。そうしないと、クラスタが再起動に失敗します。共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。

#### cloud_native_meta_port

クラウドネイティブメタサービスの RPC ポート。

- デフォルト: `6090`

#### enable_load_volume_from_conf

StarRocks が FE 設定ファイルに指定されたオブジェクトストレージ関連のプロパティを使用してデフォルトのストレージボリュームを作成できるかどうか。有効な値:

- `true` 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 設定ファイルのオブジェクトストレージ関連のプロパティを使用して組み込みストレージボリューム `builtin_storage_volume` を作成し、デフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、デフォルトのストレージボリュームとして設定する必要があります。詳細については、[Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **注意**
>
> v3.0 から既存の共有データクラスタをアップグレードする際には、この項目を `true` にしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は Azure Blob (v3.1.1 以降でサポート) と S3 プロトコルと互換性のあるオブジェクトストレージ (AWS S3、Google GCS、MinIO など) にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`
- `HDFS`

> **注意**
>
> - このパラメータを `S3` に指定する場合、`aws_s3` プレフィックスのパラメータを追加する必要があります。
> - このパラメータを `AZBLOB` に指定する場合、`azure_blob` プレフィックスのパラメータを追加する必要があります。
> - このパラメータを `HDFS` に指定する場合、`cloud_native_hdfs_url` パラメータを追加する必要があります。

#### aws_s3_path

データを保存するために使用される S3 パス。S3 バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。

#### aws_s3_endpoint

S3 バケットにアクセスするために使用されるエンドポイント。例: `https://storage.googleapis.com/`

#### aws_s3_region

S3 バケットが存在するリージョン。例: `us-west-2`。

#### aws_s3_use_instance_profile

GCS にアクセスするためのクレデンシャルメソッドとしてインスタンスプロファイルとアサインドロールを使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)

IAM ユーザーに基づくクレデンシャル (アクセスキーとシークレットキー) を使用して GCS にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。

インスタンスプロファイルを使用して GCS にアクセスする場合、この項目を `true` に指定する必要があります。

アサインドロールを使用して GCS にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。

外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。

#### aws_s3_access_key

GCS バケットにアクセスするために使用される HMAC アクセスキー ID。

#### aws_s3_secret_key

GCS バケットにアクセスするために使用される HMAC シークレットアクセスキー。

#### aws_s3_iam_role_arn

データファイルが保存されている GCS バケットに対して権限を持つ IAM ロールの ARN。

#### aws_s3_external_id

GCS バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID。

> **注意**
>
> 共有データ StarRocks クラスタが作成された後に変更できるのは、クレデンシャル関連の設定項目のみです。元のストレージパス関連の設定項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合、次の設定項目を追加するだけで済みます。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
```

## 共有データ StarRocks 用の CN ノードを設定する

<SharedDataCNconf />

## 共有データ StarRocks クラスタを使用する

<SharedDataUseIntro />

以下の例では、GCS バケット `defaultbucket` に対してストレージボリューム `def_volume` を作成し、HMAC アクセスキーとシークレットキーを使用し、[Partitioned Prefix](../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md#partitioned-prefix) 機能を有効にし、デフォルトのストレージボリュームとして設定します。

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-east1",
    "aws.s3.endpoint" = "https://storage.googleapis.com",
    "aws.s3.access_key" = "<HMAC access key>",
    "aws.s3.secret_key" = "<HMAC secret key>",
    "aws.s3.enable_partitioned_prefix" = "true"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />