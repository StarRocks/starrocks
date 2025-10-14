---
displayed_sidebar: docs
---

# Use MinIO for shared-data

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.mdx'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.mdx'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.mdx'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.mdx'

<SharedDataIntro />

## Architecture

![Shared-data Architecture](../../_assets/share_data_arch.png)

## Deploy a shared-data StarRocks cluster

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BEs の代わりに CNs をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に、FE と CN の構成ファイル **fe.conf** と **cn.conf** に追加する必要がある追加の FE および CN 構成項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順については、 [Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

> **NOTE**
>
> このドキュメントの次のセクションで共有ストレージ用に構成されるまで、クラスタを起動しないでください。

## Configure FE nodes for shared-data StarRocks

FEs を起動する前に、FE 構成ファイル **fe.conf** に次の構成項目を追加します。

### Example FE configurations for MinIO

各 FE ノードの `fe.conf` ファイルに対する共有データの追加例です。パラメータは `aws_s3` プレフィックスを使用します。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
cloud_native_storage_type = S3

# Example: testbucket/subpath
aws_s3_path = <s3_path>

# Example: us-east1
aws_s3_region = <region>

# Example: http://172.26.xx.xxx:39000
aws_s3_endpoint = <endpoint_url>

aws_s3_access_key = <minio_access_key>
aws_s3_secret_key = <minio_secret_key>
```

### All FE parameters related to shared-storage with MinIO

#### run_mode

StarRocks クラスタの実行モード。有効な値:

- `shared_data`
- `shared_nothing` (デフォルト)

> **NOTE**
>
> - StarRocks クラスタで `shared_data` と `shared_nothing` モードを同時に採用することはできません。混在デプロイはサポートされていません。
> - クラスタがデプロイされた後に `run_mode` を変更しないでください。そうしないと、クラスタが再起動に失敗します。共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。

#### cloud_native_meta_port

クラウドネイティブメタサービスの RPC ポート。

- デフォルト: `6090`

#### enable_load_volume_from_conf

FE 構成ファイルに指定されたオブジェクトストレージ関連のプロパティを使用して、StarRocks がデフォルトのストレージボリュームを作成できるかどうか。有効な値:

- `true` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 構成ファイルにあるオブジェクトストレージ関連のプロパティを使用して組み込みストレージボリューム `builtin_storage_volume` を作成し、それをデフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、それをデフォルトのストレージボリュームとして設定する必要があります。詳細については、 [Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **CAUTION**
>
> v3.0 から既存の共有データクラスタをアップグレードする際には、この項目を `true` にしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプです。共有データモードでは、StarRocks は Azure Blob (v3.1.1 以降でサポート) と S3 プロトコルと互換性のあるオブジェクトストレージ (AWS S3、Google GCP、MinIO など) にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`
- `HDFS`

> **NOTE**
>
> - このパラメータを `S3` に指定した場合、`aws_s3` プレフィックスのパラメータを追加する必要があります。
> - このパラメータを `AZBLOB` に指定した場合、`azure_blob` プレフィックスのパラメータを追加する必要があります。
> - このパラメータを `HDFS` に指定した場合、`cloud_native_hdfs_url` パラメータを追加する必要があります。

#### aws_s3_path

データを保存するために使用する S3 パスです。S3 バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。

#### aws_s3_endpoint

S3 バケットにアクセスするために使用するエンドポイントです。例: `https://s3.us-west-2.amazonaws.com`。

#### aws_s3_region

S3 バケットが存在するリージョンです。例: `us-west-2`。

#### aws_s3_use_aws_sdk_default_behavior

[AWS SDK のデフォルトのクレデンシャルプロバイダーチェーン](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) を使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)

#### aws_s3_use_instance_profile

S3 にアクセスするためのクレデンシャルメソッドとしてインスタンスプロファイルとアシュームドロールを使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)

IAM ユーザーに基づくクレデンシャル (アクセスキーとシークレットキー) を使用して S3 にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。

インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に指定する必要があります。

アシュームドロールを使用して S3 にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。

外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。

#### aws_s3_access_key

S3 バケットにアクセスするために使用するアクセスキー ID です。

#### aws_s3_secret_key

S3 バケットにアクセスするために使用するシークレットアクセスキーです。

#### aws_s3_iam_role_arn

データファイルが保存されている S3 バケットに対して権限を持つ IAM ロールの ARN です。

#### aws_s3_external_id

S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID です。

> **NOTE**
>
> 共有データ StarRocks クラスタが作成された後に変更できるのは、クレデンシャル関連の構成項目のみです。元のストレージパス関連の構成項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合は、次の構成項目を追加するだけです。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
enable_load_volume_from_conf = false
```

## Configure CN nodes for shared-data StarRocks

<SharedDataCNconf />

## Use your shared-data StarRocks cluster

<SharedDataUseIntro />

次の例では、MinIO バケット `defaultbucket` に対してアクセスキーとシークレットキーのクレデンシャルを使用し、 [Partitioned Prefix](../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md#partitioned-prefix) 機能を有効にし、それをデフォルトのストレージボリュームとして設定します。

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://hostname.domainname.com:portnumber",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.enable_partitioned_prefix" = "true"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />