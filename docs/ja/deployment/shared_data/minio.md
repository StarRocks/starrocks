---
displayed_sidebar: docs
---

# Use MinIO for shared-data

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## アーキテクチャ

![Shared-data Architecture](../../_assets/share_data_arch.png)

## 共有データ StarRocks クラスタのデプロイ

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BEs の代わりに CNs をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に、FE と CN の構成ファイル **fe.conf** と **cn.conf** に追加する必要がある追加の FE と CN の構成項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順については、 [Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

### 共有データ StarRocks のための FE ノードの設定

FEs を開始する前に、FE 構成ファイル **fe.conf** に以下の構成項目を追加してください。

#### run_mode

StarRocks クラスタの実行モード。有効な値:

- `shared_data`
- `shared_nothing` (デフォルト)。

> **注意**
>
> StarRocks クラスタでは `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイはサポートされていません。
>
> クラスタがデプロイされた後に `run_mode` を変更しないでください。そうしないと、クラスタが再起動に失敗します。共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。

#### cloud_native_meta_port

クラウドネイティブメタサービスの RPC ポート。

- デフォルト: `6090`

#### enable_load_volume_from_conf

FE 構成ファイルに指定されたオブジェクトストレージ関連のプロパティを使用して、StarRocks がデフォルトのストレージボリュームを作成できるかどうか。有効な値:

- `true` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 構成ファイルにあるオブジェクトストレージ関連のプロパティを使用して組み込みのストレージボリューム `builtin_storage_volume` を作成し、それをデフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みのストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、それをデフォルトのストレージボリュームとして設定する必要があります。詳細については、 [Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **注意**
>
> v3.0 から既存の共有データクラスタをアップグレードする際には、この項目を `true` のままにしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は Azure Blob (v3.1.1 以降でサポート) と S3 プロトコルに互換性のあるオブジェクトストレージ (AWS S3、Google GCP、MinIO など) にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`.

> 注意
>
> このパラメータを `S3` に指定した場合、`aws_s3` で始まるパラメータを追加する必要があります。
>
> このパラメータを `AZBLOB` に指定した場合、`azure_blob` で始まるパラメータを追加する必要があります。

#### aws_s3_path

データを保存するために使用される S3 パス。S3 バケットの名前とその下のサブパス (あれば) で構成されます。例: `testbucket/subpath`。

#### aws_s3_endpoint

S3 バケットにアクセスするために使用されるエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。

#### aws_s3_region

S3 バケットが存在するリージョン。例: `us-west-2`。

#### aws_s3_use_aws_sdk_default_behavior

[AWS SDK デフォルトクレデンシャルプロバイダーチェーン](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) を使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)。

#### aws_s3_use_instance_profile

S3 にアクセスするためのクレデンシャルメソッドとしてインスタンスプロファイルとアサインされたロールを使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)。

IAM ユーザーベースのクレデンシャル (アクセスキーとシークレットキー) を使用して S3 にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。

インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に指定する必要があります。

アサインされたロールを使用して S3 にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。

外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。

#### aws_s3_access_key

S3 バケットにアクセスするために使用されるアクセスキー ID。

#### aws_s3_secret_key

S3 バケットにアクセスするために使用されるシークレットアクセスキー。

#### aws_s3_iam_role_arn

データファイルが保存されている S3 バケットに特権を持つ IAM ロールの ARN。

#### aws_s3_external_id

S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID。

> **注意**
>
> 共有データ StarRocks クラスタが作成された後に変更できるのは、クレデンシャル関連の構成項目のみです。元のストレージパス関連の構成項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合、以下の構成項目のみを追加する必要があります:

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
enable_load_volume_from_conf = false
```

## 共有データ StarRocks のための CN ノードの設定

<SharedDataCNconf />

## 共有データ StarRocks クラスタの使用

<SharedDataUseIntro />

以下の例では、MinIO バケット `defaultbucket` のためのストレージボリューム `def_volume` を作成し、アクセスキーとシークレットキーのクレデンシャルを使用してストレージボリュームを有効にし、それをデフォルトのストレージボリュームとして設定します:

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://hostname.domainname.com:portnumber",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />