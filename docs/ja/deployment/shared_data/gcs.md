---
displayed_sidebar: docs
---

# GCS を使用して StarRocks をデプロイする

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## アーキテクチャ

![Shared-data Architecture](../../_assets/share_data_arch.png)

## 共有データ StarRocks クラスタをデプロイする

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BEs の代わりに CNs をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に、FE と CN の構成ファイル **fe.conf** と **cn.conf** に追加する必要がある追加の FE と CN の構成項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順については、 [Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

> **注意**
>
> このドキュメントの次のセクションで共有ストレージ用に構成されるまで、クラスタを開始しないでください。

## 共有データ StarRocks 用の FE ノードを構成する

クラスタを開始する前に、FEs と CNs を構成します。以下に例として構成を示し、その後に各パラメータの詳細を示します。

### GCS 用の FE 構成例

`fe.conf` に対する共有データの追加例は、各 FE ノードの `fe.conf` ファイルに追加できます。GCS ストレージは [Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview) を使用してアクセスされるため、パラメータはプレフィックス `aws_s3` を使用します。

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
- `shared_nothing` (デフォルト)。

> **注意**
>
> StarRocks クラスタで `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイはサポートされていません。
>
> クラスタがデプロイされた後に `run_mode` を変更しないでください。そうしないと、クラスタが再起動に失敗します。共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。

#### cloud_native_meta_port

クラウドネイティブメタサービスの RPC ポート。

- デフォルト: `6090`

#### enable_load_volume_from_conf

FE 構成ファイルに指定されたオブジェクトストレージ関連のプロパティを使用して、StarRocks がデフォルトのストレージボリュームを作成できるかどうか。有効な値:

- `true` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 構成ファイルのオブジェクトストレージ関連のプロパティを使用して組み込みストレージボリューム `builtin_storage_volume` を作成し、デフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、デフォルトのストレージボリュームとして設定する必要があります。詳細については、 [Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **注意**
>
> v3.0 から既存の共有データクラスタをアップグレードする際には、この項目を `true` のままにしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は Azure Blob (v3.1.1 以降でサポート) と S3 プロトコルと互換性のあるオブジェクトストレージ (AWS S3、Google GCS、MinIO など) にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`

#### aws_s3_path

データを保存するために使用される S3 パス。S3 バケットの名前とその下のサブパス (あれば) で構成されます。例: `testbucket/subpath`

#### aws_s3_endpoint

S3 バケットにアクセスするために使用されるエンドポイント。例: `https://storage.googleapis.com/`

#### aws_s3_region

S3 バケットが存在するリージョン。例: `us-west-2`

#### aws_s3_use_instance_profile

GCS にアクセスするための資格情報メソッドとしてインスタンスプロファイルとアサインされたロールを使用するかどうか。有効な値:

- `true`
- `false` (デフォルト)

IAM ユーザーに基づく資格情報 (アクセスキーとシークレットキー) を使用して GCS にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。

インスタンスプロファイルを使用して GCS にアクセスする場合、この項目を `true` に指定する必要があります。

アサインされたロールを使用して GCS にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。

外部 AWS アカウントを使用する場合は、`aws_s3_external_id` も指定する必要があります。

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
> 共有データ StarRocks クラスタが作成された後、資格情報に関連する構成項目のみを変更できます。元のストレージパスに関連する構成項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、データをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合、次の構成項目を追加するだけです:

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
enable_load_volume_from_conf = false
```

## 共有データ StarRocks 用の CN ノードを構成する

<SharedDataCNconf />

## 共有データ StarRocks クラスタを使用する

<SharedDataUseIntro />

次の例では、GCS バケット `defaultbucket` に対して HMAC アクセスキーとシークレットキーを使用してストレージボリューム `def_volume` を作成し、ストレージボリュームを有効にしてデフォルトのストレージボリュームとして設定します:

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-east1",
    "aws.s3.endpoint" = "https://storage.googleapis.com",
    "aws.s3.access_key" = "<HMAC access key>",
    "aws.s3.secret_key" = "<HMAC secret key>"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />