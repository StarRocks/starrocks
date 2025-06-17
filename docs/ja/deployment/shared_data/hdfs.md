---
displayed_sidebar: docs
---

# 共有データに HDFS を使用する

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## アーキテクチャ

![Shared-data Architecture](../../_assets/share_data_arch.png)

## 共有データ StarRocks クラスタをデプロイする

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BE の代わりに CN をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に、FE と CN の設定ファイル **fe.conf** と **cn.conf** に追加する必要がある FE と CN の追加設定項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順については、[Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

> **注意**
>
> このドキュメントの次のセクションで共有ストレージ用に設定するまで、クラスタを起動しないでください。

## 共有データ StarRocks 用の FE ノードを設定する

FEs を起動する前に、以下の設定項目を FE 設定ファイル **fe.conf** に追加してください。

### HDFS 用の FE 設定例

以下は、各 FE ノードの `fe.conf` ファイルに追加する共有データの例です。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
cloud_native_storage_type = HDFS

# 例: hdfs://127.0.0.1:9000/user/starrocks/
cloud_native_hdfs_url = <hdfs_url>
```

### HDFS に関連するすべての FE パラメータ

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

- `true` 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 設定ファイルのオブジェクトストレージ関連のプロパティを使用して組み込みのストレージボリューム `builtin_storage_volume` を作成し、それをデフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みのストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、それをデフォルトのストレージボリュームとして設定する必要があります。詳細については、[Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **注意**
>
> 既存の共有データクラスタを v3.0 からアップグレードする際には、この項目を `true` のままにしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、それらにデータをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は Azure Blob（v3.1.1 以降でサポート）および S3 プロトコルと互換性のあるオブジェクトストレージ（AWS S3、Google GCP、MinIO など）にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`
- `HDFS`

> **注意**
>
> - このパラメータを `S3` に指定する場合、`aws_s3` で始まるパラメータを追加する必要があります。
> - このパラメータを `AZBLOB` に指定する場合、`azure_blob` で始まるパラメータを追加する必要があります。
> - このパラメータを `HDFS` に指定する場合、`cloud_native_hdfs_url` パラメータを追加する必要があります。

#### cloud_native_hdfs_url

HDFS ストレージの URL です。例: `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。

> **注意**
>
> 共有データ StarRocks クラスタが作成された後に変更できるのは、資格情報に関連する設定項目のみです。元のストレージパスに関連する設定項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、それらにデータをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合、次の設定項目を追加するだけで済みます。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
```

## 共有データ StarRocks 用の CN ノードを設定する

<SharedDataCNconf />

## 共有データ StarRocks クラスタを使用する

<SharedDataUseIntro />

以下の例では、HDFS ストレージ用のストレージボリューム `def_volume` を作成し、そのストレージボリュームを有効にしてデフォルトのストレージボリュームとして設定します。

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/user/starrocks/");

SET def_volume AS DEFAULT STORAGE VOLUME;
```

<SharedDataUse />