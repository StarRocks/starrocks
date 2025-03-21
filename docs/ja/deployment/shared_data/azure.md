---
displayed_sidebar: docs
---

# 共有データに Azure Storage を使用する

import SharedDataIntro from '../../_assets/commonMarkdown/sharedDataIntro.md'
import SharedDataCNconf from '../../_assets/commonMarkdown/sharedDataCNconf.md'
import SharedDataUseIntro from '../../_assets/commonMarkdown/sharedDataUseIntro.md'
import SharedDataUse from '../../_assets/commonMarkdown/sharedDataUse.md'

<SharedDataIntro />

## アーキテクチャ

![Shared-data Architecture](../../_assets/share_data_arch.png)

## 共有データ StarRocks クラスタをデプロイする

共有データ StarRocks クラスタのデプロイは、共有なし StarRocks クラスタのデプロイと似ています。唯一の違いは、共有データクラスタでは BE の代わりに CN をデプロイする必要があることです。このセクションでは、共有データ StarRocks クラスタをデプロイする際に、FE と CN の設定ファイル **fe.conf** および **cn.conf** に追加する必要がある追加の FE および CN 設定項目のみをリストします。StarRocks クラスタのデプロイに関する詳細な手順については、[Deploy StarRocks](../../deployment/deploy_manually.md) を参照してください。

> **注意**
>
> このドキュメントの次のセクションで共有ストレージ用に設定されるまで、クラスタを開始しないでください。

## 共有データ StarRocks 用の FE ノードを設定する

クラスタを開始する前に、FEs と CNs を設定します。以下に例として設定を示し、その後各パラメータの詳細を提供します。

### Azure Storage 用の FE 設定例

`fe.conf` に対する共有データの追加は、各 FE ノードの `fe.conf` ファイルに追加できます。

**Azure Blob Storage**

- Azure Blob Storage にアクセスするために共有キーを使用する場合、次の設定項目を追加します。

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = AZBLOB

  # 例: testcontainer/subpath
  azure_blob_path = <blob_path>

  # 例: https://test.blob.core.windows.net
  azure_blob_endpoint = <endpoint_url>

  azure_blob_shared_key = <shared_key>
  ```

- Azure Blob Storage にアクセスするために共有アクセス署名 (SAS) を使用する場合、次の設定項目を追加します。

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = AZBLOB

  # 例: testcontainer/subpath
  azure_blob_path = <blob_path>

  # 例: https://test.blob.core.windows.net
  azure_blob_endpoint = <endpoint_url>

  azure_blob_sas_token = <sas_token>
  ```

> **注意**
>
> Azure Blob Storage アカウントを作成する際、階層的な名前空間は無効にする必要があります。

**Azure Data Lake Storage Gen2**

- Azure Data Lake Storage Gen2 にアクセスするために共有キーを使用する場合、次の設定項目を追加します。

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = ADLS2

  # For example, testfilesystem/starrocks
  azure_adls2_path = <file_system_name>/<dir_name>

  # For example, https://test.dfs.core.windows.net
  azure_adls2_endpoint = <endpoint_url>

  azure_adls2_shared_key = <shared_key>
  ```

- Azure Data Lake Storage Gen2 にアクセスするために共有アクセス署名 (SAS) を使用する場合、次の設定項目を追加します。

  ```Properties
  run_mode = shared_data
  cloud_native_meta_port = <meta_port>
  cloud_native_storage_type = ADLS2

  # For example, testfilesystem/starrocks
  azure_adls2_path = <file_system_name>/<dir_name>

  # For example, https://test.dfs.core.windows.net
  azure_adls2_endpoint = <endpoint_url>

  azure_adls2_sas_token = <sas_token>
  ```

> **注意**
>
> Azure Data Lake Storage Gen1 はサポートされていません。

### Azure Storage に関連する共有ストレージのすべての FE パラメータ

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

- `true` 新しい共有データクラスタを作成する際にこの項目を `true` に指定すると、StarRocks は FE 設定ファイルのオブジェクトストレージ関連のプロパティを使用して組み込みストレージボリューム `builtin_storage_volume` を作成し、それをデフォルトのストレージボリュームとして設定します。ただし、オブジェクトストレージ関連のプロパティを指定していない場合、StarRocks は起動に失敗します。
- `false` (デフォルト) 新しい共有データクラスタを作成する際にこの項目を `false` に指定すると、StarRocks は組み込みストレージボリュームを作成せずに直接起動します。StarRocks でオブジェクトを作成する前に、手動でストレージボリュームを作成し、それをデフォルトのストレージボリュームとして設定する必要があります。詳細については、[Create the default storage volume](#use-your-shared-data-starrocks-cluster) を参照してください。

v3.1.0 からサポートされています。

> **注意**
>
> v3.0 から既存の共有データクラスタをアップグレードする際は、この項目を `true` のままにしておくことを強くお勧めします。この項目を `false` に指定すると、アップグレード前に作成したデータベースとテーブルが読み取り専用になり、それらにデータをロードできなくなります。

#### cloud_native_storage_type

使用するオブジェクトストレージのタイプです。共有データモードでは、StarRocks は Azure Blob (v3.1.1 以降でサポート) および S3 プロトコルと互換性のあるオブジェクトストレージ (AWS S3、Google GCP、MinIO など) にデータを保存することをサポートしています。有効な値:

- `S3` (デフォルト)
- `AZBLOB`
- `HDFS`

> **注意**
>
> - このパラメータを `S3` に指定した場合、`aws_s3` で始まるパラメータを追加する必要があります。
> - このパラメータを `AZBLOB` に指定した場合、`azure_blob` で始まるパラメータを追加する必要があります。
> - このパラメータを `HDFS` に指定した場合、`cloud_native_hdfs_url` パラメータを追加する必要があります。

#### azure_blob_path

データを保存するために使用する Azure Blob Storage のパスです。ストレージアカウント内のコンテナの名前と、コンテナ内のサブパス (存在する場合) で構成されます。例: `testcontainer/subpath`。

#### azure_blob_endpoint

Azure Blob Storage アカウントのエンドポイントです。例: `https://test.blob.core.windows.net`。

#### azure_blob_shared_key

Azure Blob Storage のリクエストを承認するために使用する共有キーです。

#### azure_blob_sas_token

Azure Blob Storage のリクエストを承認するために使用する共有アクセス署名 (SAS) です。

#### azure_adls2_path

データを保存するために使用する Azure Data Lake Storage Gen2 のパスです。ストレージアカウント内のコンテナの名前と、コンテナ内のサブパス (存在する場合) で構成されます。例: `testcontainer/subpath`。

#### azure_adls2_endpoint

Azure Data Lake Storage Gen2 アカウントのエンドポイントです。例: `https://test.dfs.core.windows.net`。

#### azure_adls2_shared_key

Azure Data Lake Storage Gen2 のリクエストを承認するために使用する共有キーです。

#### azure_adls2_sas_token

Azure Data Lake Storage Gen2 のリクエストを承認するために使用する共有アクセス署名 (SAS) です。

> **注意**
>
> 共有データ StarRocks クラスタが作成された後、資格情報に関連する設定項目のみを変更できます。元のストレージパスに関連する設定項目を変更した場合、変更前に作成したデータベースとテーブルが読み取り専用になり、それらにデータをロードできなくなります。

クラスタが作成された後にデフォルトのストレージボリュームを手動で作成したい場合は、次の設定項目のみを追加する必要があります。

```Properties
run_mode = shared_data
cloud_native_meta_port = <meta_port>
```

## 共有データ StarRocks 用の CN ノードを設定する

<SharedDataCNconf />

## 共有データ StarRocks クラスタを使用する

<SharedDataUseIntro />

次の例では、Azure Blob Storage バケット `defaultbucket` に対して共有キーアクセスを使用してストレージボリューム `def_volume` を作成し、ストレージボリュームを有効にしてデフォルトのストレージボリュームとして設定します。

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = AZBLOB
LOCATIONS = ("azblob://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "azure.blob.endpoint" = "<endpoint_url>",
    "azure.blob.shared_key" = "<shared_key>"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

次の例では、Azure Data Lake Storage Gen2 ファイルシステム `testfilesystem` に対して SAS クセスを使用してストレージボリューム `adls2` を作成し、ストレージボリュームを無効にして設定します。

```SQL
CREATE STORAGE VOLUME adls2
    TYPE = ADLS2
    LOCATIONS = ("adls2://testfilesystem/starrocks")
    PROPERTIES (
        "enabled" = "false",
        "azure.adls2.endpoint" = "<endpoint_url>",
        "azure.adls2.sas_token" = "<sas_token>"
    );
```

<SharedDataUse />