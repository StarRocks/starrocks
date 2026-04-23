---
displayed_sidebar: docs
sidebar_label: "共有データ、データレイク、その他"
---

# FE 設定 - 共有データ、データレイク、その他

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## FE 設定項目の表示

FE の起動後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行して、パラメーター設定を確認できます。特定のパラメーターの設定をクエリするには、次のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスター管理関連コマンドを実行するには、管理者権限が必要です。
:::

## FE パラメーターの設定

### FE 動的パラメーターの設定

[`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して、FE 動的パラメーターの設定を構成または変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメーターの設定

<StaticFEConfigNote />

---

このトピックでは、以下の種類のFE構成について紹介します：
- [共有データ](#共有データ)
- [データレイク](#データレイク)
- [その他](#その他)

## 共有データ

### `aws_s3_access_key`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：S3 バケットにアクセスするために使用するアクセスキー ID。
- 導入時期：v3.0

### `aws_s3_endpoint`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：S3 バケットにアクセスするために使用するエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。
- 導入時期：v3.0

### `aws_s3_external_id`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID。
- 導入時期：v3.0

### `aws_s3_iam_role_arn`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：データファイルが格納されている S3 バケットに対する権限を持つ IAM ロールの ARN。
- 導入時期：v3.0

### `aws_s3_path`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：データを格納するために使用される S3 パス。S3 バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- 導入時期：v3.0

### `aws_s3_region`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：S3 バケットが存在するリージョン。例: `us-west-2`。
- 導入時期：v3.0

### `aws_s3_secret_key`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：S3 バケットにアクセスするために使用するシークレットアクセスキー。
- 導入時期：v3.0

### `aws_s3_use_aws_sdk_default_behavior`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：AWS SDK のデフォルトの認証資格情報を使用するかどうか。有効な値: true および false (デフォルト)。
- 導入時期：v3.0

### `aws_s3_use_instance_profile`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：S3 にアクセスするための認証方法としてインスタンスプロファイルと引き受けロールを使用するかどうか。有効な値: true および false (デフォルト)。
  - IAM ユーザーベースの認証情報 (アクセスキーとシークレットキー) を使用して S3 にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。
  - インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に指定する必要があります。
  - 引き受けロールを使用して S3 にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。
  - 外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。
- 導入時期：v3.0

### `azure_adls2_endpoint`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 アカウントのエンドポイント。例: `https://test.dfs.core.windows.net`。
- 導入時期：v3.4.1

### `azure_adls2_oauth2_client_id`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 の要求を承認するために使用されるマネージド ID のクライアント ID。
- 導入時期：v3.4.4

### `azure_adls2_oauth2_tenant_id`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 の要求を承認するために使用されるマネージド ID のテナント ID。
- 導入時期：v3.4.4

### `azure_adls2_oauth2_use_managed_identity`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 の要求を承認するためにマネージド ID を使用するかどうか。
- 導入時期：v3.4.4

### `azure_adls2_path`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：データを格納するために使用される Azure Data Lake Storage Gen2 パス。ファイルシステム名とディレクトリ名で構成されます。例: `testfilesystem/starrocks`。
- 導入時期：v3.4.1

### `azure_adls2_sas_token`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 の要求を承認するために使用される共有アクセスシグネチャ (SAS)。
- 導入時期：v3.4.1

### `azure_adls2_shared_key`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Data Lake Storage Gen2 の要求を承認するために使用される共有キー。
- 導入時期：v3.4.1

### `azure_blob_endpoint`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Blob Storage アカウントのエンドポイント。例: `https://test.blob.core.windows.net`。
- 導入時期：v3.1

### `azure_blob_path`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：データを格納するために使用される Azure Blob Storage パス。ストレージアカウント内のコンテナーの名前とコンテナー内のサブパス (存在する場合) で構成されます。例: `testcontainer/subpath`。
- 導入時期：v3.1

### `azure_blob_sas_token`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Blob Storage の要求を承認するために使用される共有アクセスシグネチャ (SAS)。
- 導入時期：v3.1

### `azure_blob_shared_key`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Azure Blob Storage の要求を承認するために使用される共有キー。
- 導入時期：v3.1

### `azure_use_native_sdk`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：Azure Blob Storage にアクセスするためにネイティブ SDK を使用するかどうか。これにより、マネージド ID およびサービスプリンシパルでの認証が可能になります。この項目が `false` に設定されている場合、共有キーと SAS トークンでの認証のみが許可されます。
- 導入時期：v3.4.4

### `cloud_native_hdfs_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：HDFS ストレージの URL。例: `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 導入時期：-

### `cloud_native_meta_port`

- デフォルト：6090
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE クラウドネイティブメタデータサーバー RPC リッスンポート。
- 導入時期：-

### `cloud_native_storage_type`

- デフォルト：S3
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は HDFS、Azure Blob (v3.1.1 以降でサポート)、Azure Data Lake Storage Gen2 (v3.4.1 以降でサポート)、Google Storage (ネイティブ SDK、v3.5.1 以降でサポート)、および S3 プロトコルと互換性のあるオブジェクトストレージシステム (AWS S3、MinIO など) にデータを格納することをサポートしています。有効な値: `S3` (デフォルト)、`HDFS`、`AZBLOB`、`ADLS2`、および `GS`。このパラメーターを `S3` に指定した場合、`aws_s3` で始まるパラメーターを追加する必要があります。`AZBLOB` に指定した場合、`azure_blob` で始まるパラメーターを追加する必要があります。`ADLS2` に指定した場合、`azure_adls2` で始まるパラメーターを追加する必要があります。`GS` に指定した場合、`gcp_gcs` で始まるパラメーターを追加する必要があります。`HDFS` に指定した場合、`cloud_native_hdfs_url` のみを指定する必要があります。
- 導入時期：-

### `enable_load_volume_from_conf`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：StarRocks が FE 設定ファイルで指定されたオブジェクトストレージ関連プロパティを使用して、組み込みストレージボリュームを作成することを許可するかどうか。デフォルト値は v3.4.1 以降 `true` から `false` に変更されました。
- 導入時期：v3.1.0

### `gcp_gcs_impersonation_service_account`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Google Storage にアクセスするために、偽装ベースの認証を使用する場合に偽装するサービスアカウント。
- 導入時期：v3.5.1

### `gcp_gcs_path`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：データを格納するために使用される Google Cloud パス。Google Cloud バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- 導入時期：v3.5.1

### `gcp_gcs_service_account_email`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：サービスアカウント作成時に生成された JSON ファイル内のメールアドレス。例: `user@hello.iam.gserviceaccount.com`。
- 導入時期：v3.5.1

### `gcp_gcs_service_account_private_key`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：サービスアカウント作成時に生成された JSON ファイル内の秘密鍵。例: `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 導入時期：v3.5.1

### `gcp_gcs_service_account_private_key_id`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：サービスアカウント作成時に生成された JSON ファイル内の秘密鍵 ID。
- 導入時期：v3.5.1

### `gcp_gcs_use_compute_engine_service_account`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：Compute Engine にバインドされているサービスアカウントを使用するかどうか。
- 導入時期：v3.5.1

### `hdfs_file_system_expire_seconds`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：HdfsFsManager が管理する未使用のキャッシュ HDFS/ObjectStore FileSystem の Time-to-live (秒単位)。FileSystemExpirationChecker (60 秒ごとに実行) はこの値を使用して各 HdfsFs.isExpired(...) を呼び出します。期限切れになるとマネージャーは基盤となる FileSystem を閉じ、キャッシュから削除します。アクセサーメソッド (例: `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`) は最終アクセス時刻を更新するため、有効期限は非アクティブに基づいています。値が小さいとアイドルリソースの保持は減りますが、再オープンオーバーヘッドが増加します。値が大きいとハンドルが長く保持され、より多くのリソースを消費する可能性があります。
- 導入時期：v3.2.0

### `lake_autovacuum_grace_period_minutes`

- デフォルト：30
- タイプ：Long
- 単位：Minutes
- 変更可能：Yes
- 説明：共有データクラスターで履歴データバージョンを保持する時間範囲。この時間範囲内の履歴データバージョンは、コンパクション後に AutoVacuum によって自動的にクリーンアップされません。実行中のクエリによってアクセスされるデータがクエリ完了前に削除されることを避けるため、この値を最大クエリ時間よりも大きく設定する必要があります。v3.3.0、v3.2.5、および v3.1.10 以降、デフォルト値は `5` から `30` に変更されました。
- 導入時期：v3.1.0

### `lake_autovacuum_parallel_partitions`

- デフォルト：8
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：共有データクラスターで同時に AutoVacuum を実行できるパーティションの最大数。AutoVacuum はコンパクション後のガベージコレクションです。
- 導入時期：v3.1.0

### `lake_autovacuum_partition_naptime_seconds`

- デフォルト：180
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：共有データクラスターで同じパーティションに対する AutoVacuum 操作間の最小間隔。
- 導入時期：v3.1.0

### `lake_autovacuum_stale_partition_threshold`

- デフォルト：12
- タイプ：Long
- 単位：Hours
- 変更可能：Yes
- 説明：この時間範囲内にパーティションが更新されていない場合 (ロード、DELETE、またはコンパクション)、システムはこのパーティションに対して AutoVacuum を実行しません。
- 導入時期：v3.1.0

### `lake_compaction_allow_partial_success`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、サブタスクの 1 つが成功した場合、システムは共有データクラスターでのコンパクション操作を成功と見なします。
- 導入時期：v3.5.2

### `lake_compaction_disable_ids`

- デフォルト：""
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：共有データモードでコンパクションが無効になっているテーブルまたはパーティションのリスト。形式はセミコロンで区切られた `tableId1;partitionId2` です。例: `12345;98765`。
- 導入時期：v3.4.4

### `lake_compaction_history_size`

- デフォルト：20
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターのリーダー FE ノードのメモリに保持される最近の成功したコンパクションタスクレコードの数。`SHOW PROC '/compactions'` コマンドを使用して、最近の成功したコンパクションタスクレコードを表示できます。コンパクション履歴は FE プロセスメモリに格納され、FE プロセスが再起動されると失われることに注意してください。
- 導入時期：v3.1.0

### `lake_compaction_max_parallel_default`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 当建表时未指定 `lake_compaction_max_parallel` 表属性时，每个 tablet 的默认最大并行 Compaction 子任务数。`0` 表示禁用并行 Compaction。此配置作为表属性 `lake_compaction_max_parallel` 的默认值。

### `lake_compaction_max_tasks`

- デフォルト：-1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターで許可される同時コンパクションタスクの最大数。この項目を `-1` に設定すると、同時タスク数が適応的に計算されることを示します。この値を `0` に設定すると、コンパクションが無効になります。
- 導入時期：v3.1.0

### `lake_compaction_score_selector_min_score`

- デフォルト：10.0
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターでコンパクション操作をトリガーするコンパクションスコアのしきい値。パーティションのコンパクションスコアがこの値以上の場合、システムはそのパーティションでコンパクションを実行します。
- 導入時期：v3.1.0

### `lake_compaction_score_upper_bound`

- デフォルト：2000
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターのパーティションのコンパクションスコアの上限。`0` は上限がないことを示します。この項目は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効になります。パーティションのコンパクションスコアがこの上限に達するか超えると、受信ロードタスクは拒否されます。v3.3.6 以降、デフォルト値は `0` から `2000` に変更されました。
- 導入時期：v3.2.0

### `lake_compaction_interval_ms_on_success`

- デフォルト: 10000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: 共有データクラスタで、あるパーティションの Compaction が成功した後、そのパーティションで次の Compaction を開始するまでの間隔。エイリアスは `lake_min_compaction_interval_ms_on_success` です。
- 導入時期：v3.2.0

### `lake_enable_balance_tablets_between_workers`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターでクラウドネイティブテーブルのタブレット移行中に、Compute ノード間でタブレットの数をバランスさせるかどうか。`true` は Compute ノード間でタブレットをバランスさせることを示し、`false` はこの機能を無効にすることを示します。
- 導入時期：v3.3.4

### `lake_enable_ingest_slowdown`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターでデータ取り込みの減速を有効にするかどうか。データ取り込みの減速が有効になっている場合、パーティションのコンパクションスコアが `lake_ingest_slowdown_threshold` を超えると、そのパーティションでのロードタスクがスロットルダウンされます。この設定は `run_mode` が `shared_data` に設定されている場合にのみ有効になります。v3.3.6 以降、デフォルト値は `false` から `true` に変更されました。
- 導入時期：v3.2.0

### `lake_ingest_slowdown_threshold`

- デフォルト：100
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターでデータ取り込みの減速をトリガーするコンパクションスコアのしきい値。この設定は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効になります。
- 導入時期：v3.2.0

### `lake_publish_version_max_threads`

- デフォルト：512
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターでのバージョン公開タスクの最大スレッド数。
- 導入時期：v3.2.0

### `meta_sync_force_delete_shard_meta`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：共有データクラスターのメタデータを直接削除し、リモートストレージファイルのクリーンアップをバイパスすることを許可するかどうか。クリーンアップするシャードが過剰に多く、FE JVM のメモリ圧力が極端に高くなる場合にのみ、この項目を `true` に設定することをお勧めします。この機能を有効にすると、シャードまたはタブレットに属するデータファイルが自動的にクリーンアップされないことに注意してください。
- 導入時期：v3.2.10, v3.3.3

### `run_mode`

- デフォルト：`shared_nothing`
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks クラスターの実行モード。有効な値: `shared_data` および `shared_nothing` (デフォルト)。
  - `shared_data` は StarRocks を共有データモードで実行することを示します。
  - `shared_nothing` は StarRocks を共有なしモードで実行することを示します。

  > **CAUTION**
  >
  > - StarRocks クラスターで `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイメントはサポートされていません。
  > - クラスターのデプロイ後に `run_mode` を変更しないでください。そうしないと、クラスターの再起動に失敗します。共有なしクラスターから共有データクラスターへの変換、またはその逆の変換はサポートされていません。

- 導入時期：-

### `shard_group_clean_threshold_sec`

- デフォルト：3600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：FE が共有データクラスターで未使用のタブレットおよびシャードグループをクリーンアップするまでの時間。このしきい値内に作成されたタブレットおよびシャードグループはクリーンアップされません。
- 導入時期：-

### `star_mgr_meta_sync_interval_sec`

- デフォルト：600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：FE が共有データクラスターで StarMgr と定期的なメタデータ同期を実行する間隔。
- 導入時期：-

### `starmgr_grpc_server_max_worker_threads`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE の starmgr モジュールの grpc サーバーが使用するワーカー スレッドの最大数。
- 導入時期：v4.0.0, v3.5.8

### `starmgr_grpc_timeout_seconds`

- デフォルト：5
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- Description:
- 導入時期：-

## データレイク

### `files_enable_insert_push_down_schema`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、アナライザは INSERT ... FROM files() 操作のためにターゲットテーブルスキーマを `files()` テーブル関数にプッシュしようとします。これは、ソースが FileTableFunctionRelation であり、ターゲットがネイティブテーブルであり、SELECT リストにそれに対応するスロット参照列 (または *) が含まれている場合にのみ適用されます。アナライザは選択された列をターゲット列に一致させ (カウントが一致する必要があります)、ターゲットテーブルを一時的にロックし、ファイル列の型を非複合型 (Parquet JSON -> `array<varchar>` のような複合型はスキップされます) のディープコピーされたターゲット列型で置き換えます。元のファイルテーブルからの列名は保持されます。これにより、取り込み中のファイルベースの型推論による型ミスマッチと緩さが軽減されます。
- 導入時期：v3.4.0, v3.5.0

### `hdfs_read_buffer_size_kb`

- デフォルト：8192
- タイプ：Int
- 単位：Kilobytes
- 変更可能：Yes
- 説明：HDFS 読み取りバッファのサイズ (キロバイト単位)。StarRocks はこの値をバイト (`<< 10`) に変換し、`HdfsFsManager` で HDFS 読み取りバッファを初期化したり、ブローカーアクセスが使用されていない場合に BE タスク (例: `TBrokerScanRangeParams`、`TDownloadReq`) に送信される thrift フィールド `hdfs_read_buffer_size_kb` を設定したりするために使用します。`hdfs_read_buffer_size_kb` を増やすと、シーケンシャル読み取りスループットが向上し、システムコールオーバーヘッドが減少しますが、ストリームごとのメモリ使用量が増加します。減らすとメモリフットプリントが減少しますが、I/O 効率が低下する可能性があります。調整時にはワークロード (多くの小さなストリームと少数の大きなシーケンシャル読み取り) を考慮してください。
- 導入時期：v3.2.0

### `hdfs_write_buffer_size_kb`

- デフォルト：1024
- タイプ：Int
- 単位：Kilobytes
- 変更可能：Yes
- 説明：ブローカーを使用しない HDFS またはオブジェクトストレージへの直接書き込みに使用される HDFS 書き込みバッファサイズ (KB 単位) を設定します。FE はこの値をバイト (`<< 10`) に変換し、`HdfsFsManager` でローカル書き込みバッファを初期化します。また、Thrift リクエスト (例: TUploadReq、TExportSink、シンクオプション) に伝播され、バックエンド/エージェントが同じバッファサイズを使用するようにします。この値を増やすと、大きなシーケンシャル書き込みのスループットが向上しますが、ライターごとのメモリが増加します。減らすと、ストリームごとのメモリ使用量が減少し、小さな書き込みのレイテンシーが低下する可能性があります。`hdfs_read_buffer_size_kb` と並行して調整し、利用可能なメモリと同時ライターを考慮してください。
- 導入時期：v3.2.0


### `lake_enable_drop_tablet_cache`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: shared-data モードで、実データが削除される前に BE/CN 上のキャッシュをクリーンアップします。
- 導入バージョン: v4.0



### `lake_enable_drop_tablet_cache`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: shared-data モードで、実データが削除される前に BE/CN 上のキャッシュをクリーンアップします。
- 導入バージョン: v4.0


### `lake_batch_publish_max_version_num`

- デフォルト：10
- タイプ：Int
- 単位：Count
- 変更可能：Yes
- 説明：レイク (クラウドネイティブ) テーブルの公開バッチを構築する際に、連続するトランザクションバージョンをいくつのグループにまとめるかの上限を設定します。この値はトランザクショングラフバッチ処理ルーチン (getReadyToPublishTxnListBatch を参照) に渡され、`lake_batch_publish_min_version_num` と連携して TransactionStateBatch の候補範囲サイズを決定します。値が大きいほど、より多くのコミットをバッチ処理することで公開スループットが向上しますが、アトミック公開の範囲が広がり (可視性レイテンシーが長く、ロールバック対象が大きくなる)、バージョンが連続していない場合、実行時に制限される可能性があります。ワークロードと可視性/レイテンシー要件に応じて調整してください。
- 導入時期：v3.2.0

### `lake_batch_publish_min_version_num`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：レイクテーブルの公開バッチを形成するために必要な連続するトランザクションバージョンの最小数を設定します。DatabaseTransactionMgr.getReadyToPublishTxnListBatch は、依存トランザクションを選択するためにこの値を `lake_batch_publish_max_version_num` とともに transactionGraph.getTxnsWithTxnDependencyBatch に渡します。値が `1` の場合、単一トランザクションの公開が許可されます (バッチ処理なし)。値が `>1` の場合、少なくともその数の連続したバージョンを持つ単一テーブル、非レプリケーショントランザクションが利用可能である必要があります。バージョンが連続していない場合、レプリケーショントランザクションが出現した場合、またはスキーマ変更がバージョンを消費した場合、バッチ処理は中止されます。この値を増やすと、コミットをグループ化することで公開スループットが向上する可能性がありますが、十分な連続トランザクションを待機している間に公開が遅延する可能性があります。
- 導入時期：v3.2.0

### `lake_enable_batch_publish_version`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、PublishVersionDaemon は同じ Lake (共有データ) テーブル/パーティションの準備完了トランザクションをバッチ処理し、トランザクションごとの公開を発行するのではなく、まとめてバージョンを公開します。RunMode shared-data では、デーモンは getReadyPublishTransactionsBatch() を呼び出し、publishVersionForLakeTableBatch(...) を使用してグループ化された公開操作を実行します (RPC を削減し、スループットを向上させます)。無効の場合、デーモンは publishVersionForLakeTable(...) を介してトランザクションごとの公開にフォールバックします。実装は、スイッチが切り替えられたときに重複公開を避けるために内部セットを使用して進行中の作業を調整し、`lake_publish_version_max_threads` を介したスレッドプールサイズ設定の影響を受けます。
- 導入時期：v3.2.0

### `lake_enable_tablet_creation_optimization`

- デフォルト：false
- タイプ：boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、StarRocks は共有データモードのクラウドネイティブテーブルおよびマテリアライズドビューのタブレット作成を最適化し、タブレットごとに個別のメタデータではなく、物理パーティション下のすべてのタブレットに単一の共有タブレットメタデータを作成します。これにより、テーブル作成、ロールアップ、スキーマ変更ジョブ中に作成されるタブレット作成タスクとメタデータ/ファイルの数が削減されます。この最適化はクラウドネイティブテーブル/マテリアライズドビューにのみ適用され、`file_bundling` と組み合わせて使用されます (後者は同じ最適化ロジックを再利用します)。注: スキーマ変更およびロールアップジョブは、同じ名前のファイルが上書きされるのを避けるため、`file_bundling` を使用するテーブルでは明示的に最適化を無効にします。注意して有効にしてください。作成されるタブレットメタデータの粒度が変更され、レプリカ作成とファイル命名の動作に影響する可能性があります。
- 導入時期：v3.3.1, v3.4.0, v3.5.0

### `lake_use_combined_txn_log`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、システムは Lake テーブルが関連トランザクションの結合トランザクションログパスを使用することを許可します。共有データクラスターでのみ利用可能です。
- 導入時期：v3.3.7, v3.4.0, v3.5.0

### `lake_repair_metadata_fetch_max_version_batch_size`

- デフォルト: 160
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタで、タブレット修復時にタブレットメタデータを取得する際のバージョンスキャンの最大バッチサイズ。バッチサイズは 5 から開始し、この最大値に達するまで毎回倍増します。値を大きくすると、1 回のバッチでより多くのバージョンを取得でき、バージョン間のファイル存在キャッシュを活用して修復効率が向上します。5 未満の値が設定された場合、実行時に自動的に 5 に調整されます。
- 導入時期：v3.5.16, v4.0.9

### `enable_iceberg_commit_queue`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：Iceberg テーブルのコミットキューを有効にして、同時コミット競合を回避するかどうか。Iceberg は、メタデータコミットに楽観的並行性制御 (OCC) を使用します。複数のスレッドが同じテーブルに同時にコミットすると、「Cannot commit: Base metadata location is not same as the current table metadata location」のようなエラーで競合が発生する可能性があります。有効にすると、各 Iceberg テーブルにはコミット操作用の単一スレッドエクゼキューターがあり、同じテーブルへのコミットがシリアル化され、OCC 競合が防止されます。異なるテーブルは同時にコミットでき、全体のスループットを維持します。これは信頼性を向上させるためのシステムレベルの最適化であり、デフォルトで有効にする必要があります。無効にすると、楽観的ロック競合により同時コミットが失敗する可能性があります。
- 導入時期：v4.1.0

### `iceberg_commit_queue_timeout_seconds`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：Iceberg コミット操作が完了するまでの待機タイムアウト (秒単位)。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、各コミット操作はこのタイムアウト内で完了する必要があります。コミットがこのタイムアウトよりも長くかかる場合、キャンセルされ、エラーが発生します。コミット時間に影響する要因には、コミットされるデータファイルの数、テーブルのメタデータサイズ、基盤となるストレージ (S3、HDFS など) のパフォーマンスが含まれます。
- 導入時期：v4.1.0

### `iceberg_commit_queue_max_size`

- デフォルト：1000
- タイプ：Int
- 単位：Count
- 変更可能：No
- 説明：Iceberg テーブルごとの保留中のコミット操作の最大数。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、これは単一テーブルのキューに入れられるコミット操作の数を制限します。制限に達すると、追加のコミット操作は呼び出し元のスレッドで実行されます (容量が利用可能になるまでブロックします)。この設定は FE 起動時に読み取られ、新しく作成されたテーブルエクゼキューターに適用されます。有効にするには FE の再起動が必要です。同じテーブルへの同時コミットが多いと予想される場合は、この値を増やしてください。この値が低すぎると、高並行時に呼び出し元スレッドでコミットがブロックされる可能性があります。
- 導入時期：v4.1.0

## その他

### `agent_task_resend_wait_time_ms`

- デフォルト：5000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：FE がエージェントタスクを再送信するまでに待機する必要がある期間。エージェントタスクは、タスク作成時間と現在時刻の間のギャップがこのパラメーターの値を超えた場合にのみ再送信できます。このパラメーターは、エージェントタスクの繰り返し送信を防ぐために使用されます。
- 導入時期：-

### `allow_system_reserved_names`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ユーザーが `__op` と `__row` で始まる名前の列を作成することを許可するかどうか。この機能を有効にするには、このパラメーターを `TRUE` に設定します。これらの名前形式は StarRocks の特殊な目的のために予約されており、そのような列を作成すると未定義の動作になる可能性があるため、この機能はデフォルトで無効になっています。
- 導入時期：v3.2.0

### `auth_token`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE が属する StarRocks クラスター内で ID 認証に使用されるトークン。このパラメーターが指定されていない場合、StarRocks はクラスターのリーダー FE が最初に起動されたときに、クラスターのランダムなトークンを生成します。
- 導入時期：-

### `authentication_ldap_simple_bind_base_dn`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：LDAP サーバーがユーザーの認証情報の検索を開始する基底 DN。
- 導入時期：-

### `authentication_ldap_simple_bind_dn_pattern`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：直接バインド認証用の DN パターン。ユーザー名のプレースホルダーとして `${USER}` を使用します。パターンは有効な LDAP Distinguished Name（DN）を生成する必要があります。`${USER}@domain` のような UPN 形式はサポートされていません。例：`uid=${USER},ou=People,dc=example,dc=com`。複数のパターンをセミコロンで区切ることができ、システムは成功するまで順番に試行します。設定すると検索ステップがスキップされ、構築された DN で直接バインド認証が行われます。

### `authentication_ldap_simple_bind_root_dn`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：ユーザーの認証情報を検索するために使用される管理者 DN。
- 導入時期：-

### `authentication_ldap_simple_bind_root_pwd`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：ユーザーの認証情報を検索するために使用される管理者のパスワード。
- 導入時期：-

### `authentication_ldap_simple_server_host`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：LDAP サーバーが実行されているホスト。
- 導入時期：-

### `authentication_ldap_simple_server_port`

- デフォルト：389
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：LDAP サーバーのポート。
- 導入時期：-

### `authentication_ldap_simple_user_search_attr`

- デフォルト：uid
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：LDAP オブジェクトでユーザーを識別する属性の名前。
- 導入時期：-

### `backup_job_default_timeout_ms`

- デフォルト：86400 * 1000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：バックアップジョブのタイムアウト期間。この値を超えると、バックアップジョブは失敗します。
- 導入時期：-

### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：`SHOW PROC /BACKENDS/{id}` コマンドで各ディスクのタブレット数を収集することを有効にするかどうか。
- 導入時期：v4.0.1, v3.5.8

### `enable_colocate_restore`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：コロケーションテーブルのバックアップと復元を有効にするかどうか。`true` はコロケーションテーブルのバックアップと復元を有効にすることを示し、`false` は無効にすることを示します。
- 導入時期：v3.2.10, v3.3.3

### `enable_external_catalog_information_schema_tables_access_full_metadata`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：外部カタログ（Hive、Iceberg、JDBC など）内のテーブルを解決する際に、`information_schema.tables` が外部メタデータサービスにアクセスすることを許可するかどうかを制御します。`false`（デフォルト）に設定すると、外部テーブルの `TABLE_COMMENT` などの列は空になる場合がありますが、クエリは高速でリモート呼び出しを回避します。`true` に設定すると、FE は対応する外部メタデータサービスに問い合わせ、`TABLE_COMMENT` などのフィールドを埋めることができますが、テーブルごとに追加のレイテンシとリモート呼び出しが発生します。
- 導入時期：-

### `enable_materialized_view_concurrent_prepare`

- デフォルト：true
- タイプ：Boolean
- Unit:
- 変更可能：Yes
- 説明：パフォーマンスを向上させるために、マテリアライズドビューを並行して準備するかどうか。
- 導入時期：v3.4.4

### `enable_metric_calculator`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：メトリックを定期的に収集する機能を有効にするかどうかを指定します。有効な値: `TRUE` および `FALSE`。`TRUE` はこの機能を有効にすることを指定し、`FALSE` はこの機能を無効にすることを指定します。
- 導入時期：-

### `enable_table_metrics_collect`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：FE でテーブルレベルのメトリックをエクスポートするかどうか。無効にすると、FE はテーブルメトリック (テーブルスキャン/ロードカウンターやテーブルサイズメトリックなど) のエクスポートをスキップしますが、カウンターはメモリに記録されます。
- 導入時期：-

### `enable_mv_post_image_reload_cache`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：FE がイメージをロードした後、リロードフラグチェックを実行するかどうか。ベースマテリアライズドビューに対してチェックが実行された場合、それに関連する他のマテリアライズドビューに対しては不要です。
- 導入時期：v3.5.0

### `enable_mv_query_context_cache`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：クエリ書き換えパフォーマンスを向上させるために、クエリレベルのマテリアライズドビュー書き換えキャッシュを有効にするかどうか。
- 導入時期：v3.3

### `enable_mv_refresh_collect_profile`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：すべてのマテリアライズドビューに対して、デフォルトでマテリアライズドビューの更新時にプロファイルを有効にするかどうか。
- 導入時期：v3.3.0

### `enable_mv_refresh_extra_prefix_logging`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：デバッグを容易にするために、ログにマテリアライズドビュー名をプレフィックスとして含めることを有効にするかどうか。
- 導入時期：v3.4.0

### `enable_mv_refresh_query_rewrite`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの更新中にクエリ書き換えを有効にして、クエリパフォーマンスを向上させるために基底テーブルではなく書き換えられた MV を直接使用できるようにするかどうか。
- 導入時期：v3.3

### `enable_trace_historical_node`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：システムが履歴ノードをトレースすることを許可するかどうか。この項目を `true` に設定することで、キャッシュ共有機能を有効にし、エラスティック スケーリング中にシステムが適切なキャッシュノードを選択できるようにすることができます。
- 導入時期：v3.5.1

### `es_state_sync_interval_second`

- デフォルト：10
- タイプ：Long
- 単位：Seconds
- 変更可能：No
- 説明：FE が Elasticsearch インデックスを取得し、StarRocks 外部テーブルのメタデータを同期する時間間隔。
- 導入時期：-

### `hive_meta_cache_refresh_interval_s`

- デフォルト：3600 * 2
- タイプ：Long
- 単位：Seconds
- 変更可能：No
- 説明：Hive 外部テーブルのキャッシュされたメタデータが更新される時間間隔。
- 導入時期：-

### `hive_meta_store_timeout_s`

- デフォルト：10
- タイプ：Long
- 単位：Seconds
- 変更可能：No
- 説明：Hive メタストアへの接続がタイムアウトするまでの時間。
- 導入時期：-

### `jdbc_connection_idle_timeout_ms`

- デフォルト：600000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：No
- 説明：JDBC カタログへのアクセス接続がタイムアウトするまでの最大時間。タイムアウトした接続はアイドル状態と見なされます。
- 導入時期：-

### `jdbc_connection_timeout_ms`

- デフォルト：10000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：No
- 説明：HikariCP コネクションプールが接続を取得するまでのタイムアウト (ミリ秒単位)。この時間内にプールから接続を取得できない場合、操作は失敗します。
- 導入時期：v3.5.13

### `jdbc_query_timeout_ms`

- デフォルト：30000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：JDBC ステートメントのクエリ実行のタイムアウト (ミリ秒単位)。このタイムアウトは、JDBC カタログを通じて実行されるすべての SQL クエリ (パーティションメタデータクエリなど) に適用されます。この値は、JDBC ドライバーに渡されるときに秒に変換されます。
- 導入時期：v3.5.13

### `jdbc_network_timeout_ms`

- デフォルト：30000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：JDBC ネットワーク操作 (ソケット読み取り) のタイムアウト (ミリ秒単位)。このタイムアウトは、外部データベースが応答しない場合に無期限のブロッキングを防ぐために、データベースメタデータ呼び出し (getSchemas()、getTables()、getColumns() など) に適用されます。
- 導入時期：v3.5.13

### `jdbc_connection_max_lifetime_ms`

- デフォルト: 300000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBC接続プール内の接続の最大寿命。このタイムアウト前に接続はリサイクルされ、古い接続を防ぎます。外部データベースの接続タイムアウトよりも短くする必要があります。許可される最小値は30000（30秒）です。
- 導入バージョン: -

### `jdbc_connection_keepalive_time_ms`

- デフォルト: 30000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: アイドル状態のJDBC接続のキープアライブ間隔。アイドル状態の接続はこの間隔でテストされ、古い接続を積極的に検出します。0に設定するとキープアライブプロービングを無効にします。有効な場合、>= 30000かつ`jdbc_connection_max_lifetime_ms`より小さい必要があります。無効な有効値はサイレントに無効化されます（0にリセット）。
- 導入バージョン: -

### `jdbc_connection_leak_detection_threshold_ms`

- デフォルト: 0
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBC接続リーク検出のしきい値。この値よりも長く接続が保持された場合、警告がログに記録されます。無効にするには0に設定します。これは、接続を長時間保持するコードパスを特定するためのデバッグ支援です。
- 導入バージョン: -


### `jdbc_connection_pool_size`

- デフォルト：8
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：JDBC カタログにアクセスするための JDBC 接続プールの最大容量。
- 導入時期：-

### `jdbc_meta_default_cache_enable`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：JDBC カタログのメタデータキャッシュが有効になっているかどうかのデフォルト値。true に設定すると、新しく作成された JDBC カタログはデフォルトでメタデータキャッシュが有効になります。
- 導入時期：-

### `jdbc_meta_default_cache_expire_sec`

- デフォルト：600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：JDBC カタログのメタデータキャッシュのデフォルトの有効期限。`jdbc_meta_default_cache_enable` が true に設定されている場合、新しく作成された JDBC カタログはデフォルトでメタデータキャッシュの有効期限を設定します。
- 導入時期：-

### `jdbc_minimum_idle_connections`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：JDBC カタログにアクセスするための JDBC 接続プールのアイドル接続の最小数。
- 導入時期：-

### `jwt_jwks_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JSON Web Key Set (JWKS) サービスの URL、または `fe/conf` ディレクトリ下の公開鍵ローカルファイルへのパス。
- 導入時期：v3.5.0

### `jwt_principal_field`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は StarRocks へのログインに使用するユーザー名と同一である必要があります。
- 導入時期：v3.5.0

### `jwt_required_audience`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT のオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT のオーディエンスと一致する場合のみです。
- 導入時期：v3.5.0

### `jwt_required_issuer`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT の発行者 (`iss`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT の発行者と一致する場合のみです。
- 導入時期：v3.5.0

### locale

- デフォルト：`zh_CN.UTF-8`
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE が使用する文字セット。
- 導入時期：-

### `max_agent_task_threads_num`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：エージェントタスクスレッドプールで許可されるスレッドの最大数。
- 導入時期：-

### `max_download_task_per_be`

- デフォルト：0
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：各 RESTORE 操作において、StarRocks が BE ノードに割り当てるダウンロードタスクの最大数。この項目が 0 以下に設定されている場合、タスク数に制限は課されません。
- 導入時期：v3.1.0

### `max_mv_check_base_table_change_retry_times`

- デフォルト：10
- タイプ：-
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの更新時に、基底テーブルの変更を検出するための最大再試行回数。
- 導入時期：v3.3.0

### `max_mv_refresh_failure_retry_times`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの更新が失敗した場合の最大再試行回数。
- 導入時期：v3.3.0

### `max_mv_refresh_try_lock_failure_retry_times`

- デフォルト：3
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの更新が失敗した場合の、ロック試行の最大再試行回数。
- 導入時期：v3.3.0

### `max_small_file_number`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE ディレクトリに格納できる小型ファイルの最大数。
- 導入時期：-

### `max_small_file_size_bytes`

- デフォルト：1024 * 1024
- タイプ：Int
- 単位：Bytes
- 変更可能：Yes
- 説明：小型ファイルの最大サイズ。
- 導入時期：-

### `max_upload_task_per_be`

- デフォルト：0
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：各 BACKUP 操作において、StarRocks が BE ノードに割り当てるアップロードタスクの最大数。この項目が 0 以下に設定されている場合、タスク数に制限は課されません。
- 導入時期：v3.1.0

### `mv_create_partition_batch_interval_ms`

- デフォルト：1000
- タイプ：Int
- 単位：ms
- 変更可能：Yes
- 説明：マテリアライズドビューの更新中、複数のパーティションを一括作成する必要がある場合、システムはそれらをそれぞれ 64 パーティションのバッチに分割します。頻繁なパーティション作成による障害のリスクを軽減するため、作成頻度を制御するために各バッチ間にデフォルトの間隔 (ミリ秒単位) が設定されます。
- 導入時期：v3.3

### `mv_plan_cache_max_size`

- デフォルト：1000
- タイプ：Long
- Unit:
- 変更可能：Yes
- 説明：マテリアライズドビュー計画キャッシュの最大サイズ (マテリアライズドビューの書き換えに使用されます)。透過的クエリ書き換えに多くのマテリアライズドビューが使用されている場合、この値を増やすことができます。
- 導入時期：v3.2

### `mv_plan_cache_thread_pool_size`

- デフォルト：3
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビュー計画キャッシュのデフォルトスレッドプールサイズ (マテリアライズドビュー書き換えに使用されます)。
- 導入時期：v3.2

### `mv_refresh_default_planner_optimize_timeout`

- デフォルト：30000
- タイプ：-
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビュー更新時のオプティマイザの計画フェーズのデフォルトタイムアウト。
- 導入時期：v3.3.0

### `mv_refresh_fail_on_filter_data`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：更新時にフィルタリングされたデータがある場合、マテリアライズドビューの更新は失敗します (デフォルトは true)。そうでない場合、フィルタリングされたデータを無視して成功を返します。
- 導入時期：-

### `mv_refresh_try_lock_timeout_ms`

- デフォルト：30000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：マテリアライズドビューの更新が、基底テーブル/マテリアライズドビューの DB ロックを試行するデフォルトの試行ロックタイムアウト。
- 導入時期：v3.3.0

### `oauth2_auth_server_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：認証 URL。OAuth 2.0 認証プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。
- 導入時期：v3.5.0

### `oauth2_client_id`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks クライアントの公開識別子。
- 導入時期：v3.5.0

### `oauth2_client_secret`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks クライアントを認証サーバーで認証するために使用されるシークレット。
- 導入時期：v3.5.0

### `oauth2_jwks_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JSON Web Key Set (JWKS) サービスの URL、または `conf` ディレクトリ下のローカルファイルへのパス。
- 導入時期：v3.5.0

### `oauth2_principal_field`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は StarRocks へのログインに使用するユーザー名と同一である必要があります。
- 導入時期：v3.5.0

### `oauth2_redirect_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。認証コードはこの URL に送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として設定する必要があります。
- 導入時期：v3.5.0

### `oauth2_required_audience`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT のオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT のオーディエンスと一致する場合のみです。
- 導入時期：v3.5.0

### `oauth2_required_issuer`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：JWT の発行者 (`iss`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT の発行者と一致する場合のみです。
- 導入時期：v3.5.0

### `oauth2_token_server_url`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks がアクセストークンを取得する認証サーバーのエンドポイントの URL。
- 導入時期：v3.5.0

### `plugin_dir`

- デフォルト：`System.getenv("STARROCKS_HOME")` + "/plugins"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：プラグインインストールパッケージを格納するディレクトリ。
- 導入時期：-

### `plugin_enable`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：プラグインを FE にインストールできるかどうか。プラグインはリーダー FE にのみインストールまたはアンインストールできます。
- 導入時期：-

### `proc_profile_jstack_depth`

- デフォルト：128
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：システムが CPU およびメモリプロファイルを収集する際の最大 Java スタック深度。この値は、サンプリングされた各スタックについていくつの Java スタックフレームがキャプチャされるかを制御します。値が大きいほどトレースの詳細と出力サイズが増加し、プロファイリングのオーバーヘッドが追加される可能性があります。値が小さいほど詳細は減少します。この設定は CPU およびメモリプロファイリングの両方でプロファイラーが起動されるときに使用されるため、診断のニーズとパフォーマンスへの影響のバランスをとるように調整してください。
- 導入時期：-

### `proc_profile_mem_enable`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：プロセスメモリ割り当てプロファイルの収集を有効にするかどうか。この項目が `true` に設定されている場合、システムは `sys_log_dir/proc_profile` に `mem-profile-<timestamp>.html` という名前の HTML プロファイルを生成し、`proc_profile_collect_time_s` 秒間サンプリングしながらスリープし、Java スタック深度に `proc_profile_jstack_depth` を使用します。生成されたファイルは `proc_profile_file_retained_days` および `proc_profile_file_retained_size_bytes` に従って圧縮され、パージされます。ネイティブ抽出パスは `/tmp` の noexec 問題を回避するために `STARROCKS_HOME_DIR` を使用します。この項目はメモリ割り当てホットスポットのトラブルシューティングを目的としています。これを有効にすると CPU、I/O、ディスク使用量が増加し、大きなファイルが生成される可能性があります。
- 導入時期：v3.2.12

### `query_detail_explain_level`

- デフォルト：COSTS
- タイプ：String
- 単位：-
- 変更可能：true
- 説明：EXPLAIN ステートメントによって返されるクエリ計画の詳細レベル。有効な値: COSTS, NORMAL, VERBOSE。
- 導入時期：v3.2.12, v3.3.5

### `replication_interval_ms`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：レプリケーションタスクがスケジュールされる最小時間間隔。
- 導入時期：v3.3.5

### `replication_max_parallel_data_size_mb`

- デフォルト：1048576
- タイプ：Int
- 単位：MB
- 変更可能：Yes
- 説明：同時同期に許可されるデータの最大サイズ。
- 導入時期：v3.3.5

### `replication_max_parallel_replica_count`

- デフォルト：10240
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：同時同期に許可されるタブレットレプリカの最大数。
- 導入時期：v3.3.5

### `replication_max_parallel_table_count`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：許可される同時データ同期タスクの最大数。StarRocks はテーブルごとに 1 つの同期タスクを作成します。
- 導入時期：v3.3.5

### `replication_transaction_timeout_sec`

- デフォルト：86400
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：同期タスクのタイムアウト期間。
- 導入時期：v3.3.5

### `skip_whole_phase_lock_mv_limit`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：関連するマテリアライズドビューを持つテーブルに「非ロック」最適化を StarRocks がいつ適用するかを制御します。この項目
