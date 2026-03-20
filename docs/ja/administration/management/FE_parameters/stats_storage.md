---
displayed_sidebar: docs
sidebar_label: "統計とストレージ"
---

# FE 設定 - 統計とストレージ

このトピックでは、以下の種類のFE構成について紹介します：
- [統計レポート](#統計レポート)
- [ストレージ](#ストレージ)

## 統計レポート

### `enable_collect_warehouse_metrics`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、システムはウェアハウスごとのメトリックを収集してエクスポートします。これを有効にすると、ウェアハウスレベルのメトリック (スロット/使用量/可用性) がメトリック出力に追加され、メトリックのカーディナリティと収集オーバーヘッドが増加します。ウェアハウス固有のメトリックを省略し、CPU/ネットワークと監視ストレージコストを削減するには無効にします。
- 導入時期：v3.5.0

### `enable_http_detail_metrics`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：true の場合、HTTP サーバーは詳細な HTTP ワーカーメトリック (特に `HTTP_WORKER_PENDING_TASKS_NUM` ゲージ) を計算して公開します。これを有効にすると、サーバーは Netty ワーカーエクゼキューターを反復処理し、各 `NioEventLoop` で `pendingTasks()` を呼び出して保留中のタスクカウントを合計します。無効の場合、ゲージはコストを回避するために 0 を返します。この追加の収集は CPU およびレイテンシーに敏感である可能性があるため、デバッグまたは詳細な調査のためにのみ有効にしてください。
- 導入時期：v3.2.3

### `proc_profile_collect_time_s`

- デフォルト：120
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：単一プロセスプロファイル収集の期間 (秒単位)。`proc_profile_cpu_enable` または `proc_profile_mem_enable` が `true` に設定されている場合、AsyncProfiler が起動し、コレクタースレッドはこの期間だけスリープし、その後プロファイラーが停止してプロファイルが書き込まれます。値が大きいほどサンプルカバレッジとファイルサイズは増加しますが、プロファイラーの実行時間が長くなり、その後の収集が遅れます。値が小さいほどオーバーヘッドは減少しますが、不十分なサンプルが生成される可能性があります。`proc_profile_file_retained_days` や `proc_profile_file_retained_size_bytes` などの保持設定とこの値が一致していることを確認してください。
- 導入時期：v3.2.12

## ストレージ

### `alter_table_timeout_second`

- デフォルト：86400
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：スキーマ変更操作 (ALTER TABLE) のタイムアウト期間。
- 導入時期：-

### `capacity_used_percent_high_water`

- デフォルト：0.75
- タイプ：double
- 単位：Fraction (0.0–1.0)
- 変更可能：Yes
- 説明：バックエンド負荷スコアを計算する際に使用されるディスク容量使用率 (総容量に対する割合) の高水位しきい値。`BackendLoadStatistic.calcSore` は `capacity_used_percent_high_water` を使用して `LoadScore.capacityCoefficient` を設定します。バックエンドの使用率が 0.5 未満の場合、係数は 0.5 になります。使用率が `capacity_used_percent_high_water` を超える場合、係数は 1.0 になります。それ以外の場合、係数は使用率に応じて線形に推移します (2 * usedPercent - 0.5)。係数が 1.0 の場合、負荷スコアは完全に容量比率によって決定されます。値が低いほどレプリカ数の重みが増加します。この値を調整すると、バランサーがディスク使用率の高いバックエンドにペナルティを課す積極性が変わります。
- 導入時期：v3.2.0

### `catalog_trash_expire_second`

- デフォルト：86400
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：データベース、テーブル、またはパーティションが削除された後にメタデータが保持される最長期間。この期間が経過すると、データは削除され、RECOVER コマンドで回復することはできません。
- 導入時期：-

### `catalog_recycle_bin_erase_min_latency_ms`

- デフォルト：600000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：データベース、テーブル、またはパーティションが削除された際にメタデータが消去されるまでの最小遅延時間（ミリ秒単位）。これにより、削除ログよりも先に消去ログが書き込まれることを回避します。
- 導入時期：-

### `catalog_recycle_bin_erase_max_operations_per_cycle`

- デフォルト：500
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：リサイクルビンからデータベース、テーブル、またはパーティションを実際に削除する操作における、1 サイクルあたりの最大消去回数。消去操作はロックを保持するため、1 バッチのサイズが大きくなりすぎないようにしてください。
- 導入時期：-

### `catalog_recycle_bin_erase_fail_retry_interval_ms`

- デフォルト：60000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：リサイクルビン内の削除操作が失敗した場合の再試行間隔（ミリ秒単位）。
- 導入時期：-

### `check_consistency_default_timeout_second`

- デフォルト：600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：レプリカの一貫性チェックのタイムアウト期間。タブレットのサイズに基づいてこのパラメーターを設定できます。
- 導入時期：-

### `consistency_check_cooldown_time_second`

- デフォルト：24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：同じタブレットの一貫性チェック間で必要な最小間隔 (秒単位) を制御します。タブレット選択中、タブレットは `tablet.getLastCheckTime()` が `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` 未満の場合にのみ適格と見なされます。デフォルト値 (24 * 3600) は、バックエンドのディスク I/O を減らすために、タブレットごとに約 1 日に 1 回のチェックを強制します。この値を減らすとチェック頻度とリソース使用量が増加し、増やすと不整合の検出が遅れる代わりに I/O が減少します。この値は、インデックスのタブレットリストからクールダウンしたタブレットをフィルタリングする際にグローバルに適用されます。
- 導入時期：v3.5.5

### `consistency_check_end_time`

- デフォルト："4"
- タイプ：String
- 単位：Hour of day (0-23)
- 変更可能：No
- 説明：ConsistencyChecker の作業ウィンドウの終了時間 (時、0-23) を指定します。値はシステムタイムゾーンの SimpleDateFormat("HH") で解析され、0-23 (1 桁または 2 桁) として受け入れられます。StarRocks はこれを `consistency_check_start_time` とともに使用して、一貫性チェックジョブをスケジュールして追加する時期を決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます (たとえば、デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4")。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると FE の起動がエラーをログに記録して終了するため、有効な時刻文字列を指定してください。
- 導入時期：v3.2.0

### `consistency_check_start_time`

- デフォルト："23"
- タイプ：String
- 単位：Hour of day (00-23)
- 変更可能：No
- 説明：ConsistencyChecker の作業ウィンドウの開始時間 (時、00-23) を指定します。値はシステムタイムゾーンの SimpleDateFormat("HH") で解析され、0-23 (1 桁または 2 桁) として受け入れられます。StarRocks はこれを `consistency_check_end_time` とともに使用して、一貫性チェックジョブをスケジュールして追加する時期を決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます (たとえば、デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4")。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると FE の起動がエラーをログに記録して終了するため、有効な時刻文字列を指定してください。
- 導入時期：v3.2.0

### `consistency_tablet_meta_check_interval_ms`

- デフォルト：2 * 3600 * 1000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：ConsistencyChecker が `TabletInvertedIndex` と `LocalMetastore` の間の完全なタブレットメタの一貫性スキャンを実行するために使用する間隔 (ミリ秒)。`runAfterCatalogReady` のデーモンは、`現在の時間 - lastTabletMetaCheckTime` がこの値を超えたときに checkTabletMetaConsistency をトリガーします。無効なタブレットが最初に検出された場合、その `toBeCleanedTime` は `now + (consistency_tablet_meta_check_interval_ms / 2)` に設定されるため、実際の削除は後続のスキャンまで遅延されます。この値を増やすとスキャン頻度と負荷が減少し (クリーンアップが遅くなる)、減らすと古いタブレットの検出と削除が高速化されます (オーバーヘッドが増加する)。
- 導入時期：v3.2.0

### `default_replication_num`

- デフォルト：3
- タイプ：Short
- 単位：-
- 変更可能：Yes
- 説明：StarRocks でテーブルを作成する際に、各データパーティションのレプリカのデフォルト数を設定します。この設定は、CREATE TABLE DDL で `replication_num=x` を指定することで、テーブル作成時に上書きできます。
- 導入時期：-

### `enable_auto_tablet_distribution`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：バケット数を自動的に設定するかどうか。
  - このパラメーターが `TRUE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を指定する必要はありません。StarRocks は自動的にバケット数を決定します。
  - このパラメーターが `FALSE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を手動で指定する必要があります。テーブルに新しいパーティションを追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションにバケット数を手動で指定することもできます。
- 導入時期：v2.5.7

### `enable_experimental_rowstore`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ハイブリッド行指向・列指向ストレージ機能を有効にするかどうか。
- 導入時期：v3.2.3

### `enable_fast_schema_evolution`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：StarRocks クラスター内のすべてのテーブルでFast Schema Evolutionを有効にするかどうか。有効な値は `TRUE` と `FALSE` (デフォルト) です。Fast Schema Evolutionを有効にすると、スキーマ変更の速度が向上し、列の追加または削除時のリソース使用量が削減されます。
- 導入時期：v3.2.0

> **NOTE**
>
> - StarRocks Shared-data クラスターは v3.3.0 以降でこのパラメーターをサポートしています。
> - 特定のテーブルのFast Schema Evolutionを設定する必要がある場合 (特定のテーブルのFast Schema Evolutionを無効にするなど) は、テーブル作成時にテーブルプロパティ `fast_schema_evolution` を設定できます。

### `enable_online_optimize_table`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：最適化ジョブ作成時に StarRocks が非ブロッキングのオンライン最適化パスを使用するかどうかを制御します。`enable_online_optimize_table` が true で、ターゲットテーブルが互換性チェックを満たす場合 (パーティション/キー/ソート指定なし、分散が `RandomDistributionDesc` でない、ストレージタイプが `COLUMN_WITH_ROW` でない、レプリケートストレージが有効、テーブルがクラウドネイティブテーブルまたはマテリアライズドビューでない)、プランナーは書き込みをブロックせずに最適化を実行するために `OnlineOptimizeJobV2` を作成します。false の場合、または互換性条件が失敗した場合、StarRocks は `OptimizeJobV2` にフォールバックします。これは、最適化中に書き込み操作をブロックする可能性があります。
- 導入時期：v3.3.3, v3.4.0, v3.5.0

### `enable_strict_storage_medium_check`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ユーザーがテーブルを作成する際に、FE が BE のストレージメディアを厳密にチェックするかどうか。このパラメーターが `TRUE` に設定されている場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックし、BE のストレージメディアが CREATE TABLE ステートメントで指定された `storage_medium` パラメーターと異なる場合、エラーを返します。例えば、CREATE TABLE ステートメントで指定されたストレージメディアが SSD であるのに、BE の実際のストレージメディアが HDD である場合、テーブル作成は失敗します。このパラメーターが `FALSE` の場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックしません。
- 導入時期：-

### `max_bucket_number_per_partition`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：パーティション内に作成できるバケットの最大数。
- 導入時期：v3.3.2

### `max_column_number_per_table`

- デフォルト：10000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：テーブル内に作成できる列の最大数。
- 導入時期：v3.3.2

### `max_dynamic_partition_num`

- デフォルト：500
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：動的パーティションテーブルの分析または作成時に一度に作成できる最大パーティション数を制限します。動的パーティションプロパティ検証中、`systemtask_runs_max_history_number` は予想されるパーティション (終了オフセット + 履歴パーティション番号) を計算し、合計が `max_dynamic_partition_num` を超える場合、DDL エラーをスローします。正当に大きなパーティション範囲を予想する場合にのみこの値を増やしてください。増やすとより多くのパーティションを作成できますが、メタデータサイズ、スケジューリング作業、および運用上の複雑さが増加する可能性があります。
- 導入時期：v3.2.0

### `max_partition_number_per_table`

- デフォルト：100000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：テーブル内に作成できるパーティションの最大数。
- 導入時期：v3.3.2

### `max_task_consecutive_fail_count`

- デフォルト：10
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：タスクが自動的に停止されるまでに発生する可能性のある連続失敗の最大数。`TaskSource.MV.equals(task.getSource())` が `true` で `max_task_consecutive_fail_count` が 0 より大きい場合、タスクの連続失敗カウンターが `max_task_consecutive_fail_count` に達するか超えると、タスクは TaskManager を介して停止され、マテリアライズドビュータスクの場合はマテリアライズドビューが非アクティブ化されます。停止と再アクティブ化の方法を示す例外がスローされます (例: `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`)。自動停止を無効にするには、この項目を 0 または負の値に設定します。
- 導入時期：-

### `partition_recycle_retention_period_secs`

- デフォルト：1800
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：INSERT OVERWRITE またはマテリアライズドビュー更新操作によって削除されたパーティションのメタデータ保持時間。このようなメタデータは、RECOVER の実行によって回復できないことに注意してください。
- 導入時期：v3.5.9

### `recover_with_empty_tablet`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：失われたまたは破損したタブレットレプリカを空のレプリカに置き換えるかどうか。タブレットレプリカが失われたり破損したりした場合、このタブレットまたは他の健全なタブレットのデータクエリは失敗する可能性があります。失われたまたは破損したタブレットレプリカを空のタブレットに置き換えることで、クエリは引き続き実行できます。ただし、データが失われているため、結果が正しくない可能性があります。デフォルト値は `FALSE` で、失われたまたは破損したタブレットレプリカは空のレプリカに置き換えられず、クエリは失敗します。
- 導入時期：-

### `storage_usage_hard_limit_percent`

- デフォルト：95
- Alias: `storage_flood_stage_usage_percent`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：BE ディレクトリのストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_usage_hard_limit_reserve_bytes` 未満の場合、ロードおよび復元ジョブは拒否されます。この設定を有効にするには、BE 設定項目 `storage_flood_stage_usage_percent` と合わせてこの項目を設定する必要があります。
- 導入時期：-

### `storage_usage_hard_limit_reserve_bytes`

- デフォルト：100 * 1024 * 1024 * 1024
- Alias: `storage_flood_stage_left_capacity_bytes`
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：BE ディレクトリの残りストレージスペースのハードリミット。BE ストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が `storage_usage_hard_limit_percent` を超える場合、ロードおよび復元ジョブは拒否されます。この設定を有効にするには、BE 設定項目 `storage_flood_stage_left_capacity_bytes` と合わせてこの項目を設定する必要があります。
- 導入時期：-

### `storage_usage_soft_limit_percent`

- デフォルト：90
- Alias: `storage_high_watermark_usage_percent`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：BE ディレクトリのストレージ使用率のソフトリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_usage_soft_limit_reserve_bytes` 未満の場合、タブレットは このディレクトリにクローンできません。
- 導入時期：-

### `storage_usage_soft_limit_reserve_bytes`

- デフォルト：200 * 1024 * 1024 * 1024
- Alias: `storage_min_left_capacity_bytes`
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：BE ディレクトリの残りストレージスペースのソフトリミット。BE ストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が `storage_usage_soft_limit_percent` を超える場合、タブレットはこのディレクトリにクローンできません。
- 導入時期：-

### `tablet_checker_lock_time_per_cycle_ms`

- デフォルト：1000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：テーブルロックを解放して再取得するまでの、タブレットチェッカーがサイクルごとにロックを保持する最大時間。100 未満の値は 100 として扱われます。
- 導入時期：v3.5.9, v4.0.2

### `tablet_create_timeout_second`

- デフォルト：10
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：タブレット作成のタイムアウト期間。v3.1 以降、デフォルト値は 1 から 10 に変更されました。
- 導入時期：-

### `tablet_delete_timeout_second`

- デフォルト：2
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：タブレット削除のタイムアウト期間。
- 導入時期：-

### `tablet_sched_balance_load_disk_safe_threshold`

- デフォルト：0.5
- Alias: `balance_load_disk_safe_threshold`
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：BE のディスク使用量がバランスしているかどうかを判断するためのパーセンテージしきい値。すべての BE のディスク使用量がこの値よりも低い場合、バランスしていると見なされます。ディスク使用量がこの値よりも大きく、最高と最低の BE ディスク使用量の差が 10% を超える場合、ディスク使用量はバランスしていないと見なされ、タブレットの再バランシングがトリガーされます。
- 導入時期：-

### `tablet_sched_balance_load_score_threshold`

- デフォルト：0.1
- Alias: `balance_load_score_threshold`
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：BE の負荷がバランスしているかどうかを判断するためのパーセンテージしきい値。BE の負荷がすべての BE の平均負荷よりも低く、その差がこの値よりも大きい場合、その BE は低負荷状態にあります。逆に、BE の負荷が平均負荷よりも高く、その差がこの値よりも大きい場合、その BE は高負荷状態にあります。
- 導入時期：-

### `tablet_sched_be_down_tolerate_time_s`

- デフォルト：900
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：スケジューラが BE ノードを非アクティブのままにする最大期間。この時間しきい値に達すると、その BE ノード上のタブレットは他のアクティブな BE ノードに移行されます。
- 導入時期：v2.5.7

### `tablet_sched_disable_balance`

- デフォルト：false
- Alias: `disable_balance`
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：タブレットバランシングを無効にするかどうか。`TRUE` はタブレットバランシングが無効になっていることを示します。`FALSE` はタブレットバランシングが有効になっていることを示します。
- 導入時期：-

### `tablet_sched_disable_colocate_balance`

- デフォルト：false
- Alias: `disable_colocate_balance`
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：コロケーションテーブルのレプリカバランシングを無効にするかどうか。`TRUE` はレプリカバランシングが無効になっていることを示します。`FALSE` はレプリカバランシングが有効になっていることを示します。
- 導入時期：-

### `tablet_sched_max_balancing_tablets`

- デフォルト：500
- Alias: `max_balancing_tablets`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：同時にバランスできるタブレットの最大数。この値を超えると、タブレットの再バランシングはスキップされます。
- 導入時期：-

### `tablet_sched_max_clone_task_timeout_sec`

- デフォルト：2 * 60 * 60
- Alias: `max_clone_task_timeout_sec`
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：タブレットのクローン作成の最大タイムアウト期間。
- 導入時期：-

### `tablet_sched_max_not_being_scheduled_interval_ms`

- デフォルト：15 * 60 * 1000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：タブレットクローンタスクがスケジュールされている場合、このパラメーターで指定された時間の間タブレットがスケジュールされていない場合、StarRocks はそのタブレットに高い優先度を与え、できるだけ早くスケジュールします。
- 導入時期：-

### `tablet_sched_max_scheduling_tablets`

- デフォルト：10000
- Alias: `max_scheduling_tablets`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：同時にスケジュールできるタブレットの最大数。この値を超えると、タブレットのバランシングと修復チェックはスキップされます。
- 導入時期：-

### `tablet_sched_min_clone_task_timeout_sec`

- デフォルト：3 * 60
- Alias: `min_clone_task_timeout_sec`
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：タブレットのクローン作成の最小タイムアウト期間。
- 導入時期：-

### `tablet_sched_num_based_balance_threshold_ratio`

- デフォルト：0.5
- Alias: -
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：数ベースのバランシングはディスクサイズバランスを損なう可能性がありますが、ディスク間の最大ギャップは `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold` を超えることはできません。クラスター内に A から B へ、そして B から A へと常にバランシングしているタブレットがある場合、この値を減らしてください。タブレットの分散をよりバランスさせたい場合、この値を増やしてください。
- 導入時期：- 3.1

### `tablet_sched_repair_delay_factor_second`

- デフォルト：60
- Alias: `tablet_repair_delay_factor_second`
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：レプリカが修復される間隔 (秒単位)。
- 導入時期：-

### `tablet_sched_slot_num_per_path`

- デフォルト：8
- Alias: `schedule_slot_num_per_path`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：BE ストレージディレクトリで同時に実行できるタブレット関連タスクの最大数。v2.5 以降、このパラメーターのデフォルト値は `4` から `8` に変更されました。
- 導入時期：-

### `tablet_sched_storage_cooldown_second`

- デフォルト：-1
- Alias: `storage_cooldown_second`
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：テーブル作成時からの自動冷却の遅延時間。デフォルト値 `-1` は自動冷却が無効になっていることを指定します。自動冷却を有効にするには、このパラメーターを `-1` より大きい値に設定します。
- 導入時期：-

### `tablet_stat_update_interval_second`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：FE が各 BE からタブレット統計を取得する時間間隔。
- 導入時期：-

### `enable_range_distribution`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：テーブル作成時に Range-based Distribution セマンティクスを有効化するかどうか。
- 導入時期：v4.1.0

### `tablet_reshard_max_parallel_tablets`

- デフォルト：10240
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：並行して分割または結合できるタブレット数の最大値。
- 導入時期：v4.1.0

### `tablet_reshard_target_size`

- デフォルト：1073741824 (1 GB)
- タイプ：Int
- 単位：Bytes
- 変更可能：Yes
- 説明：SPLIT または MERGE 操作後のテーブルのターゲットサイズ。
- 導入時期：v4.1.0

### `tablet_reshard_max_split_count`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：古いタブレットを分割できる新しいタブレットの最大数。
- 導入時期：v4.1.0

### `tablet_reshard_history_job_max_keep_ms`

- デフォルト：259200000 (72 hours)
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：過去のタブレット SPLIT/MERGE ジョブの最大保持期間。
- 導入時期：v4.1.0
