---
displayed_sidebar: docs
sidebar_label: "認証、クエリ、およびロード"
---

# FE 設定 - 認証、クエリ、およびロード

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
- [ユーザー、ロール、権限](#ユーザーロール権限)
- [クエリ](#クエリエンジン)
- [ロードとアンロード](#ロードとアンロード)

## ユーザー、ロール、権限

### `enable_task_info_mask_credential`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：true の場合、StarRocks は `information_schema.tasks` および `information_schema.task_runs` で返される前に、タスク SQL 定義から資格情報を編集します。これは、DEFINITION 列に SqlCredentialRedactor.redact を適用することで行われます。`information_schema.task_runs` では、定義がタスク実行ステータスから来るか、空の場合にタスク定義ルックアップから来るかに関係なく、同じ編集が適用されます。false の場合、生のタスク定義が返されます (資格情報が公開される可能性があります)。マスキングは CPU/文字列処理作業であり、タスクまたは `task_runs` の数が大きい場合は時間がかかる場合があります。非編集定義が必要であり、セキュリティリスクを受け入れる場合にのみ無効にしてください。
- 導入時期：v3.5.6

### `privilege_max_role_depth`

- デフォルト：16
- タイプ：Int
- Unit:
- 変更可能：Yes
- 説明：ロールの最大ロール深度 (継承レベル)。
- 導入時期：v3.0.0

### `privilege_max_total_roles_per_user`

- デフォルト：64
- タイプ：Int
- Unit:
- 変更可能：Yes
- 説明：ユーザーが持つことができるロールの最大数。
- 導入時期：v3.0.0

## クエリエンジン

### `brpc_send_plan_fragment_timeout_ms`

- デフォルト：60000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：プランフラグメントを送信する前に BRPC TalkTimeoutController に適用されるタイムアウト (ミリ秒単位)。`BackendServiceClient.sendPlanFragmentAsync` は、バックエンド `execPlanFragmentAsync` を呼び出す前にこの値を設定します。これは、BRPC がアイドル接続を接続プールから借りる際や送信を実行する際に待機する期間を管理します。超過した場合、RPC は失敗し、メソッドの再試行ロジックをトリガーする可能性があります。競合時に迅速に失敗させるにはこれを低く設定し、一時的なプール枯渇や低速ネットワークを許容するには高く設定します。注意: 非常に大きな値は、失敗検出を遅延させ、要求スレッドをブロックする可能性があります。
- 導入時期：v3.3.11, v3.4.1, v3.5.0

### `connector_table_query_trigger_analyze_large_table_interval`

- デフォルト：12 * 3600
- タイプ：Int
- 単位：Second
- 変更可能：Yes
- 説明：大規模テーブルのクエリトリガー ANALYZE タスクの間隔。
- 導入時期：v3.4.0

### `connector_table_query_trigger_analyze_max_pending_task_num`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE で保留状態にあるクエリトリガー ANALYZE タスクの最大数。
- 導入時期：v3.4.0

### `connector_table_query_trigger_analyze_max_running_task_num`

- デフォルト：2
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE で実行状態にあるクエリトリガー ANALYZE タスクの最大数。
- 導入時期：v3.4.0

### `connector_table_query_trigger_analyze_small_table_interval`

- デフォルト：2 * 3600
- タイプ：Int
- 単位：Second
- 変更可能：Yes
- 説明：小規模テーブルのクエリトリガー ANALYZE タスクの間隔。
- 導入時期：v3.4.0

### `connector_table_query_trigger_analyze_small_table_rows`

- デフォルト：10000000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：クエリトリガー ANALYZE タスクのテーブルが小規模テーブルであるかどうかを判断するためのしきい値。
- 導入時期：v3.4.0

### `connector_table_query_trigger_task_schedule_interval`

- デフォルト：30
- タイプ：Int
- 単位：Second
- 変更可能：Yes
- 説明：スケジューラースレッドがクエリトリガーバックグラウンドタスクをスケジュールする間隔。この項目は v3.4.0 で導入された `connector_table_query_trigger_analyze_schedule_interval` を置き換えるものです。ここで、バックグラウンドタスクとは v3.4 の `ANALYZE` タスクと、v3.4 以降の低カーディナリティ列の辞書収集タスクを指します。
- 導入時期：v3.4.2

### `create_table_max_serial_replicas`

- デフォルト：128
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：シリアルに作成するレプリカの最大数。実際のレプリカ数がこの値を超えると、レプリカは並行して作成されます。テーブル作成に時間がかかりすぎる場合は、この値を減らしてみてください。
- 導入時期：-

### `default_mv_partition_refresh_number`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの更新が複数のパーティションを伴う場合、このパラメーターは、デフォルトで 1 つのバッチで更新されるパーティションの数を制御します。
バージョン 3.3.0 以降、システムはデフォルトで一度に 1 つのパーティションを更新して、潜在的なメモリ不足 (OOM) の問題を回避します。以前のバージョンでは、デフォルトですべてのパーティションが一度に更新され、メモリ枯渇やタスク障害につながる可能性がありました。ただし、マテリアライズドビューの更新が多数のパーティションを伴う場合、一度に 1 つのパーティションのみを更新すると、スケジューリングのオーバーヘッドが過剰になり、全体の更新時間が長くなり、大量の更新レコードが生成される可能性があることに注意してください。そのような場合は、更新効率を向上させ、スケジューリングコストを削減するために、このパラメーターを適切に調整することをお勧めします。
- 導入時期：v3.3.0

### `default_mv_refresh_immediate`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：非同期マテリアライズドビューの作成後すぐに更新するかどうか。この項目が `true` に設定されている場合、新しく作成されたマテリアライズドビューはすぐに更新されます。
- 導入時期：v3.2.3

### `dynamic_partition_check_interval_seconds`

- デフォルト：600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：新しいデータがチェックされる間隔。新しいデータが検出された場合、StarRocks は自動的にデータ用のパーティションを作成します。
- 導入時期：-

### `dynamic_partition_enable`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：動的パーティショニング機能を有効にするかどうか。この機能が有効になっている場合、StarRocks は新しいデータ用のパーティションを動的に作成し、期限切れのパーティションを自動的に削除してデータの鮮度を保証します。
- 導入時期：-

### `enable_active_materialized_view_schema_strict_check`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：非アクティブなマテリアライズドビューをアクティブ化するときに、データ型の長さの一貫性を厳密にチェックするかどうか。この項目が `false` に設定されている場合、基底テーブルでデータ型の長さが変更されても、マテリアライズドビューのアクティブ化は影響を受けません。
- 導入時期：v3.3.4

### `mv_fast_schema_change_mode`

- デフォルト: strict
- タイプ: String
- 単位: -
- 可変: はい
- 説明: マテリアライズドビュー (MV) の高速スキーマ進化 (FSE) の動作を制御します。有効な値は次のとおりです: `strict` (デフォルト) - `isSupportFastSchemaEvolutionInDanger` が true の場合にのみ FSE を許可し、影響を受けるパーティションエントリをバージョンマップからクリアします。 `force` - `isSupportFastSchemaEvolutionInDanger` が false の場合でも FSE を許可し、影響を受けるパーティションエントリをクリアしてリフレッシュ時に再計算をトリガーします。 `force_no_clear` - `isSupportFastSchemaEvolutionInDanger` が false の場合でも FSE を許可しますが、パーティションエントリはクリアしません。
- 導入バージョン: v4.1.0

### `enable_auto_collect_array_ndv`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ARRAY 型の NDV 情報の自動収集を有効にするかどうか。
- 導入時期：v4.0

### `enable_backup_materialized_view`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：特定のデータベースをバックアップまたは復元する際に、非同期マテリアライズドビューの BACKUP と RESTORE を有効にするかどうか。この項目が `false` に設定されている場合、StarRocks は非同期マテリアライズドビューのバックアップをスキップします。
- 導入時期：v3.2.0

### `enable_collect_full_statistic`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：自動完全統計収集を有効にするかどうか。この機能はデフォルトで有効になっています。
- 導入時期：-

### `enable_colocate_mv_index`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：同期マテリアライズドビューを作成するときに、同期マテリアライズドビューインデックスを基底テーブルとコロケートすることをサポートするかどうか。この項目が `true` に設定されている場合、タブレットシンクは同期マテリアライズドビューの書き込みパフォーマンスを高速化します。
- 導入時期：v3.2.0

### `enable_decimal_v3`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：DECIMAL V3 データ型をサポートするかどうか。
- 導入時期：-

### `enable_experimental_mv`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：非同期マテリアライズドビュー機能を有効にするかどうか。TRUE はこの機能が有効であることを示します。v2.5.2 以降、この機能はデフォルトで有効になっています。v2.5.2 以前のバージョンでは、この機能はデフォルトで無効になっています。
- 導入時期：v2.4

### `enable_local_replica_selection`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：クエリのためにローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク転送コストを削減します。このパラメーターが TRUE に設定されている場合、CBO は現在の FE と同じ IP アドレスを持つ BE 上のタブレットレプリカを優先的に選択します。このパラメーターが `FALSE` に設定されている場合、ローカルレプリカと非ローカルレプリカの両方を選択できます。
- 導入時期：-

### `enable_manual_collect_array_ndv`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ARRAY 型の NDV 情報の手動収集を有効にするかどうか。
- 導入時期：v4.0

### `enable_materialized_view`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビューの作成を有効にするかどうか。
- 導入時期：-

### `enable_materialized_view_external_table_precise_refresh`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：基底テーブルが外部 (クラウドネイティブではない) テーブルである場合に、マテリアライズドビューの更新のための内部最適化を有効にするには、この項目を `true` に設定します。有効にすると、マテリアライズドビューの更新プロセッサは候補パーティションを計算し、すべてのパーティションではなく影響を受ける基底テーブルパーティションのみを更新し、I/O と更新コストを削減します。外部テーブルの完全パーティション更新を強制するには `false` に設定します。
- 導入時期：v3.2.9

### `enable_materialized_view_metrics_collect`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：非同期マテリアライズドビューの監視メトリックをデフォルトで収集するかどうか。
- 導入時期：v3.1.11, v3.2.5

### `enable_materialized_view_spill`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：マテリアライズドビュー更新タスクの途中結果スピルを有効にするかどうか。
- 導入時期：v3.1.1

### `enable_materialized_view_text_based_rewrite`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：デフォルトでテキストベースのクエリ書き換えを有効にするかどうか。この項目が `true` に設定されている場合、システムは非同期マテリアライズドビューの作成中に抽象構文ツリーを構築します。
- 導入時期：v3.2.5

### `enable_mv_automatic_active_check`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：スキーマ変更または基底テーブル (ビュー) の削除と再作成によって非アクティブになった非同期マテリアライズドビューを、システムが自動的にチェックして再アクティブ化する機能を有効にするかどうか。この機能は、ユーザーが手動で非アクティブに設定したマテリアライズドビューを再アクティブ化しないことに注意してください。
- 導入時期：v3.1.6

### `enable_mv_automatic_repairing_for_broken_base_tables`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、StarRocks は、基底外部テーブルが削除され再作成されたり、テーブル識別子が変更されたりした場合に、マテリアライズドビューの基底テーブルメタデータを自動的に修復しようとします。修復フローは、マテリアライズドビューの基底テーブル情報を更新し、外部テーブルパーティションのパーティションレベルの修復情報を収集し、`autoRefreshPartitionsLimit` を尊重しながら非同期自動更新マテリアライズドビューのパーティション更新決定を駆動することができます。現在、自動修復は Hive 外部テーブルをサポートしています。サポートされていないテーブルタイプでは、マテリアライズドビューが非アクティブに設定され、修復例外が発生します。パーティション情報収集は非ブロッキングであり、失敗はログに記録されます。
- 導入時期：v3.3.19, v3.4.8, v3.5.6

### `enable_predicate_columns_collection`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：述語列収集を有効にするかどうか。無効にすると、クエリオプティマイゼーション中に述語列は記録されません。
- 導入時期：-

### `enable_query_queue_v2`

- デフォルト：true
- タイプ：boolean
- 単位：-
- 変更可能：No
- 説明：true の場合、FE のスロットベースクエリスケジューラを Query Queue V2 に切り替えます。このフラグはスロットマネージャーとトラッカー (例: `BaseSlotManager.isEnableQueryQueueV2` と `SlotTracker#createSlotSelectionStrategy`) によって読み取られ、レガシー戦略ではなく `SlotSelectionStrategyV2` を選択します。`query_queue_v2_xxx` 構成オプションと `QueryQueueOptions` は、このフラグが有効な場合にのみ有効になります。v4.1 以降、デフォルト値は `false` から `true` に変更されました。
- 導入時期：v3.3.4, v3.4.0, v3.5.0

### `enable_sql_blacklist`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：SQL クエリのブラックリストチェックを有効にするかどうか。この機能が有効になっている場合、ブラックリスト内のクエリは実行できません。
- 導入時期：-

### `enable_statistic_collect`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：CBO の統計情報を収集するかどうか。この機能はデフォルトで有効になっています。
- 導入時期：-

### `enable_statistic_collect_on_first_load`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：データロード操作によってトリガーされる自動統計収集とメンテナンスを制御します。これには以下が含まれます。
  - データがパーティションに最初にロードされたとき (パーティションバージョンが 2 のとき) の統計収集。
  - マルチパーティションテーブルの空のパーティションにデータがロードされたときの統計収集。
  - INSERT OVERWRITE 操作の統計コピーと更新。

  **統計収集タイプの決定ポリシー:**
  
  - INSERT OVERWRITE の場合: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。既存の統計のみがコピーされます。
    - それ以外の場合、`targetRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  - 最初のロードの場合: `deltaRatio = loadRows / (totalRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。
    - それ以外の場合、`loadRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  **同期動作:**
  
  - DML ステートメント (INSERT INTO/INSERT OVERWRITE) の場合: テーブルロックを使用した同期モード。ロード操作は統計収集が完了するまで待機します (最大 `semi_sync_collect_statistic_await_seconds` まで)。
  - ストリームロードとブローカーロードの場合: ロックなしの非同期モード。統計収集はバックグラウンドで実行され、ロード操作をブロックしません。
  
  :::note
  この設定を無効にすると、ロードトリガー統計操作がすべて防止されます。これには INSERT OVERWRITE の統計メンテナンスも含まれ、テーブルに統計が不足する可能性があります。新しいテーブルが頻繁に作成され、データが頻繁にロードされる場合、この機能を有効にするとメモリと CPU のオーバーヘッドが増加します。
  :::

- 導入時期：v3.1

### `enable_statistic_collect_on_update`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：UPDATE ステートメントが自動統計収集をトリガーできるかどうかを制御します。有効にすると、テーブルデータを変更する UPDATE 操作は、`enable_statistic_collect_on_first_load` によって制御される同じインジェストベースの統計フレームワークを通じて統計収集をスケジュールする可能性があります。この設定を無効にすると、UPDATE ステートメントの統計収集はスキップされますが、ロードトリガー統計収集の動作は変更されません。
- 導入時期：v3.5.11, v4.0.4

### `enable_udf`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：UDF を有効にするかどうか。
- 導入時期：-

### `expr_children_limit`

- デフォルト：10000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：式で許可される子式の最大数。
- 導入時期：-

### `histogram_buckets_size`

- デフォルト：64
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：ヒストグラムのデフォルトのバケット数。
- 導入時期：-

### `histogram_max_sample_row_count`

- デフォルト：10000000
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：ヒストグラムで収集する最大行数。
- 導入時期：-

### `histogram_mcv_size`

- デフォルト：100
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：ヒストグラムの最頻値 (MCV) の数。
- 導入時期：-

### `histogram_sample_ratio`

- デフォルト：0.1
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：ヒストグラムのサンプリング比率。
- 導入時期：-

### `http_slow_request_threshold_ms`

- デフォルト：5000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：HTTP リクエストの応答時間がこのパラメーターで指定された値を超えると、そのリクエストを追跡するためのログが生成されます。
- 導入時期：v2.5.15, v3.1.5

### `lock_checker_enable_deadlock_check`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、LockChecker スレッドは ThreadMXBean.findDeadlockedThreads() を使用して JVM レベルのデッドロック検出を実行し、問題のあるスレッドのスタックトレースをログに記録します。このチェックは LockChecker デーモン内で実行され (その頻度は `lock_checker_interval_second` で制御されます)、詳細なスタック情報をログに書き込みます。これは CPU および I/O を集中的に行う可能性があります。このオプションは、ライブまたは再現可能なデッドロック問題のトラブルシューティングにのみ有効にしてください。通常の操作で有効のままにすると、オーバーヘッドとログのボリュームが増加する可能性があります。
- 導入時期：v3.2.0

### `low_cardinality_threshold`

- デフォルト：255
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：低カーディナリティ辞書のしきい値。
- 導入時期：v3.5.0

### `materialized_view_min_refresh_interval`

- デフォルト：60
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：非同期マテリアライズドビューのスケジュールで許可される最小更新間隔 (秒単位)。マテリアライズドビューが時間ベースの間隔で作成された場合、その間隔は秒に変換され、この値以上でなければなりません。そうでない場合、CREATE/ALTER 操作は DDL エラーで失敗します。この値が 0 より大きい場合、チェックが強制されます。TaskManager の過剰なスケジューリングと、頻繁すぎる更新による FE のメモリ/CPU 使用率の高さを防ぐため、制限を無効にするには 0 または負の値に設定します。この項目は `EVENT_TRIGGERED` の更新には適用されません。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `materialized_view_refresh_ascending`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、マテリアライズドビューのパーティション更新は昇順のパーティションキー順 (最も古いものから最も新しいものへ) でパーティションを反復処理します。`false` (デフォルト) に設定されている場合、システムは降順 (最も新しいものから最も古いものへ) で反復処理します。StarRocks は、パーティション更新制限が適用される場合に処理するパーティションを選択し、後続の TaskRun 実行の次の開始/終了パーティション境界を計算するために、リストパーティションおよび範囲パーティションのマテリアライズドビュー更新ロジックの両方でこの項目を使用します。この項目を変更すると、最初に更新されるパーティションと、次のパーティション範囲がどのように導出されるかが変わります。範囲パーティションのマテリアライズドビューの場合、スケジューラは新しい開始/終了を検証し、変更によって繰り返される境界 (デッドループ) が作成される場合はエラーを発生させるため、この項目は注意して設定してください。
- 導入時期：v3.3.1, v3.4.0, v3.5.0

### `max_allowed_in_element_num_of_delete`

- デフォルト：10000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：DELETE ステートメントの IN 述語で許可される要素の最大数。
- 導入時期：-

### `max_create_table_timeout_second`

- デフォルト：600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：テーブル作成の最大タイムアウト期間。
- 導入時期：-

### `max_distribution_pruner_recursion_depth`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：パーティションプルーナーによって許可される最大再帰深度。再帰深度を増やすと、より多くの要素をプルーニングできますが、CPU 消費量も増加します。
- 導入時期：-

### `max_partitions_in_one_batch`

- デフォルト：4096
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：複数のパーティションを一括作成する際に作成できる最大パーティション数。
- 導入時期：-

### `max_planner_scalar_rewrite_num`

- デフォルト：100000
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：オプティマイザがスカラー演算子を書き換えできる最大回数。
- 導入時期：-

### `max_query_queue_history_slots_number`

- デフォルト：0
- タイプ：Int
- 単位：Slots
- 変更可能：Yes
- 説明：監視および可観測性のために、クエリキューごとに保持される最近解放された (履歴) 割り当てスロットの数を制御します。`max_query_queue_history_slots_number` が `> 0` の値に設定されている場合、BaseSlotTracker は、その数の最も最近解放された LogicalSlot エントリをメモリ内キューに保持し、制限を超えると最も古いエントリを削除します。これを有効にすると、getSlots() にこれらの履歴エントリ (最新のものが最初) が含まれるようになり、BaseSlotTracker はより豊富な ExtraMessage データのために ConnectContext にスロットを登録しようとすることができ、LogicalSlot.ConnectContextListener はクエリ完了メタデータを履歴スロットにアタッチできます。`max_query_queue_history_slots_number` が `<= 0` の場合、履歴メカニズムは無効になります (余分なメモリは使用されません)。可観測性とメモリオーバーヘッドのバランスを取るために、適切な値を使用してください。
- 導入時期：v3.5.0

### `max_query_retry_time`

- デフォルト：2
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE でのクエリ再試行の最大数。
- 導入時期：-

### `max_running_rollup_job_num_per_table`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：テーブルに対して並行して実行できるロールアップジョブの最大数。
- 導入時期：-

### `max_scalar_operator_flat_children`

- Default：10000
- Type：Int
- Unit：-
- 変更可能：Yes
- Description：ScalarOperator のフラットな子の最大数。オプティマイザが過剰なメモリを使用するのを防ぐために、この制限を設定できます。
- 導入時期：-

### `max_scalar_operator_optimize_depth`

- Default：256
- Type：Int
- Unit：-
- 変更可能：Yes
- 説明：ScalarOperator 最適化が適用できる最大深度。
- 導入時期：-

### `mv_active_checker_interval_seconds`

- デフォルト：60
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：バックグラウンドの `active_checker` スレッドが有効な場合、システムは定期的に、スキーマ変更または基底テーブル (ビュー) の再構築によって非アクティブになったマテリアライズドビューを検出して自動的に再アクティブ化します。このパラメーターは、チェッカー スレッドのスケジューリング間隔を秒単位で制御します。デフォルト値はシステム定義です。
- 導入時期：v3.1.6

### `mv_rewrite_consider_data_layout_mode`

- デフォルト：`enable`
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：最適なマテリアライズドビューを選択する際に、マテリアライズドビューの書き換えで基底テーブルのデータレイアウトを考慮するかどうかを制御します。有効な値:
  - `disable`: 候補マテリアライズドビューを選択する際に、データレイアウト基準を使用しない。
  - `enable`: クエリがレイアウトを意識していると認識された場合にのみ、データレイアウト基準を使用する。
  - `force`: 最適なマテリアライズドビューを選択する際に、常にデータレイアウト基準を適用する。
  この項目を変更すると、`BestMvSelector` の動作に影響し、物理的なレイアウトが計画の正確性やパフォーマンスに影響するかどうかに応じて、書き換えの適用範囲を改善または拡大することができます。
- 導入時期：-

### `publish_version_interval_ms`

- デフォルト：10
- タイプ：Int
- 単位：Milliseconds
- 変更可能：No
- 説明：リリース検証タスクが発行される時間間隔。
- 導入時期：-

### `query_queue_slots_estimator_strategy`

- デフォルト：MAX
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：`enable_query_queue_v2` が true の場合に、キューベースクエリに使用されるスロット推定戦略を選択します。有効な値は、MBE (メモリベース)、PBE (並行性ベース)、MAX (MBE と PBE の最大値を取る)、MIN (MBE と PBE の最小値を取る) です。MBE は、予測されたメモリまたは計画コストをスロットあたりのメモリターゲットで割ってスロットを推定し、`totalSlots` で上限が設定されます。PBE は、フラグメントの並行性 (スキャン範囲のカウントまたはカーディナリティ / スロットあたりの行数) と CPU コストベースの計算 (スロットあたりの CPU コストを使用) からスロットを導出し、結果を [numSlots/2, numSlots] の範囲内に制限します。MAX と MIN は、MBE と PBE の最大値または最小値を取ることによってそれらを結合します。設定された値が無効な場合、デフォルト (`MAX`) が使用されます。
- 導入時期：v3.5.0

### `query_queue_v2_concurrency_level`

- デフォルト：4
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：システムの総クエリスロットを計算する際に使用される論理的な並行性「レイヤー」の数を制御します。Shared-nothing モードでは、総スロット = `query_queue_v2_concurrency_level` * BE の数 * BE ごとのコア数 (BackendResourceStat から派生)。マルチウェアハウスモードでは、実効並行性は max(1, `query_queue_v2_concurrency_level` / 4) にスケーリングされます。設定値が非正の場合、`4` として扱われます。この値を変更すると、totalSlots (したがって同時クエリ容量) が増減し、スロットごとのリソースに影響します。memBytesPerSlot はワーカーごとのメモリを (ワーカーごとのコア数 * 並行性) で割って導出され、CPU アカウンティングは `query_queue_v2_cpu_costs_per_slot` を使用します。クラスターサイズに比例して設定してください。非常に大きな値はスロットごとのメモリを減らし、リソースの断片化を引き起こす可能性があります。
- 導入時期：v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_cpu_costs_per_slot`

- デフォルト：1000000000
- タイプ：Long
- 単位：planner CPU cost units
- 変更可能：Yes
- 説明：プランナー CPU コストからクエリが必要とするスロット数を推定するために使用されるスロットごとの CPU コストしきい値。スケジューラーは、スロットを整数 (`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`) として計算し、結果を [1, totalSlots] の範囲にクランプします (totalSlots はクエリキュー V2 `V2` パラメーターから派生)。V2 コードは非正の設定を 1 に正規化するため (Math.max(1, value))、非正の値は事実上 `1` になります。この値を増やすと、クエリごとに割り当てられるスロットが減少し (より少ない、より大きなスロットのクエリを優先)、減らすとクエリごとのスロットが増加します。並行性対リソースの粒度を制御するために、`query_queue_v2_num_rows_per_slot` および並行性設定と合わせて調整してください。
- 導入時期：v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_num_rows_per_slot`

- デフォルト：4096
- タイプ：Int
- 単位：Rows
- 変更可能：Yes
- 説明：クエリごとのスロット数を推定する際に、単一のスケジューリングスロットに割り当てられるターゲットのソース行レコード数。StarRocks は、`estimated_slots` = (ソースノードのカーディナリティ) / `query_queue_v2_num_rows_per_slot` を計算し、その結果を [1, totalSlots] の範囲にクランプし、計算された値が非正の場合は最低 1 を強制します。totalSlots は利用可能なリソース (おおよそ DOP * `query_queue_v2_concurrency_level` * ワーカー/BE の数) から導出され、したがってクラスター/コア数に依存します。この値を増やすと、スロット数が減少し (各スロットがより多くの行を処理)、スケジューリングオーバーヘッドが減少します。減らすと、並行性が増加し (より多くの、より小さいスロット)、リソース制限まで増加します。
- 導入時期：v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_schedule_strategy`

- デフォルト：SWRR
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：Query Queue V2 が保留中のクエリを並べ替えるために使用するスケジューリングポリシーを選択します。サポートされている値 (大文字と小文字を区別しない) は `SWRR` (Smooth Weighted Round Robin) - デフォルトで、公平な重み付き共有が必要な混合/ハイブリッドワークロードに適しています - と `SJF` (Short Job First + Aging) - 短いジョブを優先し、エージングを使用してスタベーションを回避します。値は大文字と小文字を区別しない enum ルックアップで解析されます。認識されない値はエラーとしてログに記録され、デフォルトポリシーが使用されます。この設定は Query Queue V2 が有効な場合にのみ動作に影響し、`query_queue_v2_concurrency_level` などの V2 サイジング設定と相互作用します。
- 導入時期：v3.3.12, v3.4.2, v3.5.0

### `semi_sync_collect_statistic_await_seconds`

- デフォルト：30
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：DML 操作 (INSERT INTO および INSERT OVERWRITE ステートメント) 中の半同期統計収集の最大待機時間。ストリームロードおよびブローカーロードは非同期モードを使用するため、この設定の影響を受けません。統計収集時間がこの値を超えると、ロード操作は収集完了を待たずに続行します。この設定は `enable_statistic_collect_on_first_load` と連携して機能します。
- 導入時期：v3.1

### `slow_query_analyze_threshold`

- デフォルト：5
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：クエリフィードバックの分析をトリガーするクエリの実行時間しきい値。
- 導入時期：v3.4.0

### `statistic_analyze_status_keep_second`

- デフォルト：3 * 24 * 3600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：収集タスクの履歴を保持する期間。デフォルト値は 3 日間です。
- 導入時期：-

### `statistic_auto_analyze_end_time`

- デフォルト：23:59:59
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：自動収集の終了時刻。値の範囲: `00:00:00` - `23:59:59`。
- 導入時期：-

### `statistic_auto_analyze_start_time`

- デフォルト：00:00:00
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：自動収集の開始時刻。値の範囲: `00:00:00` - `23:59:59`。
- 導入時期：-

### `statistic_auto_collect_ratio`

- デフォルト：0.8
- タイプ：Double
- 単位：-
- 変更可能：Yes
- 説明：自動収集の統計が健全であるかどうかを判断するためのしきい値。統計の健全性がこのしきい値よりも低い場合、自動収集がトリガーされます。
- 導入時期：-

### `statistic_auto_collect_small_table_rows`

- デフォルト：10000000
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：自動収集中に、外部データソース (Hive、Iceberg、Hudi) のテーブルが小さいテーブルであるかどうかを判断するためのしきい値。テーブルの行数がこの値より少ない場合、そのテーブルは小さいテーブルと見なされます。
- 導入時期：v3.2

### `statistic_cache_columns`

- デフォルト：100000
- タイプ：Long
- 単位：-
- 変更可能：No
- 説明：統計テーブルにキャッシュできる行数。
- 導入時期：-

### `statistic_cache_thread_pool_size`

- デフォルト：10
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：統計キャッシュを更新するために使用されるスレッドプールのサイズ。
- 導入時期：-

### `statistic_collect_interval_sec`

- デフォルト：5 * 60
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：自動収集中にデータ更新をチェックする間隔。
- 導入時期：-

### `statistic_max_full_collect_data_size`

- デフォルト：100 * 1024 * 1024 * 1024
- タイプ：Long
- 単位：bytes
- 変更可能：Yes
- 説明：統計の自動収集のデータサイズしきい値。合計サイズがこの値を超えると、完全収集ではなくサンプリング収集が実行されます。
- 導入時期：-

### `statistic_sample_collect_rows`

- デフォルト：200000
- タイプ：Long
- 単位：-
- 変更可能：Yes
- 説明：ロードトリガー統計操作中に SAMPLE 統計収集と FULL 統計収集のどちらかを決定するための行数しきい値。ロードまたは変更された行数がこのしきい値 (デフォルト 200,000) を超える場合、SAMPLE 統計収集が使用されます。そうでない場合、FULL 統計収集が使用されます。この設定は、`enable_statistic_collect_on_first_load` および `statistic_sample_collect_ratio_threshold_of_first_load` と連携して機能します。
- 導入時期：-

### `statistic_update_interval_sec`

- デフォルト：24 * 60 * 60
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：統計情報のキャッシュが更新される間隔。
- 導入時期：-

### `task_check_interval_second`

- デフォルト：60
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：タスクバックグラウンドジョブの実行間隔。GlobalStateMgr はこの値を使用して `doTaskBackgroundJob()` を呼び出す TaskCleaner FrontendDaemon をスケジュールします。この値はデーモン間隔をミリ秒単位で設定するために 1000 倍されます。値を減らすとバックグラウンドメンテナンス (タスククリーンアップ、チェック) の実行頻度が高まり、反応が速くなりますが、CPU/IO オーバーヘッドが増加します。値を増やすとオーバーヘッドが減少しますが、クリーンアップと古いタスクの検出が遅れます。この値を調整して、メンテナンスの応答性とリソース使用量のバランスを取ります。
- 導入時期：v3.2.0

### `task_min_schedule_interval_s`

- デフォルト：10
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：SQL レイヤーによってチェックされるタスクスケジュールの最小許容スケジュール間隔 (秒単位)。タスクが送信されると、TaskAnalyzer はスケジュール期間を秒に変換し、期間が `task_min_schedule_interval_s` より小さい場合、`ERR_INVALID_PARAMETER` で送信を拒否します。これにより、頻繁すぎる実行タスクの作成が防止され、スケジューラーが高頻度タスクから保護されます。スケジュールに明示的な開始時間がない場合、TaskAnalyzer は開始時間を現在のエポック秒に設定します。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `task_runs_timeout_second`

- デフォルト：4 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：TaskRun のデフォルトの実行タイムアウト (秒単位)。この項目は TaskRun 実行の基準タイムアウトとして使用されます。タスク実行のプロパティに、正の整数値を持つセッション変数 `query_timeout` または `insert_timeout` が含まれている場合、実行時はそのセッションタイムアウトと `task_runs_timeout_second` のうち大きい方の値を使用します。その後、実質的なタイムアウトは、設定された `task_runs_ttl_second` および `task_ttl_second` を超えないように制限されます。タスク実行の最大実行時間を制限するには、この項目を設定します。非常に大きな値は、タスク/タスク実行の TTL 設定によって切り捨てられる可能性があります。
- 導入時期：-

## ロードとアンロード

### `broker_load_default_timeout_second`

- デフォルト：14400
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：Broker Load ジョブのタイムアウト期間。
- 導入時期：-

### `desired_max_waiting_jobs`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE での保留中のジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FE での保留中のジョブの数がこの値に達すると、FE は新しいロード要求を拒否します。このパラメーターは、非同期ロードにのみ有効です。v2.5 以降、デフォルト値は 100 から 1024 に変更されました。
- 導入時期：-

### `disable_load_job`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：クラスターでエラーが発生した場合にロードを無効にするかどうか。これにより、クラスターエラーによる損失を防ぎます。デフォルト値は `FALSE` で、ロードが無効になっていないことを示します。`TRUE` はロードが無効になっており、クラスターが読み取り専用状態であることを示します。
- 導入時期：-

### `empty_load_as_error`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：データがロードされない場合にエラーメッセージ「すべてのパーティションにロードデータがありません」を返すかどうか。有効な値:
  - `true`: データがロードされない場合、システムは失敗メッセージを表示し、エラー「すべてのパーティションにロードデータがありません」を返します。
  - `false`: データがロードされない場合、システムは成功メッセージを表示し、エラーではなく OK を返します。
- 導入時期：-

### `enable_file_bundling`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：クラウドネイティブテーブルのファイルバンドル最適化を有効にするかどうか。この機能が有効 ( `true` に設定) の場合、システムはロード、コンパクション、または公開操作によって生成されたデータファイルを自動的にバンドルし、それによって外部ストレージシステムへの高頻度アクセスによって発生する API コストを削減します。この動作は、CREATE TABLE プロパティ `file_bundling` を使用してテーブルレベルで制御することもできます。詳細な手順については、CREATE TABLE を参照してください。
- 導入時期：v4.0

### `enable_routine_load_lag_metrics`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ルーチンロード Kafka パーティションオフセットラグメトリックを収集するかどうか。この項目を `true` に設定すると、Kafka API が呼び出されてパーティションの最新オフセットが取得されることに注意してください。
- 導入時期：-

### `enable_sync_publish`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ロードトランザクションの公開フェーズで適用タスクを同期的に実行するかどうか。このパラメーターは主キーテーブルにのみ適用されます。有効な値:
  - `TRUE` (デフォルト): 適用タスクはロードトランザクションの公開フェーズで同期的に実行されます。これは、適用タスクが完了し、ロードされたデータが実際にクエリ可能になった後にのみ、ロードトランザクションが成功として報告されることを意味します。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりする場合、このパラメーターを `true` に設定するとクエリパフォーマンスと安定性が向上しますが、ロードレイテンシーが増加する可能性があります。
  - `FALSE`: 適用タスクはロードトランザクションの公開フェーズで非同期的に実行されます。これは、適用タスクが送信された後にロードトランザクションが成功として報告されますが、ロードされたデータはすぐにクエリできないことを意味します。この場合、同時クエリは適用タスクが完了するかタイムアウトするまで待機してから続行する必要があります。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりしたりする場合、このパラメーターを `false` に設定すると、クエリパフォーマンスと安定性に影響する可能性があります。
- 導入時期：v3.2.0

### `export_checker_interval_second`

- デフォルト：5
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：ロードジョブがスケジュールされる時間間隔。
- 導入時期：-

### `export_max_bytes_per_be_per_task`

- デフォルト：268435456
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：単一の BE から単一のデータアンロードタスクによってエクスポートできる最大データ量。
- 導入時期：-

### `export_running_job_num_limit`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：並行して実行できるデータエクスポートタスクの最大数。
- 導入時期：-

### `export_task_default_timeout_second`

- デフォルト：2 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：データエクスポートタスクのタイムアウト期間。
- 導入時期：-

### `export_task_pool_size`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：アンロードタスクのスレッドプールのサイズ。
- 導入時期：-

### `external_table_commit_timeout_ms`

- デフォルト：10000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：StarRocks 外部テーブルへの書き込みトランザクションをコミット (公開) するためのタイムアウト期間。デフォルト値 `10000` は 10 秒のタイムアウト期間を示します。
- 導入時期：-

### `finish_transaction_default_lock_timeout_ms`

- デフォルト：1000
- タイプ：Int
- 単位：MilliSeconds
- 変更可能：Yes
- 説明：トランザクション完了中の db およびテーブルロック取得のデフォルトタイムアウト。
- 導入時期：v4.0.0, v3.5.8

### `history_job_keep_max_second`

- デフォルト：7 * 24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：スキーマ変更ジョブなど、履歴ジョブが保持できる最大期間。
- 導入時期：-

### `insert_load_default_timeout_second`

- デフォルト：3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：データをロードするために使用される INSERT INTO ステートメントのタイムアウト期間。
- 導入時期：-

### `label_clean_interval_second`

- デフォルト：4 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入時期：-

### `label_keep_max_num`

- デフォルト：1000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報は削除されます。
- 導入時期：-

### `label_keep_max_second`

- デフォルト：3 * 24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：完了し、FINISHED または CANCELLED 状態にあるロードジョブのラベルを保持する最大期間 (秒単位)。デフォルト値は 3 日間です。この期間が経過すると、ラベルは削除されます。このパラメーターはすべてのタイプのロードジョブに適用されます。値が大きすぎると大量のメモリを消費します。
- 導入時期：-

### `load_checker_interval_second`

- デフォルト：5
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：ロードジョブがローリングベースで処理される時間間隔。
- 導入時期：-

### `load_parallel_instance_num`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：ブローカーおよびストリームロードの単一ホストで作成される並行ロードフラグメントインスタンスの数を制御します。LoadPlanner は、セッションがアダプティブシンク DOP を有効にしない限り、この値をホストごとの並行度として使用します。セッション変数 `enable_adaptive_sink_dop` が true の場合、セッションの `sink_degree_of_parallelism` がこの設定を上書きします。シャッフルが必要な場合、この値はフラグメント並行実行 (スキャンフラグメントおよびシンクフラグメント並行実行インスタンス) に適用されます。シャッフルが不要な場合、シンクパイプライン DOP として使用されます。注: ローカルファイルからのロードは、ローカルディスク競合を避けるため、単一インスタンスに強制されます (パイプライン DOP = 1、並行実行 = 1)。この数を増やすと、ホストごとの並行性とスループットが向上しますが、CPU、メモリ、I/O 競合が増加する可能性があります。
- 導入時期：v3.2.0

### `load_straggler_wait_second`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：BE レプリカが許容できる最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするクローニングが実行されます。
- 導入時期：-

### `loads_history_retained_days`

- デフォルト：30
- タイプ：Int
- 単位：Days
- 変更可能：Yes
- 説明：内部 `_statistics_.loads_history` テーブルにロード履歴を保持する日数。この値はテーブル作成時にテーブルプロパティ `partition_live_number` を設定するために使用され、`TableKeeper` (最小値 1 にクランプ) に渡されて、保持する日次パーティションの数を決定します。この値を増減すると、完了したロードジョブが日次パーティションに保持される期間が調整されます。新しいテーブルの作成とキーパーのプルーニング動作に影響しますが、過去のパーティションを自動的に再作成することはありません。`LoadsHistorySyncer` は、ロード履歴のライフサイクルを管理する際にこの保持に依存します。その同期頻度は `loads_history_sync_interval_second` によって制御されます。
- 導入時期：v3.3.6, v3.4.0, v3.5.0

### `loads_history_sync_interval_second`

- デフォルト：60
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：LoadsHistorySyncer が `information_schema.loads` から内部 `_statistics_.loads_history` テーブルに完了したロードジョブを定期的に同期する間隔 (秒単位)。この値は、FrontendDaemon の間隔を設定するためにコンストラクターで 1000 倍されます。シンカーは最初の実行をスキップし (テーブル作成を許可するため)、1 分以上前に完了したロードのみをインポートします。値が小さいと DML とエクゼキュータの負荷が増加し、値が大きいと履歴ロードレコードの可用性が遅延します。ターゲットテーブルの保持/パーティショニング動作については `loads_history_retained_days` を参照してください。
- 導入時期：v3.3.6, v3.4.0, v3.5.0

### `max_broker_load_job_concurrency`

- デフォルト：5
- Alias: `async_load_task_pool_size`
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：StarRocks クラスター内で許可される同時 Broker Load ジョブの最大数。このパラメーターは Broker Load にのみ有効です。このパラメーターの値は、`max_running_txn_num_per_db` の値よりも小さくなければなりません。v2.5 以降、デフォルト値は `10` から `5` に変更されました。
- 導入時期：-

### `max_load_timeout_second`

- デフォルト：259200
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべての種類のロードジョブに適用されます。
- 導入時期：-

### `max_routine_load_batch_size`

- デフォルト：4294967296
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：ルーチンロードタスクでロードできる最大データ量。
- 導入時期：-

### `max_routine_load_task_concurrent_num`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：各ルーチンロードジョブの同時実行タスクの最大数。
- 導入時期：-

### `max_routine_load_task_num_per_be`

- デフォルト：16
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：各 BE での同時ルーチンロードタスクの最大数。v3.1.0 以降、このパラメーターのデフォルト値は 5 から 16 に増加され、BE 静的パラメーター `routine_load_thread_pool_size` (非推奨) の値以下である必要はなくなりました。
- 導入時期：-

### `max_running_txn_num_per_db`

- デフォルト：1000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：StarRocks クラスター内の各データベースで実行できるロードトランザクションの最大数。デフォルト値は `1000` です。v3.1 以降、デフォルト値は `100` から `1000` に変更されました。データベースで実行されているロードトランザクションの実際の数がこのパラメーターの値を超えると、新しいロード要求は処理されません。同期ロードジョブの新しい要求は拒否され、非同期ロードジョブの新しい要求はキューに配置されます。この値を増やすことはシステム負荷を増加させるため、お勧めしません。
- 導入時期：-

### `max_stream_load_timeout_second`

- デフォルト：259200
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：Stream Load ジョブに許可される最大タイムアウト期間。
- 導入時期：-

### `max_tolerable_backend_down_num`

- デフォルト：0
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：許可される BE ノードの最大障害数。この数を超えると、ルーチンロードジョブは自動的に回復できません。
- 導入時期：-

### `min_bytes_per_broker_scanner`

- デフォルト：67108864
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：Broker Load インスタンスによって処理できる最小データ量。
- 導入時期：-

### `min_load_timeout_second`

- デフォルト：1
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：ロードジョブに許可される最小タイムアウト期間。この制限はすべての種類のロードジョブに適用されます。
- 導入時期：-

### `min_routine_load_lag_for_metrics`

- デフォルト：10000
- タイプ：INT
- 単位：-
- 変更可能：Yes
- 説明：監視メトリックに表示されるルーチンロードジョブの最小オフセットラグ。オフセットラグがこの値よりも大きいルーチンロードジョブがメトリックに表示されます。
- 導入時期：-

### `period_of_auto_resume_min`

- デフォルト：5
- タイプ：Int
- 単位：Minutes
- 変更可能：Yes
- 説明：ルーチンロードジョブが自動的に回復される間隔。
- 導入時期：-

### `prepared_transaction_default_timeout_second`

- デフォルト：86400
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：準備されたトランザクションのデフォルトのタイムアウト期間。
- 導入時期：-

### `routine_load_task_consume_second`

- デフォルト：15
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：クラスター内の各ルーチンロードタスクがデータを消費する最大時間。v3.1.0 以降、ルーチンロードジョブは `job_properties` に新しいパラメーター `task_consume_second` をサポートしています。このパラメーターはルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入時期：-

### `routine_load_task_timeout_second`

- デフォルト：60
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：クラスター内の各ルーチンロードタスクのタイムアウト期間。v3.1.0 以降、ルーチンロードジョブは `job_properties` に新しいパラメーター `task_timeout_second` をサポートしています。このパラメーターはルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入時期：-

### `routine_load_unstable_threshold_second`

- デフォルト：3600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：ルーチンロードジョブ内のいずれかのタスクが遅延した場合に、ルーチンロードジョブが UNSTABLE 状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在時刻の差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。
- 導入時期：-

### `spark_dpp_version`

- デフォルト：1.0.0
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：使用される Spark Dynamic Partition Pruning (DPP) のバージョン。
- 導入時期：-

### `spark_home_default_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Spark クライアントのルートディレクトリ。
- 導入時期：-

### `spark_launcher_log_dir`

- デフォルト：`sys_log_dir` + "/spark_launcher_log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Spark ログファイルを格納するディレクトリ。
- 導入時期：-

### `spark_load_default_timeout_second`

- デフォルト：86400
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：各 Spark Load ジョブのタイムアウト期間。
- 導入時期：-

### `spark_load_submit_timeout_second`

- デフォルト：300
- タイプ：long
- 単位：Seconds
- 変更可能：No
- 説明：Spark アプリケーションの送信後、YARN 応答を待機する最大時間 (秒単位)。`SparkLauncherMonitor.LogMonitor` はこの値をミリ秒に変換し、ジョブが UNKNOWN/CONNECTED/SUBMITTED 状態のままこのタイムアウトを超えると、監視を停止し、Spark ランチャープロセスを強制終了します。`SparkLoadJob` はこの設定をデフォルトとして読み取り、`LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` プロパティを使用してロードごとのオーバーライドを許可します。YARN キューイングの遅延に対応するのに十分な大きさに設定してください。低く設定しすぎると、正当にキューに入れられたジョブが中止される可能性があり、高く設定しすぎると、障害処理とリソースクリーンアップが遅れる可能性があります。
- 導入時期：v3.2.0

### `spark_resource_path`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Spark 依存関係パッケージのルートディレクトリ。
- 導入時期：-

### `stream_load_default_timeout_second`

- デフォルト：600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：各 Stream Load ジョブのデフォルトのタイムアウト期間。
- 導入時期：-

### `stream_load_max_txn_num_per_be`

- デフォルト：-1
- タイプ：Int
- 単位：Transactions
- 変更可能：Yes
- 説明：単一の BE (バックエンド) ホストから受け入れられる同時ストリームロードトランザクションの数を制限します。非負の整数に設定されている場合、FrontendServiceImpl は BE (クライアント IP 別) の現在のトランザクション数をチェックし、数がこの制限を `>=` 超える場合、新しいストリームロード開始要求を拒否します。値が `< 0` の場合、制限は無効になります (無制限)。このチェックはストリームロード開始時に発生し、超過すると `streamload txn num per be exceeds limit` エラーが発生する可能性があります。関連する実行時動作では、`stream_load_default_timeout_second` を使用して要求タイムアウトのフォールバックが行われます。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `stream_load_task_keep_max_num`

- デフォルト：1000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：StreamLoadMgr がメモリに保持する Stream Load タスクの最大数 (すべてのデータベースにわたるグローバル)。追跡されたタスク (`idToStreamLoadTask`) の数がこのしきい値を超えると、StreamLoadMgr はまず `cleanSyncStreamLoadTasks()` を呼び出して完了した同期ストリームロードタスクを削除します。サイズがまだこのしきい値の半分より大きい場合、`cleanOldStreamLoadTasks(true)` を呼び出して古いまたは完了したタスクを強制的に削除します。メモリにさらにタスク履歴を保持するにはこの値を増やし、メモリ使用量を減らしてクリーンアップをより積極的するには減らします。この値はインメモリ保持のみを制御し、永続化/再生されたタスクには影響しません。
- 導入時期：v3.2.0

### `stream_load_task_keep_max_second`

- デフォルト：3 * 24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：完了またはキャンセルされた Stream Load タスクの保持ウィンドウ。タスクが最終状態に達し、その終了タイムスタンプがこのしきい値 (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`) より古い場合、`StreamLoadMgr.cleanOldStreamLoadTasks` による削除の対象となり、永続化された状態のロード時に破棄されます。`StreamLoadTask` と `StreamLoadMultiStmtTask` の両方に適用されます。合計タスク数が `stream_load_task_keep_max_num` を超える場合、クリーンアップが早期にトリガーされる可能性があります (同期タスクは `cleanSyncStreamLoadTasks` によって優先されます)。履歴/デバッグ可能性とメモリ使用量のバランスを取るためにこれを設定します。
- 導入時期：v3.2.0

### `transaction_clean_interval_second`

- デフォルト：30
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：完了したトランザクションがクリーンアップされる時間間隔。単位: 秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入時期：-

### `transaction_stream_load_coordinator_cache_capacity`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：トランザクションラベルからコーディネーターノードへのマッピングを格納するキャッシュの容量。
- 導入時期：-

### `transaction_stream_load_coordinator_cache_expire_seconds`

- デフォルト：900
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：キャッシュ内のコーディネーターマッピングを削除するまでの時間 (TTL)。
- 導入時期：-

### `yarn_client_path`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Yarn クライアントパッケージのルートディレクトリ。
- 導入時期：-

### `yarn_config_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：Yarn 設定ファイルを格納するディレクトリ。
- 導入時期：-
