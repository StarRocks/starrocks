---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical d - h"
---

# メトリクス d から h

:::note

マテリアライズドビューと共有データクラスターのメトリクスは、対応するセクションで詳しく説明されています。

- [非同期マテリアライズドビューのメトリクス](../metrics-materialized_view.md)
- [共有データダッシュボードのメトリクス、およびStarletダッシュボードのメトリクス](../metrics-shared-data.md)

StarRocksクラスターの監視サービスを構築する方法の詳細については、以下を参照してください。[監視とアラート](../monitoring.md).

:::

## `data_stream_receiver_count`

- 単位: カウント
- 説明: BEでExchangeレシーバーとして機能するインスタンスの累積数。

## `datacache_disk_quota_bytes`

- 単位: バイト
- タイプ: ゲージ
- 説明: データキャッシュに設定されたディスククォータ。

## `datacache_disk_used_bytes`

- 単位: バイト
- タイプ: ゲージ
- 説明: データキャッシュの現在のディスク使用量。

## `datacache_mem_quota_bytes`

- 単位: バイト
- タイプ: ゲージ
- 説明: データキャッシュに設定されたメモリクォータ。

## `datacache_mem_used_bytes`

- 単位: バイト
- タイプ: ゲージ
- 説明: データキャッシュの現在のメモリ使用量。

## `datacache_meta_used_bytes`

- 単位: バイト
- タイプ: ゲージ
- 説明: データキャッシュメタデータのメモリ使用量。

## `date_column_pool_bytes`

- 単位: バイト
- 説明: DATEカラムプールで使用されるメモリ。

## `datetime_column_pool_bytes`

- 単位: バイト
- 説明: DATETIMEカラムプールで使用されるメモリ。

## `decimal_column_pool_bytes`

- 単位: バイト
- 説明: DECIMALカラムプールで使用されるメモリ。

## `delta_column_group_get_hit_cache`

- 単位: カウント
- 説明: デルタカラムグループキャッシュヒットの合計数（プライマリキーテーブルのみ）。

## `delta_column_group_get_non_pk_hit_cache`

- 単位: カウント
- 説明: デルタカラムグループキャッシュ内のヒットの合計数（非プライマリキーテーブルの場合）。

## `delta_column_group_get_non_pk_total`

- 単位: カウント
- 説明: デルタカラムグループを取得した合計回数（非プライマリキーテーブルのみ）。

## `delta_column_group_get_total`

- 単位: カウント
- 説明: デルタカラムグループを取得した合計回数（プライマリキーテーブルの場合）。

## `disk_bytes_read`

- 単位: バイト
- 説明: ディスクから読み取られた合計バイト数 (読み取られたセクター数 * 512)。

## `disk_bytes_written`

- 単位: バイト
- 説明: ディスクに書き込まれた合計バイト数。

## `disk_free`

- 単位: バイト
- タイプ: 平均
- 説明: 空きディスク容量。

## `disk_io_svctm`

- 単位: ミリ秒
- タイプ: 平均
- 説明: ディスクIOサービス時間。

## `disk_io_time_ms`

- 単位: ミリ秒
- 説明: I/Oに費やされた時間。

## `disk_io_time_weigthed`

- 単位: ミリ秒
- 説明: I/Oに費やされた加重時間。

## `disk_io_util`

- 単位: -
- タイプ: 平均
- 説明: ディスク使用率。

## `disk_read_time_ms`

- 単位: ミリ秒
- 説明: ディスクからの読み取りに費やされた時間。単位: ミリ秒。

## `disk_reads_completed`

- 単位: カウント
- 説明: 正常に完了したディスク読み取りの数。

## `disk_sync_total (Deprecated)`

## `disk_used`

- 単位: バイト
- タイプ: 平均
- 説明: 使用済みディスク容量。

## `disk_write_time_ms`

- 単位: ミリ秒
- 説明: ディスク書き込みに費やされた時間。単位: ミリ秒。

## `disk_writes_completed`

- 単位: カウント
- 説明: 正常に完了したディスク書き込みの合計数。

## `disks_avail_capacity`

- 説明: 特定のディスクの利用可能な容量。

## `disks_data_used_capacity`

- 説明: 各ディスクの使用済み容量 (ストレージパスで表される)。

## `disks_state`

- 単位: -
- 説明: 各ディスクの状態。`1` はディスクが使用中であることを示し、`0` は使用中でないことを示します。

## `disks_total_capacity`

- 説明: ディスクの総容量。

## `double_column_pool_bytes`

- 単位: バイト
- 説明: DOUBLEカラムプールによって使用されるメモリ。

## `encryption_keys_created`

- 単位: カウント
- タイプ: 累積
- 説明: ファイル暗号化のために作成されたファイル暗号化キーの数

## `encryption_keys_in_cache`

- 単位: カウント
- タイプ: 瞬間
- 説明: 現在キーキャッシュにある暗号化キーの数

## `encryption_keys_unwrapped`

- 単位: カウント
- タイプ: 累積
- 説明: ファイル復号のためにアンラップされた暗号化メタの数

## `engine_requests_total`

- 単位: カウント
- 説明: BEとFE間のあらゆる種類の要求の総数。CREATE TABLE、Publish Version、タブレットクローンを含む。

## `fd_num_limit`

- 単位: カウント
- 説明: ファイルディスクリプタの最大数。

## `fd_num_used`

- 単位: カウント
- 説明: 現在使用中のファイルディスクリプタの数。

## `fe_cancelled_broker_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: キャンセルされたブローカジョブの数。

## `fe_cancelled_delete_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: キャンセルされた削除ジョブの数。

## `fe_cancelled_hadoop_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: キャンセルされたHadoopジョブの数。

## `fe_cancelled_insert_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: キャンセルされた挿入ジョブの数。

## `fe_checkpoint_push_per_second`

- 単位: カウント/秒
- タイプ: 平均
- 説明: FEチェックポイントの数。

## `fe_committed_broker_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: コミットされたブローカジョブの数。

## `fe_committed_delete_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: コミットされた削除ジョブの数。

## `fe_committed_hadoop_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: コミットされたHadoopジョブの数。

## `fe_committed_insert_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: コミットされた挿入ジョブの数。

## `fe_connection_total`

- 単位: カウント
- タイプ: 累積
- 説明: FE接続の総数。

## `fe_connections_per_second`

- 単位: カウント/秒
- タイプ: 平均
- 説明: FEの新規接続レート。

## `fe_edit_log_read`

- 単位: カウント/秒
- タイプ: 平均
- 説明: FE編集ログの読み取り速度。

## `fe_edit_log_retained`

- 単位: カウント
- タイプ: 累積
- 説明: 最後に有効なクリーンアップが行われて以降、BDB JE に保持されている FE メタデータジャーナルの件数です。ジャーナルの書き込みに応じて加算され、チェックポイントのラウンドが実際に journal database を削除したときにのみ基準が引き直され、その時点で BDB JE が保持しているジャーナル件数に設定されます。チェックポイントデーモンは登録済みのすべての FE が新しいイメージを受け取った後にのみ古いジャーナルを削除するため、到達不能な登録済み FE があるとこの値は増え続け、メタデータディスクが逼迫していることの早期警告となります。FE プロセスが再起動すると 0 から再開します。また、ストレージ・コンピュート分離モードで同じ BDB JE 環境を共有する StarMgr ジャーナルは含まず、FE メタデータジャーナルのみを対象とします。

## `fe_edit_log_retained_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: 最後に有効なクリーンアップが行われて以降、BDB JE に保持されている FE メタデータジャーナルのバイト数です。加算と基準の引き直しの挙動は [`fe_edit_log_retained`](#fe_edit_log_retained) と同じですが、クリーンアップ成功時には残量に設定されるのではなく 0 に戻ります。ディスク使用量の正確な値ではなく増加傾向の観察に使用してください。前回のクリーンアップ後に残ったジャーナルや、BDB JE がまだ回収していない領域は含まれず、FE プロセスの再起動時に 0 から再開します。

## `fe_edit_log_size_bytes`

- 単位: バイト/秒
- タイプ: 平均
- 説明: FE編集ログのサイズ。

## `fe_edit_log_write`

- 単位: バイト/秒
- タイプ: 平均
- 説明: FE編集ログの書き込み速度。

## `fe_finished_broker_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 完了したブローカージョブの数。

## `fe_finished_delete_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 完了した削除ジョブの数。

## `fe_finished_hadoop_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 完了したHadoopジョブの数。

## `fe_finished_insert_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 完了した挿入ジョブの数。

## `fe_image_push`

- 単位: カウント
- タイプ: 累積
- ラベル: `type` (`success` または `failed`)
- 説明: Leader FE が新しく生成したイメージファイルを他の FE ノードにプッシュした回数。試行ごとに対象ノード単位で計上されます。すべてのノードが新しいイメージを受け取った後にのみ古い編集ログが削除されるため、`failed` 系列が増え続けている場合は `fe_edit_log{type="current"}` が下がらない理由を説明できます。

## `fe_image_write`

- 単位: カウント
- タイプ: 累積
- ラベル: `type` (`success` または `failed`)
- 説明: イメージ生成の試行回数。新しいイメージの生成が実際に必要なチェックポイントのラウンドごとに 1 回計上されます。イメージが既に最新で生成が不要なラウンドは計上されません。

## `fe_loading_broker_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: ロード中のブローカージョブの数。

## `fe_loading_delete_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: ロード中の削除ジョブの数。

## `fe_loading_hadoop_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: ロード中のHadoopジョブの数。

## `fe_loading_insert_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: ロード中の挿入ジョブの数。

## `fe_pending_broker_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 保留中のブローカージョブの数。

## `fe_pending_delete_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 保留中の削除ジョブの数。

## `fe_pending_hadoop_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 保留中のHadoopジョブの数。

## `fe_pending_insert_load_job`

- 単位: カウント
- タイプ: 平均
- 説明: 保留中の挿入ジョブの数。

## `fe_rollup_running_alter_job`

- 単位: カウント
- タイプ: 平均
- 説明: ロールアップで作成されたジョブの数。

## `fe_schema_change_running_job`

- 単位: カウント
- タイプ: 平均
- 説明: スキーマ変更中のジョブの数。

## `float_column_pool_bytes`

- 単位: バイト
- 説明: FLOAT列プールによって使用されるメモリ。

## `fragment_endpoint_count`

- 単位: カウント
- 説明: BEでExchange送信者として機能するインスタンスの累積数。

## `fragment_request_duration_us`

- 単位: us
- 説明: フラグメントインスタンスの累積実行時間（非パイプラインエンジン用）。

## `fragment_requests_total`

- 単位: カウント
- 説明: BEで実行中のフラグメントインスタンスの総数（非パイプラインエンジン用）。

## `hive_write_bytes`

- 単位: バイト
- タイプ: 累積
- ラベル: `write_type` (`insert` または `overwrite`)
- 説明: Hive書き込みタスク (`INSERT`, `INSERT OVERWRITE`) から書き込まれた合計バイト数。これは、Hiveテーブルに書き込まれたデータファイルの合計サイズを表します。`write_type` は操作タイプを区別します。

## `hive_write_duration_ms_total`

- 単位: ミリ秒
- タイプ: 累積
- ラベル: `write_type` (`insert` または `overwrite`)
- 説明: Hive書き込みタスク (`INSERT`, `INSERT OVERWRITE`) の合計実行時間（ミリ秒）。各タスクの期間は、終了後に加算されます。`write_type` は操作タイプを区別します。

## `hive_write_files`

- 単位: カウント
- タイプ: 累積
- ラベル: `write_type` (`insert` または `overwrite`)
- 説明: 書き込みタスク (`INSERT`, `INSERT OVERWRITE`) からHiveに書き込まれたデータファイルの総数。これは、Hiveテーブルに書き込まれたデータファイルの数を表します。`write_type` は操作タイプを区別します。

## `hive_write_rows`

- 単位: 行
- タイプ: 累積
- ラベル: `write_type` (`insert` または `overwrite`)
- 説明: Hive書き込みタスク (`INSERT`, `INSERT OVERWRITE`) から書き込まれた合計行数。これは、Hiveテーブルに書き込まれた行数を表します。`write_type` は操作タイプを区別します。

## `hive_write_total`

- 単位: カウント
- タイプ: 累積
- ラベル:
  - `status`（`success` または `failed`）
  - `reason`（`none`、`timeout`、`oom`、`access_denied`、`unknown`）
  - `write_type`（`insert` または `overwrite`）
- 説明: Hiveテーブルをターゲットとする`INSERT`または`INSERT OVERWRITE`タスクの総数。このメトリックは、各タスクが成功または失敗にかかわらず終了した後、1ずつ増加します。`write_type`は操作タイプを区別します。

## `http_request_send_bytes (Deprecated)`

## `http_requests_total (Deprecated)`
