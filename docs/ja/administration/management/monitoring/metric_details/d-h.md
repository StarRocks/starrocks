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

## `default_cache_hit_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: データキャッシュインスタンス `default_cache` でキャッシュヒットした際に読み取った累積バイト数。共有データクラスタで [`datacache_unified_instance_enable`](../../../configuration/BE_parameters/shared_lake_other.md#datacache_unified_instance_enable) が `true`（デフォルト）の場合、クラウドネイティブテーブルのデータもこのインスタンスにキャッシュされます。クラウドネイティブテーブルについては、`fslib_open_cache_hits` と同じ読み取りを集計します。同じノード上の外部テーブルのデータキャッシュ読み取りも含まれます。`datacache_unified_instance_enable` が `false` の場合、クラウドネイティブテーブルは別のキャッシュインスタンスを使用し、そのメトリクスは `cloud_cache_hit_bytes`、`cloud_cache_miss_bytes`、`cloud_cache_hit_count`、`cloud_cache_miss_count` として出力されます。このメトリクスは BE 設定項目 `dump_metrics_with_bvar` が `true`（デフォルト）の場合にのみ出力されます。Prometheus の `gauge` タイプとして出力されますが、値は BE または CN が再起動するまで増加し続けるため、`rate()` を使用して計算してください。バイト数ベースのキャッシュヒット率は `sum(rate(default_cache_hit_bytes[5m])) / (sum(rate(default_cache_hit_bytes[5m])) + sum(rate(default_cache_miss_bytes[5m])))` です。`fslib_open_cache_hits` と `fslib_open_cache_misses` から計算する回数ベースのヒット率とは異なり、小さな読み取りのミスが多数発生してもほとんど下がりません。

## `default_cache_hit_count`

- 単位: カウント
- タイプ: 累積
- 説明: データキャッシュインスタンス `default_cache` の累積キャッシュヒット数。クラウドネイティブテーブルについては、`fslib_open_cache_hits` と同じ読み取りを集計します。集計範囲と出力条件については `default_cache_hit_bytes` を参照してください。

## `default_cache_miss_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: データキャッシュインスタンス `default_cache` でキャッシュミスした際に読み取りを要求した累積バイト数。読み取りで要求されたサイズを計上し、その後オブジェクトストレージから読み取るサイズ（キャッシュブロック単位に揃えられます）ではありません。キャッシュ内のファイルサイズエントリがミスした場合は 0 を計上します。集計範囲と出力条件については `default_cache_hit_bytes` を参照してください。

## `default_cache_miss_count`

- 単位: カウント
- タイプ: 累積
- 説明: データキャッシュインスタンス `default_cache` の累積キャッシュミス数。クラウドネイティブテーブルについては、`fslib_open_cache_misses` と同じ読み取りを集計します。集計範囲と出力条件については `default_cache_hit_bytes` を参照してください。

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

## `fslib_open_cache_hits`

- 単位: カウント
- タイプ: カウンター
- ラベル: `fstype` (`cachefs`)
- 説明: 共有データクラスタでクラウドネイティブテーブルのファイルを読み取る際の、データキャッシュの累積ヒット数。1 回の読み取りは最大で 1 つのキャッシュブロックを対象とします。読み取りはサイズに関係なく 1 回として数えられるため、メタデータの読み取りなど小さな読み取りのミスが多数発生すると、大部分のバイトがキャッシュから読み取られていても、このメトリクスと `fslib_open_cache_misses` から計算するヒット率は下がります。バイト数ベースのメトリクスについては `default_cache_hit_bytes` を参照してください。

## `fslib_open_cache_misses`

- 単位: カウント
- タイプ: カウンター
- ラベル: `fstype` (`cachefs`)
- 説明: 共有データクラスタでクラウドネイティブテーブルのファイルを読み取る際の、データキャッシュの累積ミス数。1 回の読み取りは最大で 1 つのキャッシュブロックを対象とします。バイト数ベースのメトリクスについては `default_cache_miss_bytes` を参照してください。

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
