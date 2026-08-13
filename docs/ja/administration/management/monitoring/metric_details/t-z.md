---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical t - z"
---

# メトリクス t から z

## `tablet_base_max_compaction_score`

- 単位: -
- 説明: このBE内のタブレットの最高ベースコンパクションスコア。

## `tablet_cumulative_max_compaction_score`

- 単位: -
- 説明: このBE内のタブレットの最高累積コンパクションスコア。

## `tablet_metadata_mem_bytes`

- 単位: バイト
- 説明: タブレットメタデータが使用するメモリ。

## `tablet_schema_mem_bytes`

- 単位: バイト
- 説明: タブレットスキーマが使用するメモリ。

## `tablet_update_max_compaction_score`

- 単位: -
- 説明: 現在のBEにおけるプライマリキーテーブル内のタブレットの最高コンパクションスコア。

## `threadpool_task_exception_total`

- 単位: カウント
- 説明: BE プロセス内のすべての ThreadPool ワーカースレッドがキャッチして飲み込んだタスク例外の累計回数。[`enable_threadpool_catch_task_exception`](../../../configuration/BE_parameters/log_server_meta.md#enable_threadpool_catch_task_exception) が `true` の場合にのみ増加します。この項目が `false`（デフォルト）のときは外層の catch 句がないため、このメトリクスは変化しません。catch モード有効時のアラートに利用できます。プール名および例外の詳細は BE の ERROR ログに記録されます。

## `thrift_connections_total`

- 単位: カウント
- 説明: thrift接続の合計数（完了した接続を含む）。

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- 単位: カウント
- 説明: 現在開かれているthriftクライアントの数。

## `thrift_used_clients`

- 単位: カウント
- 説明: 現在使用中のthriftクライアントの数。

## `total_column_pool_bytes (Deprecated)`

## `transaction_streaming_load_bytes`

- 単位: バイト
- 説明: トランザクションロードの合計ロードバイト数。

## `transaction_streaming_load_current_processing`

- 単位: カウント
- 説明: 現在実行中のトランザクションStream Loadタスクの数。

## `transaction_streaming_load_duration_ms`

- 単位: ms
- 説明: Stream Loadトランザクションインターフェースに費やされた合計時間。

## `transaction_streaming_load_requests_total`

- 単位: カウント
- 説明: トランザクションロードリクエストの合計数。

## `txn_request`

- 単位: -
- 説明: BEGIN、COMMIT、ROLLBACK、EXECのトランザクションリクエスト。

## `uint8_column_pool_bytes`

- 単位: バイト
- 説明: UINT8カラムプールが使用するバイト数。

## `unused_rowsets_count`

- 単位: カウント
- 説明: 未使用の行セットの合計数。これらの行セットは後で再利用されます。

## `update_apply_queue_count`

- 単位: カウント
- 説明: プライマリキーテーブルトランザクションAPPLYスレッドプール内のキューイングされたタスク数。

## `update_compaction_duration_us`

- 単位: us
- Description: Primary Keyテーブルのコンパクションに費やされた合計時間。

## `update_compaction_outputs_bytes_total`

- Unit: バイト
- Description: Primary Keyテーブルのコンパクションによって書き込まれた合計バイト数。

## `update_compaction_outputs_total`

- Unit: カウント
- Description: Primary Keyテーブルのコンパクションの合計数。

## `update_compaction_task_byte_per_second`

- Unit: バイト/秒
- Description: Primary Keyテーブルのコンパクションの推定レート。

## `update_compaction_task_cost_time_ns`

- Unit: ns
- Description: Primary Keyテーブルのコンパクションに費やされた合計時間。

## `update_del_vector_bytes_total`

- Unit: バイト
- Description: Primary KeyテーブルでDELETEベクトルをキャッシュするために使用された合計メモリ。

## `update_del_vector_deletes_new`

- Unit: カウント
- Description: Primary Keyテーブルで使用された新しく生成されたDELETEベクトルの合計数。

## `update_del_vector_deletes_total (Deprecated)`

## `update_del_vector_dels_num (Deprecated)`

## `update_del_vector_num`

- Unit: カウント
- Description: Primary Keyテーブル内のDELETEベクトルキャッシュアイテムの数。

## `update_mem_bytes`

- Unit: バイト
- Description: Primary KeyテーブルのAPPLYタスクとPrimary Keyインデックスによって使用されるメモリ。

## `update_primary_index_bytes_total`

- Unit: バイト
- Description: Primary Keyインデックスの合計メモリコスト。

## `update_primary_index_num`

- Unit: カウント
- Description: メモリにキャッシュされたPrimary Keyインデックスの数。

## `update_rowset_commit_apply_duration_us`

- Unit: us
- Description: Primary KeyテーブルのAPPLYタスクに費やされた合計時間。

## `update_rowset_commit_apply_total`

- Unit: カウント
- Description: Primary KeyテーブルのCOMMITおよびAPPLYの合計数。

## `update_rowset_commit_request_failed`

- Unit: カウント
- Description: Primary Keyテーブルでの失敗した行セットCOMMITリクエストの合計数。

## `update_rowset_commit_request_total`

- Unit: カウント
- Description: Primary Keyテーブルでの行セットCOMMITリクエストの合計数。

## `vector_index_cache_async_load_failure`

- タイプ: 累積
- 単位: カウント
- 説明: 実行を開始したものの、ロードまたはキャッシュへの公開中に失敗したベクターインデックスキャッシュのバックグラウンドロードタスクの累計数です。実行前にキャンセルされたタスクは含まれません。

## `vector_index_cache_async_load_inflight`

- タイプ: ゲージ
- 単位: カウント
- 説明: バックグラウンド worker で現在実行中のベクターインデックスキャッシュロードタスク数です。

## `vector_index_cache_async_load_ns`

- タイプ: 累積
- 単位: ナノ秒
- 説明: 実行を開始したベクターインデックスキャッシュのバックグラウンドロードタスクの累積実行時間です。成功したタスクと失敗したタスクを含み、キューでの待機時間および拒否されたタスクは含みません。

## `vector_index_cache_async_load_queued`

- タイプ: ゲージ
- 単位: カウント
- 説明: バックグラウンドプールに受け付けられたものの、まだ実行を開始していないベクターインデックスキャッシュロードタスク数です。

## `vector_index_cache_async_load_rejected`

- タイプ: 累積
- 単位: カウント
- 説明: 実行前に拒否されたベクターインデックスキャッシュのバックグラウンドロード要求の累計数です。たとえば、キャッシュ容量がゼロ、プールが停止済み、またはキューがタスクを受け付けられない場合に増加します。

## `vector_index_cache_async_load_success`

- タイプ: 累積
- 単位: カウント
- 説明: インデックスのロードとキャッシュへの公開に成功したバックグラウンドタスクの累計数です。キャッシュがエントリを保持できない場合、容量制御によって公開直後のエントリが削除されることがあります。

## `vector_index_cache_loading_wait_timeout`

- タイプ: 累積
- 単位: カウント
- 説明: 同期キャッシュ呼び出し元が、進行中のベクターインデックスロードを `vector_index_cache_loading_wait_timeout_ms` まで待機した累計回数です。このメトリクスは一意のインデックス数ではなく呼び出し元ごとにカウントされ、タイムアウト後も既存の loader は実行を継続します。

## `wait_base_compaction_task_num`

- Unit: カウント
- Description: 実行を待機しているベースコンパクションタスクの数。

## `wait_cumulative_compaction_task_num`

- Unit: カウント
- Description: 実行を待機している累積コンパクションタスクの数。

## `writable_blocks_total (Deprecated)`
