---
displayed_sidebar: docs
---

# ロード

`loads` はロードジョブの結果を提供します。このビューは StarRocks v3.1 以降でサポートされています。

`loads` には以下のフィールドが提供されています:

| フィールド                | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| ID                   | グローバルに一意の識別子。                                  |
| LABEL                | ロードジョブのラベル。                                       |
| PROFILE_ID           | `ANALYZE PROFILE` を通じて分析できるプロファイルの ID。 |
| DB_NAME              | 対象テーブルが属するデータベース。              |
| TABLE_NAME           | 対象テーブル。                                            |
| USER                 | ロードジョブを開始したユーザー。                         |
| WAREHOUSE            | ロードジョブが属するウェアハウス。                 |
| STATE                | ロードジョブの状態。 有効な値:<ul><li>`PENDING`/`BEGIN`: ロードジョブが作成された。</li><li>`QUEUEING`/`BEFORE_LOAD`: ロードジョブがスケジュール待ちのキューにある。</li><li>`LOADING`: ロードジョブが実行中。</li><li>`PREPARING`: トランザクションが事前コミットされている。</li><li>`PREPARED`: トランザクションが事前コミットされた。</li><li>`COMMITED`: トランザクションがコミットされた。</li><li>`FINISHED`: ロードジョブが成功した。</li><li>`CANCELLED`: ロードジョブが失敗した。</li></ul> |
| PROGRESS             | ロードジョブの ETL ステージと LOADING ステージの進捗。 |
| TYPE                 | ロードジョブのタイプ。 Broker Load の場合、返される値は `BROKER`。INSERT の場合、返される値は `INSERT`。Stream Load の場合、返される値は `STREAM`。Routine Load の場合、返される値は `ROUTINE`。 |
| PRIORITY             | ロードジョブの優先度。 有効な値: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, `LOWEST`。 |
| SCAN_ROWS            | スキャンされたデータ行の数。                    |
| SCAN_BYTES           | スキャンされたバイト数。                        |
| FILTERED_ROWS        | データ品質が不十分なためにフィルタリングされたデータ行の数。 |
| UNSELECTED_ROWS      | WHERE 句で指定された条件によりフィルタリングされたデータ行の数。 |
| SINK_ROWS            | ロードされたデータ行の数。                     |
| RUNTIME_DETAILS      | ロードの実行時メタデータ。詳細は [RUNTIME_DETAILS](#runtime_details) を参照。 |
| CREATE_TIME          | ロードジョブが作成された時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | ロードジョブの LOADING ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_COMMIT_TIME     | ロードトランザクションがコミットされた時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | ロードジョブの LOADING ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| PROPERTIES           | ロードジョブの静的プロパティ。詳細は [PROPERTIES](#properties) を参照。 |
| ERROR_MSG            | ロードジョブのエラーメッセージ。エラーが発生しなかった場合、`NULL` が返されます。 |
| TRACKING_SQL         | ロードジョブの追跡ログをクエリするために使用できる SQL ステートメント。ロードジョブが不適格なデータ行を含む場合にのみ SQL ステートメントが返されます。不適格なデータ行を含まない場合、`NULL` が返されます。 |
| REJECTED_RECORD_PATH | ロードジョブでフィルタリングされたすべての不適格なデータ行にアクセスできるパス。ログに記録される不適格なデータ行の数は、ロードジョブで設定された `log_rejected_record_num` パラメータによって決まります。このパスにアクセスするには `wget` コマンドを使用できます。不適格なデータ行を含まない場合、`NULL` が返されます。 |

## RUNTIME_DETAILS

- 共通メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | ロード実行計画のグローバルに一意の ID。               |
| txn_id               | ロードトランザクション ID。                                         |

- Broker Load、INSERT INTO、Spark Load の特定メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL 詳細。このフィールドは Spark Load ジョブにのみ有効です。他のタイプのロードジョブでは、値は空になります。 |
| etl_start_time       | ロードジョブの ETL ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| etl_start_time       | ロードジョブの ETL ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| unfinished_backends  | 実行が完了していない BEs のリスト。                      |
| backends             | 実行に参加している BEs のリスト。                      |
| file_num             | 読み取られたファイルの数。                                        |
| file_size            | 読み取られたファイルの合計サイズ。                                    |
| task_num             | サブタスクの数。                                          |

- Routine Load の特定メトリクス:

| メトリック               | 説明                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | Routine Load がスケジュールされる間隔。               |
| wait_slot_time       | Routine Load タスクが実行スロットを待機している間に経過した時間。 |
| check_offset_time    | Routine Load タスクのスケジューリング中にオフセット情報を確認する際に消費される時間。 |
| consume_time         | Routine Load タスクが上流データを読み取るのに消費する時間。 |
| plan_time            | 実行計画を生成する時間。                      |
| commit_publish_time  | COMMIT RPC を実行するのに消費される時間。                     |

- Stream Load の特定メトリクス:

| メトリック                 | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | ロードタスクのタイムアウト。                                    |
| begin_txn_ms           | トランザクションを開始するのに消費される時間。                    |
| plan_time_ms           | 実行計画を生成する時間。                    |
| receive_data_time_ms   | データを受信する時間。                                   |
| commit_publish_time_ms | COMMIT RPC を実行するのに消費される時間。                   |
| client_ip              | クライアントの IP アドレス。                                         |

## PROPERTIES

- Broker Load、INSERT INTO、Spark Load の特定プロパティ:

| プロパティ               | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | ロードタスクのタイムアウト。                                    |
| max_filter_ratio       | データ品質が不十分なためにフィルタリングされるデータ行の最大比率。 |

- Routine Load の特定プロパティ:

| プロパティ               | 説明                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load ジョブ名。                                     |
| task_num               | 実際に並行して実行されるサブタスクの数。          |
| timeout                | ロードタスクのタイムアウト。                                    |
