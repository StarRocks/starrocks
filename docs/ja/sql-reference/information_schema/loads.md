---
displayed_sidebar: docs
---

# ロード

`loads` はロードジョブの結果を提供します。このビューは StarRocks v3.1 以降でサポートされています。現在、このビューから [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) と [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) ジョブの結果のみを表示できます。

`loads` には以下のフィールドが提供されています:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | StarRocks によってロードジョブを識別するために割り当てられた一意の ID。 |
| LABEL                | ロードジョブのラベル。                                       |
| DATABASE_NAME        | 宛先の StarRocks テーブルが属するデータベースの名前。          |
| STATE                | ロードジョブの状態。 有効な値:<ul><li>`PENDING`: ロードジョブが作成されました。</li><li>`QUEUEING`: ロードジョブがスケジュール待ちのキューにあります。</li><li>`LOADING`: ロードジョブが実行中です。</li><li>`PREPARED`: トランザクションがコミットされました。</li><li>`FINISHED`: ロードジョブが成功しました。</li><li>`CANCELLED`: ロードジョブが失敗しました。</li></ul> |
| PROGRESS             | ロードジョブの ETL ステージと LOADING ステージの進捗。         |
| TYPE                 | ロードジョブのタイプ。 Broker Load の場合、返される値は `BROKER` です。INSERT の場合、返される値は `INSERT` です。 |
| PRIORITY             | ロードジョブの優先度。 有効な値: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, `LOWEST`。 |
| SCAN_ROWS            | スキャンされたデータ行の数。                                 |
| FILTERED_ROWS        | データ品質が不十分なためにフィルタリングされたデータ行の数。   |
| UNSELECTED_ROWS      | WHERE 句で指定された条件によりフィルタリングされたデータ行の数。 |
| SINK_ROWS            | ロードされたデータ行の数。                                   |
| ETL_INFO             | ロードジョブの ETL 詳細。 Spark Load の場合のみ非空の値が返されます。他のタイプのロードジョブの場合、空の値が返されます。 |
| TASK_INFO            | ロードジョブのタスク実行詳細、例えば `timeout` や `max_filter_ratio` の設定。 |
| CREATE_TIME          | ロードジョブが作成された時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| ETL_START_TIME       | ロードジョブの ETL ステージの開始時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| ETL_FINISH_TIME      | ロードジョブの ETL ステージの終了時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | ロードジョブの LOADING ステージの開始時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | ロードジョブの LOADING ステージの終了時刻。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| JOB_DETAILS          | ロードされたデータの詳細、例えばバイト数やファイル数。         |
| ERROR_MSG            | ロードジョブのエラーメッセージ。エラーが発生しなかった場合、`NULL` が返されます。 |
| TRACKING_URL         | ロードジョブで検出された不適格なデータ行サンプルにアクセスできる URL。`curl` または `wget` コマンドを使用して URL にアクセスし、不適格なデータ行サンプルを取得できます。不適格なデータが検出されなかった場合、`NULL` が返されます。 |
| TRACKING_SQL         | ロードジョブの追跡ログをクエリするために使用できる SQL ステートメント。不適格なデータ行が含まれる場合のみ SQL ステートメントが返されます。不適格なデータ行が含まれない場合、`NULL` が返されます。 |
| REJECTED_RECORD_PATH | ロードジョブでフィルタリングされたすべての不適格なデータ行にアクセスできるパス。ログに記録される不適格なデータ行の数は、ロードジョブで設定された `log_rejected_record_num` パラメータによって決まります。`wget` コマンドを使用してパスにアクセスできます。不適格なデータ行が含まれない場合、`NULL` が返されます。 |