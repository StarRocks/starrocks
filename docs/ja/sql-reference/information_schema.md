---
displayed_sidebar: docs
---

# Information Schema

StarRocks の `information_schema` は、各 StarRocks インスタンス内のデータベースです。`information_schema` には、StarRocks インスタンスが管理するすべてのオブジェクトの広範なメタデータ情報を格納する、いくつかの読み取り専用のシステム定義テーブルが含まれています。

## Information Schema を介したメタデータの表示

StarRocks インスタンス内のメタデータ情報は、`information_schema` のテーブルの内容をクエリすることで表示できます。

次の例では、StarRocks のテーブル `sr_member` に関するメタデータ情報を、テーブル `tables` をクエリすることで表示します。

```Plain
mysql> SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'sr_member'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: sr_hub
     TABLE_NAME: sr_member
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: NULL
     TABLE_ROWS: 6
 AVG_ROW_LENGTH: 542
    DATA_LENGTH: 3255
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2022-11-17 14:32:30
    UPDATE_TIME: 2022-11-17 14:32:55
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: NULL
  TABLE_COMMENT: OLAP
1 row in set (1.04 sec)
```

## Information Schema テーブル

StarRocks は、`tables`、`tables_config`、`load_tracking_logs` テーブルによって提供されるメタデータ情報を最適化し、v3.1 以降の `information_schema` に `loads` テーブルを提供しています。

| **Information Schema テーブル名** | **説明**                                              |
| --------------------------------- | ------------------------------------------------------------ |
| [tables](#tables)                            | テーブルの一般的なメタデータ情報を提供します。             |
| [tables_config](#tables_config)                     | StarRocks に固有の追加のテーブルメタデータ情報を提供します。 |
| [load_tracking_logs](#load_tracking_logs)                | ロードジョブのエラー情報（ある場合）を提供します。 |
| [loads](#loads)                             | ロードジョブの結果を提供します。このテーブルは v3.1 以降でサポートされています。現在、このテーブルから [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) と [Insert](../sql-reference/sql-statements/loading_unloading/INSERT.md) ジョブの結果のみを表示できます。                 |

### loads

`loads` には次のフィールドが提供されています。

| **フィールド**            | **説明**                                              |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | ロードジョブを識別するために StarRocks によって割り当てられた一意の ID。 |
| LABEL                | ロードジョブのラベル。                                   |
| DATABASE_NAME        | 宛先の StarRocks テーブルが属するデータベースの名前。 |
| STATE                | ロードジョブの状態。 有効な値:<ul><li>`PENDING`: ロードジョブが作成されました。</li><li>`QUEUEING`: ロードジョブがスケジュール待ちのキューにあります。</li><li>`LOADING`: ロードジョブが実行中です。</li><li>`PREPARED`: トランザクションがコミットされました。</li><li>`FINISHED`: ロードジョブが成功しました。</li><li>`CANCELLED`: ロードジョブが失敗しました。</li></ul>詳細は [Asynchronous loading](../loading/Loading_intro.md#asynchronous-loading) を参照してください。 |
| PROGRESS             | ロードジョブの ETL ステージと LOADING ステージの進行状況。 |
| TYPE                 | ロードジョブのタイプ。 Broker Load の場合、返される値は `BROKER` です。INSERT の場合、返される値は `INSERT` です。 |
| PRIORITY             | ロードジョブの優先度。 有効な値: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, `LOWEST`。 |
| SCAN_ROWS            | スキャンされたデータ行の数。                    |
| FILTERED_ROWS        | データ品質が不十分なためにフィルタリングされたデータ行の数。 |
| UNSELECTED_ROWS      | WHERE 句で指定された条件によりフィルタリングされたデータ行の数。 |
| SINK_ROWS            | ロードされたデータ行の数。                     |
| ETL_INFO             | ロードジョブの ETL 詳細。Spark Load の場合のみ値が返されます。他のタイプのロードジョブの場合、空の値が返されます。 |
| TASK_INFO            | ロードジョブのタスク実行詳細、例えば `timeout` や `max_filter_ratio` 設定など。 |
| CREATE_TIME          | ロードジョブが作成された時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| ETL_START_TIME       | ロードジョブの ETL ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| ETL_FINISH_TIME      | ロードジョブの ETL ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | ロードジョブの LOADING ステージの開始時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | ロードジョブの LOADING ステージの終了時間。フォーマット: `yyyy-MM-dd HH:mm:ss`。例: `2023-07-24 14:58:58`。 |
| JOB_DETAILS          | ロードされたデータの詳細、例えばバイト数やファイル数など。 |
| ERROR_MSG            | ロードジョブのエラーメッセージ。エラーが発生しなかった場合、`NULL` が返されます。 |
| TRACKING_URL         | ロードジョブで検出された不適格なデータ行サンプルにアクセスできる URL。`curl` または `wget` コマンドを使用して URL にアクセスし、不適格なデータ行サンプルを取得できます。不適格なデータが検出されなかった場合、`NULL` が返されます。 |
| TRACKING_SQL         | ロードジョブのトラッキングログをクエリするために使用できる SQL ステートメント。ロードジョブに不適格なデータ行が含まれる場合のみ SQL ステートメントが返されます。不適格なデータ行が含まれない場合、`NULL` が返されます。 |
| REJECTED_RECORD_PATH | ロードジョブでフィルタリングされたすべての不適格なデータ行にアクセスできるパス。ログに記録される不適格なデータ行の数は、ロードジョブで設定された `log_rejected_record_num` パラメータによって決まります。`wget` コマンドを使用してパスにアクセスできます。不適格なデータ行が含まれない場合、`NULL` が返されます。 |

### tables

`tables` には次のフィールドが提供されています。

| **フィールド**       | **説明**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | テーブルを格納するカタログの名前。                   |
| TABLE_SCHEMA    | テーブルを格納するデータベースの名前。                  |
| TABLE_NAME      | テーブルの名前。                                           |
| TABLE_TYPE      | テーブルのタイプ。有効な値: "BASE TABLE" または "VIEW"。     |
| ENGINE          | テーブルのエンジンタイプ。有効な値: "StarRocks", "MySQL", "MEMORY" または空の文字列。 |
| VERSION         | StarRocks で利用できない機能に適用されます。             |
| ROW_FORMAT      | StarRocks で利用できない機能に適用されます。             |
| TABLE_ROWS      | テーブルの行数。                                      |
| AVG_ROW_LENGTH  | テーブルの平均行長（サイズ）。これは `DATA_LENGTH` / `TABLE_ROWS` に相当します。単位: バイト。 |
| DATA_LENGTH     | テーブルのデータ長（サイズ）。単位: バイト。                 |
| MAX_DATA_LENGTH | StarRocks で利用できない機能に適用されます。             |
| INDEX_LENGTH    | StarRocks で利用できない機能に適用されます。             |
| DATA_FREE       | StarRocks で利用できない機能に適用されます。             |
| AUTO_INCREMENT  | StarRocks で利用できない機能に適用されます。             |
| CREATE_TIME     | テーブルが作成された時間。                          |
| UPDATE_TIME     | テーブルが最後に更新された時間。                     |
| CHECK_TIME      | テーブルに対して整合性チェックが最後に実行された時間。 |
| TABLE_COLLATION | テーブルのデフォルトの照合順序。                          |
| CHECKSUM        | StarRocks で利用できない機能に適用されます。             |
| CREATE_OPTIONS  | StarRocks で利用できない機能に適用されます。             |
| TABLE_COMMENT   | テーブルに関するコメント。                                        |

### tables_config

`tables_config` には次のフィールドが提供されています。

| **フィールド**        | **説明**                                              |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | テーブルを格納するデータベースの名前。                  |
| TABLE_NAME       | テーブルの名前。                                           |
| TABLE_ENGINE     | テーブルのエンジンタイプ。                                    |
| TABLE_MODEL      | テーブルタイプ。有効な値: "DUP_KEYS", "AGG_KEYS", "UNQ_KEYS", "PRI_KEYS"。 |
| PRIMARY_KEY      | 主キーテーブルまたはユニークキーテーブルの主キー。テーブルが主キーテーブルまたはユニークキーテーブルでない場合、空の文字列が返されます。 |
| PARTITION_KEY    | テーブルのパーティション列。                       |
| DISTRIBUTE_KEY   | テーブルのバケッティング列。                          |
| DISTRIBUTE_TYPE  | テーブルのデータ分散方法。                   |
| DISTRIBUTE_BUCKET | テーブルのバケット数。                              |
| SORT_KEY         | テーブルのソートキー。                                      |
| PROPERTIES       | テーブルのプロパティ。                                     |
| TABLE_ID         | テーブルの ID。                                             |

## load_tracking_logs

この機能は StarRocks v3.0 以降でサポートされています。

`load_tracking_logs` には次のフィールドが提供されています。

| **フィールド**     | **説明**                                                                       |
|---------------|---------------------------------------------------------------------------------------|
| JOB_ID        | ロードジョブの ID。                                                               |
| LABEL         | ロードジョブのラベル。                                                            |
| DATABASE_NAME | ロードジョブが属するデータベース。                                            |
| TRACKING_LOG  | ロードジョブのエラーログ（ある場合）。                                                  |
| Type          | ロードジョブのタイプ。有効な値: BROKER, INSERT, ROUTINE_LOAD, STREAM_LOAD。 |

## materialized_views

`materialized_views` には次のフィールドが提供されています。

| **フィールド**                            | **説明**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | マテリアライズドビューの ID                                  |
| TABLE_SCHEMA                         | マテリアライズドビューが存在するデータベース              |
| TABLE_NAME                           | マテリアライズドビューの名前                                |
| REFRESH_TYPE                         | マテリアライズドビューのリフレッシュタイプ。有効な値: `ROLLUP` (同期マテリアライズドビュー), `ASYNC` (非同期リフレッシュマテリアライズドビュー), `MANUAL` (手動リフレッシュマテリアライズドビュー)。値が `ROLLUP` の場合、アクティベーションステータスとリフレッシュに関連するすべてのフィールドは空です。  |
| IS_ACTIVE                            | マテリアライズドビューがアクティブかどうかを示します。非アクティブなマテリアライズドビューはリフレッシュまたはクエリできません。 |
| INACTIVE_REASON                      | マテリアライズドビューが非アクティブである理由            |
| PARTITION_TYPE                       | マテリアライズドビューのパーティション戦略のタイプ      |
| TASK_ID                              | マテリアライズドビューをリフレッシュするタスクの ID |
| TASK_NAME                            | マテリアライズドビューをリフレッシュするタスクの名前 |
| LAST_REFRESH_START_TIME              | 最新のリフレッシュタスクの開始時間                   |
| LAST_REFRESH_FINISHED_TIME           | 最新のリフレッシュタスクの終了時間                     |
| LAST_REFRESH_DURATION                | 最新のリフレッシュタスクの期間                     |
| LAST_REFRESH_STATE                   | 最新のリフレッシュタスクの状態                        |
| LAST_REFRESH_FORCE_REFRESH           | 最新のリフレッシュタスクが強制リフレッシュであったかどうかを示します |
| LAST_REFRESH_START_PARTITION         | 最新のリフレッシュタスクの開始パーティション          |
| LAST_REFRESH_END_PARTITION           | 最新のリフレッシュタスクの終了パーティション            |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | 最新のリフレッシュタスクに関与したベーステーブルのパーティション |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | 最新のリフレッシュタスクでリフレッシュされたマテリアライズドビューのパーティション |
| LAST_REFRESH_ERROR_CODE              | 最新のリフレッシュタスクのエラーコード                   |
| LAST_REFRESH_ERROR_MESSAGE           | 最新のリフレッシュタスクのエラーメッセージ                |
| TABLE_ROWS                           | バックグラウンド統計に基づくマテリアライズドビューのデータ行数 |
| MATERIALIZED_VIEW_DEFINITION         | マテリアライズドビューの SQL 定義                      |