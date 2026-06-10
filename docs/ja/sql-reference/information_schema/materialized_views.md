---
displayed_sidebar: docs
description: "materialized_viewsはすべてのマテリアライズドビューに関する情報を提供します。"
---

# materialized_views

`materialized_views` は、すべてのマテリアライズドビューに関する情報を提供します。

`materialized_views` には次のフィールドが含まれています:

| **Field**                            | **Description**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | マテリアライズドビューの ID。                                |
| TABLE_SCHEMA                         | マテリアライズドビューが存在するデータベース。               |
| TABLE_NAME                           | マテリアライズドビューの名前。                               |
| REFRESH_TYPE                         | マテリアライズドビューのリフレッシュタイプ。有効な値: `SYNC` (同期マテリアライズドビュー) および `ASYNC` (非同期マテリアライズドビュー。リフレッシュのトリガー方法に関係なく)。値が `SYNC` の場合、アクティベーションステータスとリフレッシュに関連するすべてのフィールドは空です。非同期マテリアライズドビューのリフレッシュ方法については `REFRESH_TRIGGER` と `REFRESH_POLICY` を参照してください。 |
| IS_ACTIVE                            | マテリアライズドビューがアクティブかどうかを示します。 非アクティブなマテリアライズドビューはリフレッシュまたはクエリできません。 |
| INACTIVE_REASON                      | マテリアライズドビューが非アクティブである理由。             |
| PARTITION_TYPE                       | マテリアライズドビューのパーティショニング戦略のタイプ。     |
| TASK_ID                              | マテリアライズドビューをリフレッシュするタスクの ID。        |
| TASK_NAME                            | マテリアライズドビューをリフレッシュするタスクの名前。       |
| LAST_REFRESH_START_TIME              | 最新のリフレッシュタスクの開始時間。                         |
| LAST_REFRESH_FINISHED_TIME           | 最新のリフレッシュタスクの終了時間。                         |
| LAST_REFRESH_DURATION                | 最新のリフレッシュタスクの期間。                             |
| LAST_REFRESH_STATE                   | 最新のリフレッシュタスクの状態。                             |
| LAST_REFRESH_FORCE_REFRESH           | 最新のリフレッシュタスクが強制リフレッシュであったかどうかを示します。 |
| LAST_REFRESH_START_PARTITION         | 最新のリフレッシュタスクの開始パーティション。               |
| LAST_REFRESH_END_PARTITION           | 最新のリフレッシュタスクの終了パーティション。               |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | 最新のリフレッシュタスクに関与したベーステーブルのパーティション。 |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | 最新のリフレッシュタスクでリフレッシュされたマテリアライズドビューパーティション。 |
| LAST_REFRESH_ERROR_CODE              | 最新のリフレッシュタスクのエラーコード。                     |
| LAST_REFRESH_ERROR_MESSAGE           | 最新のリフレッシュタスクのエラーメッセージ。                 |
| TABLE_ROWS                           | マテリアライズドビュー内のデータ行数（おおよそのバックグラウンド統計に基づく）。 |
| MATERIALIZED_VIEW_DEFINITION         | マテリアライズドビューの SQL 定義。                          |
| EXTRA_MESSAGE                        | マテリアライズドビューの追加メッセージ。                     |
| QUERY_REWRITE_STATUS                 | マテリアライズドビューのクエリリライトステータス。           |
| CREATOR                              | マテリアライズドビューの作成者。                             |
| LAST_REFRESH_PROCESS_TIME            | 最新のリフレッシュタスクの処理時間。                         |
| LAST_REFRESH_JOB_ID                  | 最新のリフレッシュタスクのジョブ ID。                        |
| LAST_REFRESH_TIME                    | ベーステーブルの更新がマテリアライズドビューに反映されている最新の時間。 |
| WAREHOUSE                            | 非同期マテリアライズドビューがリフレッシュタスクに使用するウェアハウスの名前。ストレージ・コンピュート一体型モードの場合、または同期 (rollup) マテリアライズドビューの場合は空です。 |
| REFRESH_MODE                         | 非同期マテリアライズドビューに設定されたリフレッシュモード。有効な値: `PCT` (パーティション変更追跡。変更されたパーティションのみをリフレッシュ)、`INCREMENTAL` (インクリメンタルビューメンテナンス)、`AUTO`。同期マテリアライズドビューの場合は空です。 |
| REFRESH_TRIGGER                      | リフレッシュがトリガーされる方法。有効な値: `NONE` (同期マテリアライズドビュー)、`MANUAL` (REFRESH MATERIALIZED VIEW 経由のみ)、`SCHEDULED` (EVERY 間隔による定期実行)、`ON_BASE_TABLE_CHANGE` (ベーステーブルのロードまたは変更時に自動実行)。 |
| REFRESH_POLICY                       | 人間が読めるリフレッシュポリシー。有効な値: `NONE`、`MANUAL`、`ON_BASE_TABLE_CHANGE`、または `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` のようなスケジュール (`START` 句は開始時刻が定義されている場合にのみ含まれます)。 |
| RESOURCE_GROUP                       | マテリアライズドビューのリフレッシュタスクに使用されるリソースグループ (マテリアライズドビューの `resource_group` プロパティから)。設定されていない場合は `default_mv_wg` がデフォルトです。 |
| QUERY_REWRITE_STATUS_REASON          | `QUERY_REWRITE_STATUS` の理由。有効な値: `OK`、`MV_INACTIVE`、`QUERY_REWRITE_DISABLED`、`UNSUPPORTED_DEFINITION`、`UNKNOWN`。 |
