---
displayed_sidebar: docs
---

# materialized_views

`materialized_views` は、すべてのマテリアライズドビューに関する情報を提供します。

`materialized_views` には次のフィールドが含まれています:

| **Field**                            | **Description**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | マテリアライズドビューの ID。                                |
| TABLE_SCHEMA                         | マテリアライズドビューが存在するデータベース。               |
| TABLE_NAME                           | マテリアライズドビューの名前。                               |
| REFRESH_TYPE                         | マテリアライズドビューのリフレッシュタイプ。 有効な値: `ROLLUP` (同期マテリアライズドビュー), `ASYNC` (非同期リフレッシュマテリアライズドビュー), および `MANUAL` (手動リフレッシュマテリアライズドビュー)。値が `ROLLUP` の場合、アクティベーションステータスとリフレッシュに関連するすべてのフィールドは空です。 |
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
| LAST_FRESHNESS_CONFIRMED_AT          | 最後に成功した更新の開始時刻。更新全体（そのすべてのタスク実行）が完了した時点で記録されます。ベーステーブルに変更がなく更新不要と確認された場合も新鮮さが確認されます。マテリアライズドビューはこの時点のベーステーブルのデータを反映します。`LAST_REFRESH_TIME`（ベーステーブルのデータバージョン時刻）とは異なり、これは実時刻です。最初の更新が成功するまで、および同期マテリアライズドビューの場合は `NULL`。パーティション範囲を指定した REFRESH（部分更新）では値は進みません。 |
