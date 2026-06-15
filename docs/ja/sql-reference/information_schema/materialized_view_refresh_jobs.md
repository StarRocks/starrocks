---
displayed_sidebar: docs
description: "materialized_view_refresh_jobs はマテリアライズドビューのリフレッシュに関するジョブレベルの情報を提供します。"
---

# materialized_view_refresh_jobs

`materialized_view_refresh_jobs` は、マテリアライズドビューのリフレッシュに関するジョブレベルの情報を提供します。

単一のリフレッシュジョブは複数の task run（例えばパーティションごとにリフレッシュするバッチ）から構成される場合があります。このビューは、それらの task run をジョブごとに 1 行へ集約します。データソースは [`task_runs`](./task_runs.md) と共有しているため、`JOB_ID` を使ってジョブ内の個々の task run をドリルダウンして確認できます（`SELECT * FROM information_schema.task_runs WHERE JOB_ID = '<job_id>'`）。また、ジョブ記録の保持期間は `task_runs` の履歴と同じです。

`materialized_view_refresh_jobs` には以下のフィールドが含まれています:

| **Field**                          | **Description**                                              |
| ---------------------------------- | ------------------------------------------------------------ |
| JOB_ID                             | リフレッシュジョブの ID。1 回のリフレッシュに含まれるすべての task run がこの ID を共有します。これを使って `task_runs.JOB_ID` をドリルダウンできます。 |
| MATERIALIZED_VIEW_ID               | マテリアライズドビューの ID。                                |
| TABLE_SCHEMA                       | マテリアライズドビューが属するデータベース。                 |
| TABLE_NAME                         | マテリアライズドビューの名前。マテリアライズドビューが削除されている場合は `NULL`。 |
| TASK_ID                            | リフレッシュタスクの ID。                                    |
| WAREHOUSE                          | リフレッシュジョブが使用するウェアハウス。                   |
| RESOURCE_GROUP                     | リフレッシュジョブが使用するリソースグループ。これはマテリアライズドビューに設定された `resource_group` プロパティです。設定されていない場合は `default_mv_wg` を返します。 |
| CREATOR                            | マテリアライズドビューを作成したユーザー（create-user。実行時の identity は RUN_AS_USER を参照）。 |
| SUBMIT_USER                        | リフレッシュジョブを送信したユーザー。手動リフレッシュの場合はそれを発行したユーザーであり、定期リフレッシュやベーステーブル変更によるリフレッシュの場合はシステムによって送信されます。 |
| RUN_AS_USER                        | リフレッシュが実際に実行されるユーザー ID。creator-based 認可（デフォルト、`mv_use_creator_based_authorization=true`）では実体化ビューの作成者、root-based 認可では `'root'@'%'`。 |
| SUBMIT_TIME                        | ジョブが送信された時間（最初の task run の作成時間）。       |
| REFRESH_STATE                      | ジョブの状態。最後の task run から集約されます。有効な値: `PENDING`、`RUNNING`、`FAILED`、`SUCCESS`、`SKIPPED`。 |
| FINISH_TIME                        | ジョブが終了した時間。ジョブが終了していない場合は `NULL`。 |
| DURATION_TIME                      | ジョブの実時間での所要時間（秒単位。最後の task run の終了時間から最初の task run の処理開始時間を引いた値）。ジョブが終了していない場合は `NULL`。 |
| REFRESH_TRIGGER                    | このジョブがトリガーされた方法。手動で `REFRESH MATERIALIZED VIEW` を発行した場合は `MANUAL`（マテリアライズドビューのスキームが定期または自動であっても）。それ以外の場合はマテリアライズドビューに設定されたスキーム。有効な値: `MANUAL`、`SCHEDULED`、`ON_BASE_TABLE_CHANGE`、`NONE`。マテリアライズドビューが削除されていてジョブが手動でなかった場合は `UNKNOWN`。 |
| REFRESH_MODE                       | マテリアライズドビューに設定されたリフレッシュモード。有効な値: `AUTO`、`PCT`、`INCREMENTAL`。マテリアライズドビューが削除されている場合は `NULL`。 |
| IMV_SOURCE_VERSION_RANGE           | インクリメンタルリフレッシュが消費したソースバージョン範囲の JSON。非インクリメンタル（PCT）リフレッシュの場合、またはソース範囲が消費されなかった場合は `NULL` を返します。 |
| IMV_SOURCE_TIMESTAMP_RANGE         | インクリメンタルリフレッシュが消費したソースタイムスタンプ範囲の JSON。非インクリメンタル（PCT）リフレッシュの場合、またはソース範囲が消費されなかった場合は `NULL` を返します。 |
| IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP  | 固定されたソーススナップショット ID の JSON。その JSON キーは connector のテーブル識別子（Iceberg の場合は `<table>:<uuid>`）であり、IMV_SOURCE_VERSION_RANGE および IMV_SOURCE_TIMESTAMP_RANGE が使用する `<catalog>.<db>.<table>` キーとは異なります。baseline/PCT パスのリフレッシュで設定され、純粋なインクリメンタル実行の場合、またはスナップショットが固定されなかった場合は `NULL` を返します。 |
| FAILED_TASK_RUN_ID                 | ジョブ内で失敗した task run の ID。失敗した task run がない場合は `NULL`。`task_runs` にドリルダウンするには、`FAILED_QUERY_ID = task_runs.QUERY_ID`（または `JOB_ID`）で join してください。`task_runs` には task-run-id 列はありません。 |
| FAILED_QUERY_ID                    | 失敗した task run のクエリ ID。失敗した task run がない場合は `NULL`。 |
| ERROR_CODE                         | 失敗した task run のエラーコード。失敗した task run がない場合は `NULL`。 |
| ERROR_MESSAGE                      | 失敗した task run のエラーメッセージ。失敗した task run がない場合は `NULL`。 |

:::note
このビューは永続的なストレージを持ちません。その行はクエリ時に `task_runs` から導出されるため、記録の保持期間は `task_runs` の履歴設定に従います。
:::
