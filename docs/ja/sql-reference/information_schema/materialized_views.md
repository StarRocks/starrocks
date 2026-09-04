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
| LAST_REFRESH_DURATION                | 最新のリフレッシュの実時間（秒）。最後のタスク実行の終了時刻から最初のタスク実行の処理開始時刻を引いた値。そのジョブの `materialized_view_refresh_jobs.DURATION_TIME` と一致します。 |
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
| REFRESH_MODE                         | 非同期マテリアライズドビューに設定されたリフレッシュモード。有効な値: `PCT` (パーティション変更追跡。変更されたパーティションのみをリフレッシュ)、`INCREMENTAL` (インクリメンタルビューメンテナンス)、`AUTO` (可能な限りインクリメンタル。増分プランを構築できない変更が発生した場合は `PCT` にフォールバック)。同期マテリアライズドビューの場合は空です。 |
| REFRESH_TRIGGER                      | リフレッシュがトリガーされる方法。有効な値: `NONE` (同期マテリアライズドビュー)、`MANUAL` (REFRESH MATERIALIZED VIEW 経由のみ)、`SCHEDULED` (EVERY 間隔による定期実行)、`ON_BASE_TABLE_CHANGE` (ベーステーブルのロードまたは変更時に自動実行)。 |
| REFRESH_POLICY                       | 人間が読めるリフレッシュポリシー。有効な値: `NONE`、`MANUAL`、`ON_BASE_TABLE_CHANGE`、または `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` のようなスケジュール (`START` 句は開始時刻が定義されている場合にのみ含まれます)。 |
| RESOURCE_GROUP                       | マテリアライズドビューのリフレッシュタスクに使用されるリソースグループ (マテリアライズドビューの `resource_group` プロパティから)。設定されていない場合は `default_mv_wg` がデフォルトです。 |
| QUERY_REWRITE_STATUS_REASON          | `QUERY_REWRITE_STATUS` の理由。有効な値: `OK`、`MV_INACTIVE`、`QUERY_REWRITE_DISABLED`、`UNSUPPORTED_DEFINITION`、`UNKNOWN`。 |
| LAST_FRESHNESS_CONFIRMED_AT          | 最後に成功した更新の開始時刻。更新全体（そのすべてのタスク実行）が完了した時点で記録されます。ベーステーブルに変更がなく更新不要と確認された場合も新鮮さが確認されます。マテリアライズドビューはこの時点のベーステーブルのデータを反映します。`LAST_REFRESH_TIME`（ベーステーブルのデータバージョン時刻）とは異なり、これは実時刻です。最初の更新が成功するまで、および同期マテリアライズドビューの場合は `NULL`。パーティション範囲を指定した REFRESH（部分更新）では値は進みません。 |
| BASE_TABLE_REFRESH_VERSION_TIMES     | 各ベーステーブルのデータバージョン時刻を、ベーステーブルの `catalog.database.table` 名から観測された最新のデータバージョン時刻へのマッピングとして JSON オブジェクトで示します。これは `LAST_REFRESH_TIME`（それらの単一の最大値）の背後にあるテーブルごとの内訳です。外部/データレイクのベーステーブルはパーティションのソース更新時刻を、OLAP（内部）ベーステーブルは可視バージョンのコミット時刻を報告します。記録された時刻を持つベーステーブルがない場合は `{}` です。本列はリフレッシュが成功した場合にのみ進みます（失敗またはスキップされたリフレッシュでは変化しません）。精度は 1 秒であり、同一秒内に発生した書き込みとリフレッシュは区別できません。 |
| EFFECTIVE_REFRESH_MODE | このマテリアライズドビューが実際に構築されたリフレッシュモードです。有効値は `REFRESH_MODE` と同じ（`PCT`、`INCREMENTAL`、`AUTO`）。通常は `REFRESH_MODE` と一致し、例外は 1 つだけです。`REFRESH_MODE` が `AUTO` でも定義を増分で維持できない場合、`CREATE` は `PCT` のマテリアライズドビューを作成し、この列は `PCT` になります。この判定は作成時に一度だけ行われ、その後変わりません。増分を再度試すにはマテリアライズドビューを作り直すしかありません。同期マテリアライズドビューでは空。 |
| EFFECTIVE_REFRESH_MODE_REASON | `EFFECTIVE_REFRESH_MODE` が `REFRESH_MODE` と異なる理由、つまりこの定義を増分で維持できない理由の説明です。マテリアライズドビューの作成時に記録され、その後は更新されません。2 つのモード列が一致する場合は空。 |
| LAST_EXECUTED_REFRESH_MODE | 直近のリフレッシュが実際に使用したリフレッシュモードです（有効値: `PCT`、`INCREMENTAL`）。`REFRESH_MODE` と `EFFECTIVE_REFRESH_MODE` がどちらも `AUTO` のとき、この列が `PCT` であれば、そのリフレッシュだけがフォールバックしたことを意味し、以降のリフレッシュは増分に戻れます。一方 `EFFECTIVE_REFRESH_MODE` が `PCT` の場合は、そのマテリアライズドビューが増分をまったく試みないことを意味します。変更がなくスキップされたリフレッシュは、この列の値を変えません。最初のリフレッシュより前、および同期マテリアライズドビューでは空。 |
| LAST_REFRESH_MODE_REASON | 直近のリフレッシュ が増分リフレッシュではなく `LAST_EXECUTED_REFRESH_MODE` で実行された理由です。モードの決定が行われなかった場合は空です。値:`NON_APPEND_ONLY_CHANGE`(append-only ではないベーステーブルの変更。パーティションの削除、truncate、上書き、外部テーブルの削除、行レベルの削除など)、`BASELINE_UNREACHABLE`(記録されたベースラインがテーブルの head の祖先ではなくなった。スナップショットの期限切れ、またはテーブルのロールバックや置き換え)、`BASELINE_MISSING`(差分を読む基準がそもそもない。初回リフレッシュ、またはメタデータ修復の後)、`CHANGE_CAPTURE_DISABLED`(ウィンドウ内のいずれかのバージョンが、そのベーステーブルで変更キャプチャが無効な間に発行された)、`FORCE_REFRESH`(強制リフレッシュ)、`UNKNOWN`(上記のいずれにも分類されないフォールバック。原因は FE のログにあります。フォールバック自体が成功した場合 `ERROR_MESSAGE` は空のままです)。 |
| LAST_REFRESH_MODE_REASON_TABLE | モードの決定を引き起こしたベーステーブルです（`catalog.database.table` 形式）。単一のベーステーブルに起因しない場合は空です。`FORCE_REFRESH` はテーブルではなくリクエストに由来し、BE が変更の読み取り中に報告する理由はテーブルではなく tablet を指します。 |
