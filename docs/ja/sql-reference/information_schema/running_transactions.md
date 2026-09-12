---
displayed_sidebar: docs
description: "running_transactions は、すべてのデータベースで現在実行中のトランザクションを一覧表示します。"
---

# running_transactions

`running_transactions` は、すべてのデータベースにわたって現在実行中（つまり非終了状態）のトランザクションごとに 1 行を返します。トランザクションは終了状態（`VISIBLE` または `ABORTED`）に到達すると、このビューから消えます。

この行には、通常のロードトランザクション、Routine Load タスク、レイク（lake）コンパクショントランザクションなど、あらゆる種類の実行中トランザクションが含まれます。

`running_transactions` は次のフィールドを提供します。

| フィールド            | 説明                                                         |
| --------------------- | ------------------------------------------------------------ |
| TXN_ID                | トランザクション ID。                                        |
| GLOBAL_TXN_ID         | グローバルトランザクション ID（GTID）。GTID がない場合は `0`。 |
| LABEL                 | トランザクションのラベル。                                   |
| DATABASE_ID           | トランザクションが属するデータベースの ID。                  |
| DATABASE_NAME         | データベース名。 |
| TABLE_IDS             | トランザクションが対象とするテーブルの ID。カンマ区切り。     |
| TABLE_NAMES           | トランザクションが対象とするテーブル名。カンマ区切り。ベストエフォート: 名前に解決できない ID は生の ID として表示されます。 |
| STATE                 | トランザクションの状態。有効な値:<ul><li>`PREPARE`: トランザクションが開始されました。</li><li>`PREPARED`: トランザクションがプリコミットされました。</li><li>`COMMITTED`: トランザクションがコミットされ、`VISIBLE` への発行を待っています。</li></ul> |
| COORDINATOR           | トランザクションのコーディネーターノード。例: `FE: 127.0.0.1`。 |
| SOURCE_TYPE           | トランザクションのロードソースタイプ。例: `BACKEND_STREAMING`、`INSERT_STREAMING`、`LAKE_COMPACTION`、`ROUTINE_LOAD_TASK`、`FRONTEND`。 |
| WAREHOUSE_ID          | トランザクションが属するウェアハウスの ID。                  |
| PREPARE_TIME          | トランザクションが開始された（`PREPARE` に入った）時刻。未設定の場合は `NULL`。 |
| PREPARED_TIME         | トランザクションが `PREPARED` に到達した時刻。未設定の場合は `NULL`。 |
| COMMIT_TIME           | トランザクションがコミットされた時刻。まだコミットされていない場合は `NULL`。 |
| PUBLISH_TIME          | 発行が開始された時刻。まだ発行が開始されていない場合は `NULL`。 |
| FINISH_TIME           | トランザクションが完了した時刻。完了したトランザクションはこのビューから消えるため、実行中のトランザクションでは常に `NULL`。 |
| PENDING_PUBLISH_MS    | `COMMITTED` 状態のトランザクションについて、`VISIBLE` への発行を待った時間（ミリ秒。現在時刻からコミット時刻を引いた値）。それ以外の状態では `0`。発行の停止を診断するための中心的なフィールドです。 |
| TIMEOUT_MS            | トランザクションのタイムアウト（ミリ秒）。                   |
| PREPARED_TIMEOUT_MS   | `PREPARED` 状態のタイムアウト（ミリ秒）。                    |
| ERROR_REPLICA_NUM     | エラーレプリカの数。                                         |
| REASON                | 中止または失敗の理由テキスト。空の場合があります。           |
| ERROR_MSG             | エラーメッセージテキスト。空の場合があります。               |
| IS_NO_OP_PUBLISH      | 発行がノーオペレーション（no-op）かどうか。                  |
| NO_OP_PUBLISH_REASON  | 発行がノーオペレーションである理由。                         |

## 使用上の注意

`running_transactions` は、発行の停止（publish stall）を診断するための観測ビューであり、この問題は共有データ（shared-data、レイク）モードで最も顕著に現れます。バージョンの発行が停止すると、トランザクションが `COMMITTED` 状態に滞留し、`PENDING_PUBLISH_MS` は各トランザクションが `VISIBLE` への発行を待っている時間を示します。`COMMITTED` トランザクションを `PENDING_PUBLISH_MS` で並べ替えると、最も長く停止しているトランザクションが先頭に表示されます。

実行中のトランザクションの集合は FE Leader 上でのみ正確であるため、このビューのスキャンは常に Leader FE によって処理されます。

`running_transactions` は、クエリを実行するユーザーの権限に基づいて行をフィルタリングします。あるデータベース、またはその中のいずれかのオブジェクトに対して何らかの権限（例: そのデータベース自体に対する権限や、その中のいずれかのテーブルに対する `SELECT` 権限）を持つユーザーにのみ、そのデータベースのトランザクションが表示されます。クラスター全体へのアクセス権を持つユーザー（例: `root` ユーザーや広範な権限を付与されたユーザー）は、実行中のすべてのトランザクションを参照できます。トランザクションの実行中にそのデータベースが削除された場合、そのトランザクションは認可できないため、すべてのユーザー（管理者を含む）から非表示になります。

:::note

このビューの `COUNT(*)` が `txn_running` メトリックと等しいと仮定しないでください。両者は計算方法が異なり、一致しない場合があります。

:::

## 例

発行を最も長く待っているコミット済みトランザクションを見つけます。

```sql
SELECT TXN_ID, DATABASE_NAME, TABLE_NAMES, STATE, PENDING_PUBLISH_MS
FROM information_schema.running_transactions
WHERE STATE = 'COMMITTED'
ORDER BY PENDING_PUBLISH_MS DESC;
```
