---
displayed_sidebar: docs
---

# SHOW RESTORE

## Description

指定したデータベースで最後の RESTORE タスクを表示します。詳細については、 [data backup and restoration](../../../administration/management/Backup_and_restore.md) を参照してください。

> **NOTE**
>
> StarRocks には、最後の RESTORE タスクの情報のみが保存されます。

## Syntax

```SQL
SHOW RESTORE [FROM <db_name>]
```

## Parameters

| **Parameter** | **Description**                                        |
| ------------- | ------------------------------------------------------ |
| db_name       | RESTORE タスクが属するデータベースの名前。             |

## Return

| **Return**           | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JobId                | ユニークなジョブ ID。                                        |
| Label                | データスナップショットの名前。                               |
| Timestamp            | バックアップのタイムスタンプ。                               |
| DbName               | RESTORE タスクが属するデータベースの名前。                   |
| State                | RESTORE タスクの現在の状態:<ul><li>PENDING: ジョブを送信した後の初期状態。</li><li>SNAPSHOTING: ローカルスナップショットの実行中。</li><li>DOWNLOAD: スナップショットダウンロードタスクの送信中。</li><li>DOWNLOADING: スナップショットをダウンロード中。</li><li>COMMIT: ダウンロードしたスナップショットをコミットする。</li><li>COMMITTING: ダウンロードしたスナップショットをコミット中。</li><li>FINISHED: RESTORE タスクが完了。</li><li>CANCELLED: RESTORE タスクが失敗またはキャンセルされた。</li></ul> |
| AllowLoad            | RESTORE タスク中にデータのロードが許可されているか。         |
| ReplicationNum       | 復元されるレプリカの数。                                     |
| RestoreObjs          | 復元されたオブジェクト（テーブルとパーティション）。        |
| CreateTime           | タスクの送信時間。                                           |
| MetaPreparedTime     | ローカルメタデータの完了時間。                               |
| SnapshotFinishedTime | スナップショットの完了時間。                                 |
| DownloadFinishedTime | スナップショットダウンロードの完了時間。                     |
| FinishedTime         | タスクの完了時間。                                           |
| UnfinishedTasks      | SNAPSHOTTING、DOWNLOADING、および COMMITTING フェーズでの未完了のサブタスク ID。 |
| Progress             | スナップショットダウンロードタスクの進捗。                   |
| TaskErrMsg           | エラーメッセージ。                                           |
| Status               | ステータス情報。                                             |
| Timeout              | タスクのタイムアウト。単位: 秒。                             |

## Examples

Example 1: データベース `example_db` で最後の RESTORE タスクを表示します。

```SQL
SHOW RESTORE FROM example_db;
```