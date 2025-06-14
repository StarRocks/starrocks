---
displayed_sidebar: docs
---

# SHOW RESTORE

指定したデータベース内の最後の RESTORE タスクを表示します。

> **注意**
>
> StarRocks には、最後の RESTORE タスクの情報のみが保存されます。

## 構文

```SQL
SHOW RESTORE [FROM <db_name>]
```

## パラメータ

| **パラメータ** | **説明**                                        |
| ------------- | ------------------------------------------------ |
| db_name       | RESTORE タスクが属するデータベースの名前。       |

## 戻り値

| **戻り値**           | **説明**                                              |
| -------------------- | ------------------------------------------------------ |
| JobId                | ユニークなジョブ ID。                                  |
| Label                | データスナップショットの名前。                         |
| Timestamp            | バックアップのタイムスタンプ。                         |
| DbName               | RESTORE タスクが属するデータベースの名前。             |
| State                | RESTORE タスクの現在の状態:<ul><li>PENDING: ジョブを送信した後の初期状態。</li><li>SNAPSHOTING: ローカルスナップショットを実行中。</li><li>DOWNLOAD: スナップショットダウンロードタスクを送信中。</li><li>DOWNLOADING: スナップショットをダウンロード中。</li><li>COMMIT: ダウンロードしたスナップショットをコミットする。</li><li>COMMITTING: ダウンロードしたスナップショットをコミット中。</li><li>FINISHED: RESTORE タスクが完了。</li><li>CANCELLED: RESTORE タスクが失敗またはキャンセルされた。</li></ul> |
| AllowLoad            | RESTORE タスク中にデータのロードが許可されているか。   |
| ReplicationNum       | 復元されるレプリカの数。                               |
| RestoreObjs          | 復元されたオブジェクト（テーブルとパーティション）。  |
| CreateTime           | タスクの送信時間。                                     |
| MetaPreparedTime     | ローカルメタデータの完了時間。                         |
| SnapshotFinishedTime | スナップショットの完了時間。                           |
| DownloadFinishedTime | スナップショットダウンロードの完了時間。               |
| FinishedTime         | タスクの完了時間。                                     |
| UnfinishedTasks      | SNAPSHOTTING、DOWNLOADING、COMMITTING フェーズでの未完了のサブタスク ID。 |
| Progress             | スナップショットダウンロードタスクの進捗。             |
| TaskErrMsg           | エラーメッセージ。                                     |
| Status               | ステータス情報。                                       |
| Timeout              | タスクのタイムアウト。単位: 秒。                       |

## 例

例 1: データベース `example_db` の最後の RESTORE タスクを表示します。

```SQL
SHOW RESTORE FROM example_db;
```