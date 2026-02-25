---
displayed_sidebar: docs
---

# SHOW BACKUP

指定したデータベース内の最後の BACKUP タスクを表示します。

> **注意**
>
> StarRocks には、最後の BACKUP タスクの情報のみが保存されます。

## 構文

```SQL
SHOW BACKUP [FROM <db_name>]
```

## パラメータ

| **パラメータ** | **説明**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | BACKUP タスクが属するデータベースの名前。 |

## 戻り値

| **戻り値**           | **説明**                                              |
| -------------------- | ------------------------------------------------------------ |
| JobId                | ユニークなジョブ ID。                                               |
| SnapshotName         | データスナップショットの名前。                                   |
| DbName               | BACKUP タスクが属するデータベースの名前。        |
| State                | BACKUP タスクの現在の状態:<ul><li>PENDING: ジョブを送信した後の初期状態。</li><li>SNAPSHOTING: スナップショットを作成中。</li><li>UPLOAD_SNAPSHOT: スナップショットが完了し、アップロードの準備ができた状態。</li><li>UPLOADING: スナップショットをアップロード中。</li><li>SAVE_META: ローカルメタデータファイルを作成中。</li><li>UPLOAD_INFO: メタデータファイルと BACKUP タスクの情報をアップロード中。</li><li>FINISHED: BACKUP タスクが完了した状態。</li><li>CANCELLED: BACKUP タスクが失敗またはキャンセルされた状態。</li></ul> |
| BackupObjs           | バックアップされたオブジェクト。                                           |
| CreateTime           | タスクの送信時間。                                        |
| SnapshotFinishedTime | スナップショットの完了時間。                                    |
| UploadFinishedTime   | スナップショットのアップロード完了時間。                             |
| FinishedTime         | タスクの完了時間。                                        |
| UnfinishedTasks      | SNAPSHOTING および UPLOADING フェーズでの未完了のサブタスク ID。 |
| Progress             | スナップショットアップロードタスクの進捗状況。                             |
| TaskErrMsg           | エラーメッセージ。                                              |
| Status               | ステータス情報。                                          |
| Timeout              | タスクのタイムアウト。単位: 秒。                                  |

## 例

例 1: データベース `example_db` 内の最後の BACKUP タスクを表示します。

```SQL
SHOW BACKUP FROM example_db;
```