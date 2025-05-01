---
displayed_sidebar: docs
---

# SHOW ANALYZE STATUS

## 説明

収集タスクのステータスを表示します。

このステートメントは、カスタム収集タスクのステータスを表示するためには使用できません。カスタム収集タスクのステータスを表示するには、SHOW ANALYZE JOB を使用してください。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
SHOW ANALYZE STATUS [WHERE]
```

`LIKE または WHERE` を使用して、返す情報をフィルタリングできます。

このステートメントは以下の列を返します。

| **リスト名** | **説明**                                                        |
| ------------ | -------------------------------------------------------------- |
| Id           | 収集タスクのID。                                               |
| Database     | データベース名。                                               |
| Table        | テーブル名。                                                   |
| Columns      | カラム名。                                                     |
| Type         | 統計の種類。FULL、SAMPLE、HISTOGRAM を含みます。               |
| Schedule     | スケジュールの種類。`ONCE` は手動、`SCHEDULE` は自動を意味します。 |
| Status       | タスクのステータス。                                           |
| StartTime    | タスクの実行開始時間。                                         |
| EndTime      | タスクの実行終了時間。                                         |
| Properties   | カスタムパラメータ。                                           |
| Reason       | タスクが失敗した理由。実行が成功した場合は NULL が返されます。 |

## 参照

[ANALYZE TABLE](ANALYZE_TABLE.md): 手動収集タスクを作成します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタム収集タスクをキャンセルします。

CBO の統計収集についての詳細は、 [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。