---
displayed_sidebar: docs
---

# SHOW ANALYZE STATUS

## 説明

収集タスクのステータスを表示します。

このステートメントはカスタム収集タスクのステータスを表示するためには使用できません。カスタム収集タスクのステータスを表示するには、SHOW ANALYZE JOB を使用してください。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
SHOW ANALYZE STATUS [WHERE]
```

`LIKE` または `WHERE` を使用して、返される情報をフィルタリングできます。

このステートメントは以下の列を返します。

| **リスト名** | **説明**                                                      |
| ------------- | ------------------------------------------------------------ |
| Id            | 収集タスクの ID。                                            |
| Database      | データベース名。                                             |
| Table         | テーブル名。                                                 |
| Columns       | カラム名。                                                   |
| Type          | 統計のタイプ。FULL、SAMPLE、HISTOGRAM を含みます。           |
| Schedule      | スケジュールのタイプ。`ONCE` は手動、`SCHEDULE` は自動を意味します。 |
| Status        | タスクのステータス。                                         |
| StartTime     | タスクが実行を開始した時間。                                 |
| EndTime       | タスクの実行が終了した時間。                                 |
| Properties    | カスタムパラメータ。                                         |
| Reason        | タスクが失敗した理由。実行が成功した場合は NULL が返されます。 |

## 参考文献

[ANALYZE TABLE](ANALYZE_TABLE.md): 手動収集タスクを作成します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタム収集タスクをキャンセルします。

CBO の統計収集に関する詳細は、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。