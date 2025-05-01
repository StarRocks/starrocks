---
displayed_sidebar: docs
---

# SHOW ANALYZE JOB

## 説明

カスタムコレクションタスクの情報とステータスを表示します。

デフォルトでは、StarRocks はテーブルの完全な統計情報を自動的に収集します。データの更新を5分ごとにチェックし、データの変更が検出されると、データ収集が自動的にトリガーされます。自動の完全収集を使用したくない場合は、FE の設定項目 `enable_collect_full_statistic` を `false` に設定し、コレクションタスクをカスタマイズできます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
SHOW ANALYZE JOB [WHERE]
```

WHERE 句を使用して結果をフィルタリングできます。このステートメントは以下の列を返します。

| **列**       | **説明**                                                    |
| ------------ | ----------------------------------------------------------- |
| Id           | コレクションタスクのID。                                    |
| Database     | データベース名。                                            |
| Table        | テーブル名。                                                |
| Columns      | カラム名。                                                  |
| Type         | 統計のタイプ。`FULL` と `SAMPLE` を含む。                   |
| Schedule     | スケジューリングのタイプ。自動タスクの場合は `SCHEDULE`。   |
| Properties   | カスタムパラメータ。                                        |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED を含む。|
| LastWorkTime | 最後の収集時間。                                            |
| Reason       | タスクが失敗した理由。タスクが成功した場合は NULL が返されます。|

## 例

```SQL
-- すべてのカスタムコレクションタスクを表示します。
SHOW ANALYZE JOB

-- データベース `test` のカスタムコレクションタスクを表示します。
SHOW ANALYZE JOB where `database` = 'test';
```

## 参考

[CREATE ANALYZE](CREATE_ANALYZE.md): 自動コレクションタスクをカスタマイズします。

[DROP ANALYZE](DROP_ANALYZE.md): カスタムコレクションタスクを削除します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタムコレクションタスクをキャンセルします。

CBO の統計情報収集についての詳細は、 [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。