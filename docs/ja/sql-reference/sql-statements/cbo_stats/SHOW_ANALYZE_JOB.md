---
displayed_sidebar: docs
---

# SHOW ANALYZE JOB

## 説明

カスタム収集タスクの情報とステータスを表示します。

デフォルトでは、StarRocks はテーブルの完全な統計を自動的に収集します。5 分ごとにデータの更新をチェックし、データの変更が検出されると、データ収集が自動的にトリガーされます。自動の完全収集を使用したくない場合は、FE の設定項目 `enable_collect_full_statistic` を `false` に設定し、収集タスクをカスタマイズできます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
SHOW ANALYZE JOB [WHERE]
```

WHERE 句を使用して結果をフィルタリングできます。このステートメントは以下の列を返します。

| **Column**   | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | 収集タスクの ID。                                            |
| Database     | データベース名。                                             |
| Table        | テーブル名。                                                 |
| Columns      | カラム名。                                                   |
| Type         | 統計のタイプ。`FULL` と `SAMPLE` を含みます。                |
| Schedule     | スケジュールのタイプ。自動タスクの場合、タイプは `SCHEDULE` です。 |
| Properties   | カスタムパラメータ。                                         |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED を含みます。 |
| LastWorkTime | 最後の収集時間。                                             |
| Reason       | タスクが失敗した理由。タスクが成功した場合は NULL が返されます。 |

## 例

```SQL
-- すべてのカスタム収集タスクを表示します。
SHOW ANALYZE JOB

-- データベース `test` のカスタム収集タスクを表示します。
SHOW ANALYZE JOB where `database` = 'test';
```

## 参考

[CREATE ANALYZE](CREATE_ANALYZE.md): 自動収集タスクをカスタマイズします。

[DROP ANALYZE](DROP_ANALYZE.md): カスタム収集タスクを削除します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタム収集タスクをキャンセルします。

CBO の統計収集についての詳細は、 [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。