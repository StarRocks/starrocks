---
displayed_sidebar: docs
---

# SHOW ANALYZE JOB

## 説明

カスタムコレクションタスクの情報とステータスを表示します。

デフォルトでは、StarRocks はテーブルの完全な統計情報を自動的に収集します。5 分ごとにデータの更新をチェックし、データの変更が検出されると、データ収集が自動的にトリガーされます。自動完全収集を使用したくない場合は、FE の設定項目 `enable_collect_full_statistic` を `false` に設定し、コレクションタスクをカスタマイズできます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
SHOW ANALYZE JOB [WHERE]
```

WHERE 句を使用して結果をフィルタリングできます。このステートメントは以下の列を返します。

| **Column**   | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | コレクションタスクの ID。                                    |
| Database     | データベース名。                                             |
| Table        | テーブル名。                                                 |
| Columns      | カラム名。                                                   |
| Type         | 統計のタイプ。`FULL` と `SAMPLE` を含みます。                |
| Schedule     | スケジューリングのタイプ。自動タスクの場合は `SCHEDULE` です。|
| Properties   | カスタムパラメータ。                                         |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED を含みます。|
| LastWorkTime | 最後の収集時間。                                             |
| Reason       | タスクが失敗した理由。タスクが成功した場合は NULL が返されます。|

## 例

```SQL
-- すべてのカスタムコレクションタスクを表示します。
SHOW ANALYZE JOB

-- データベース `test` のカスタムコレクションタスクを表示します。
SHOW ANALYZE JOB where `database` = 'test';
```

## 参考文献

[CREATE ANALYZE](CREATE_ANALYZE.md): 自動コレクションタスクをカスタマイズします。

[DROP ANALYZE](DROP_ANALYZE.md): カスタムコレクションタスクを削除します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタムコレクションタスクをキャンセルします。

CBO の統計情報収集についての詳細は、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。