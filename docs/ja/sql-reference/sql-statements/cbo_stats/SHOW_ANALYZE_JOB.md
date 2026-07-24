---
displayed_sidebar: docs
description: "SHOW ANALYZE JOB は、カスタム収集タスクの情報とステータスを表示します。"
---

# SHOW ANALYZE JOB

SHOW ANALYZE JOB は、カスタム収集タスクの情報とステータスを表示します。

デフォルトでは、StarRocks はテーブルの完全な統計情報を自動的に収集します。5 分ごとにデータの更新を確認し、データの変更が検出された場合は、データ収集が自動的にトリガーされます。自動完全収集を使用したくない場合は、FE 設定項目 `enable_collect_full_statistic` を `false` に設定し、収集タスクをカスタマイズできます。

このステートメントは v2.4 以降でサポートされています。

## 構文

```SQL
SHOW ANALYZE JOB [WHERE]
```

WHERE 句を使用して結果をフィルタリングできます。このステートメントは以下の列を返します。

| **列**   | **説明**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | 収集タスクの ID。                               |
| Database     | データベース名。                                           |
| Table        | テーブル名。                                              |
| Columns      | 列名。                                            |
| Type         | 統計情報のタイプ（`FULL` および `SAMPLE` を含む）。       |
| Schedule     | スケジューリングのタイプ。自動タスクの場合、タイプは `SCHEDULE` です。 |
| Properties   | カスタムパラメータ。                                           |
| Status       | タスクのステータス（PENDING、RUNNING、SUCCESS、FAILED を含む）。 |
| LastWorkTime | 最後の収集の時刻。                             |
| Reason       | タスクが失敗した理由。タスクの実行が成功した場合は NULL が返されます。 |

## 例

```SQL
-- すべてのカスタム収集タスクを表示する。
SHOW ANALYZE JOB;

-- データベース `test` のカスタム収集タスクを表示する。
SHOW ANALYZE JOB where `database` = 'test';
```

## 参照

[CREATE ANALYZE](CREATE_ANALYZE.md)：自動収集タスクをカスタマイズする。

[DROP ANALYZE](DROP_ANALYZE.md)：カスタム収集タスクを削除する。

[KILL ANALYZE](KILL_ANALYZE.md)：実行中のカスタム収集タスクをキャンセルする。

CBO の統計情報収集の詳細については、[CBO のための統計情報の収集](../../../using_starrocks/Cost_based_optimizer.md)を参照してください。
