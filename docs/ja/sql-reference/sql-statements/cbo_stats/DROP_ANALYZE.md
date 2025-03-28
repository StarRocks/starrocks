---
displayed_sidebar: docs
---

# DROP ANALYZE

## 説明

カスタムコレクションタスクを削除します。

デフォルトでは、StarRocks はテーブルの完全な統計情報を自動的に収集します。データの更新を5分ごとにチェックし、データの変更が検出されると、データ収集が自動的にトリガーされます。自動の完全収集を使用したくない場合は、FE の設定項目 `enable_collect_full_statistic` を `false` に設定し、カスタムのコレクションタスクを作成できます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
DROP ANALYZE <ID>
```

タスク ID は SHOW ANALYZE JOB ステートメントを使用して取得できます。

## 例

```SQL
DROP ANALYZE 266030;
```

## 参考文献

[CREATE ANALYZE](CREATE_ANALYZE.md): 自動コレクションタスクをカスタマイズします。

[SHOW ANALYZE JOB](SHOW_ANALYZE_JOB.md): 自動コレクションタスクのステータスを表示します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタムコレクションタスクをキャンセルします。

CBO の統計情報収集の詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。