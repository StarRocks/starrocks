---
displayed_sidebar: docs
---

# KILL ANALYZE

## 説明

**実行中の** コレクションタスクをキャンセルします。手動およびカスタム自動タスクを含みます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
KILL ANALYZE <ID>
```

手動コレクションタスクのタスク ID は SHOW ANALYZE STATUS から取得できます。カスタムコレクションタスクのタスク ID は SHOW ANALYZE JOB から取得できます。

## 参考

[SHOW ANALYZE STATUS](SHOW_ANALYZE_STATUS.md)

[SHOW ANALYZE JOB](SHOW_ANALYZE_JOB.md)

CBO の統計収集についての詳細は、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。