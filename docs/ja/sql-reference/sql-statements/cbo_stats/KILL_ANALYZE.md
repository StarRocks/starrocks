---
displayed_sidebar: docs
---

# KILL ANALYZE

## 説明

**実行中の** コレクションタスクをキャンセルします。これには、手動およびカスタム自動タスクが含まれます。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
KILL ANALYZE <ID>
```

手動コレクションタスクのタスク ID は SHOW ANALYZE STATUS から取得できます。カスタムコレクションタスクのタスク ID は SHOW ANALYZE SHOW ANALYZE JOB から取得できます。

## 参照

[SHOW ANALYZE STATUS](SHOW_ANALYZE_STATUS.md)

[SHOW ANALYZE JOB](SHOW_ANALYZE_JOB.md)

CBO の統計収集に関する詳細は、 [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。