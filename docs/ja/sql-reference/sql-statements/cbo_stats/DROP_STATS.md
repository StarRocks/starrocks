---
displayed_sidebar: docs
---

# DROP STATS

## 説明

CBO 統計情報を削除します。これには基本統計情報とヒストグラムが含まれます。詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics) を参照してください。

不要な統計情報を削除することができます。統計情報を削除すると、統計情報のデータとメタデータ、および期限切れキャッシュ内の統計情報も削除されます。自動収集タスクが進行中の場合、以前に削除された統計情報が再収集される可能性があることに注意してください。収集タスクの履歴を表示するには、`SHOW ANALYZE STATUS` を使用できます。

このステートメントは v2.4 からサポートされています。

## 構文

### 基本統計情報の削除

```SQL
DROP STATS tbl_name
```

### ヒストグラムの削除

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name];
```

## 参考文献

CBO の統計情報収集についての詳細は、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。