# DROP STATS

## Description

Deletes CBO statistics, which include basic statistics and histograms. For more information, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics).

You can delete statistical information you do not need. When you delete statistics, both the data and metadata of the statistics are deleted, as well as the statistics in expired cache. Note that if an automatic collection task is ongoing, previously deleted statistics may be collected again. You can use `SHOW ANALYZE STATUS` to view the history of collection tasks.

## Syntax

### Delete basic statistics

```SQL
DROP STATS tbl_name
```

### Delete histograms

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name];
```

## References

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
