---
displayed_sidebar: docs
---

# DROP ANALYZE

## Description

Deletes a custom collection task.

By default, StarRocks automatically collects full statistics of a table. It checks for any data updates every 5 minutes. If data change is detected, data collection will be automatically triggered. If you do not want to use automatic full collection, you can set the FE configuration item `enable_collect_full_statistic` to `false` and customize a collection task.

This statement is supported from v2.4.

## Syntax

```SQL
DROP ANALYZE <ID>
```

The task ID can be obtained by using the SHOW ANALYZE JOB statement.

## Examples

```SQL
DROP ANALYZE 266030;
```

## References

[CREATE ANALYZE](CREATE_ANALYZE.md): customize an automatic collection task.

[SHOW ANALYZE JOB](SHOW_ANALYZE_JOB.md): view the status of an automatic collection task.

[KILL ANALYZE](KILL_ANALYZE.md): cancel a custom collection task that is running.

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
