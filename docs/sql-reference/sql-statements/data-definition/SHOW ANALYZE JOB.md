# SHOW ANALYZE JOB

## Description

Views the information and status of custom collection tasks.

By default, StarRocks automatically collects full statistics of a table. It checks for any data updates every 5 minutes. If data change is detected, data collection will be automatically triggered. If you do not want to use automatic full collection, you can set the FE configuration item `enable_collect_full_statistic` to `false` and customize a collection task.

This statement is supported from v2.4.

## Syntax

```SQL
SHOW ANALYZE JOB [WHERE]
```

You can filter results by using the WHERE clause. The statement returns the following columns.

| **Column**   | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | The ID of the collection task.                               |
| Database     | The database name.                                           |
| Table        | The table name.                                              |
| Columns      | The column names.                                            |
| Type         | The type of statistics, including `FULL` and `SAMPLE`.       |
| Schedule     | The type of scheduling. The type is `SCHEDULE` for an automatic task. |
| Properties   | Custom parameters.                                           |
| Status       | The task status, including PENDING, RUNNING, SUCCESS, and FAILED. |
| LastWorkTime | The time of the last collection.                             |
| Reason       | The reason why the task failed. NULL is returned if task execution was successful. |

## Examples

```SQL
-- View all the custom collection tasks.
SHOW ANALYZE JOB

-- View custom collection tasks of database `test`.
SHOW ANALYZE JOB where `database` = 'test';
```

## References

[CREATE ANALYZE](../data-definition/CREATE%20ANALYZE.md): customize an automatic collection task.

[DROP ANALYZE](../data-definition/DROP%20ANALYZE.md): delete a custom collection task.

[KILL ANALYZE](../data-definition/KILL%20ANALYZE.md): cancel a custom collection task that is running.

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
