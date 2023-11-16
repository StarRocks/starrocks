---
displayed_sidebar: "English"
---

# SHOW ANALYZE STATUS

## Description

Views the status of collection tasks.

This statement cannot be used to view the status of custom collection tasks. To view the status of custom collection tasks, use SHOW ANALYZE JOB.

This statement is supported from v2.4.

## Syntax

```SQL
SHOW ANALYZE STATUS [WHERE]
```

You can use `LILE or WHERE` to filter the information to return.

This statement returns the following columns.

| **List name** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Id            | The ID of the collection task.                               |
| Database      | The database name.                                           |
| Table         | The table name.                                              |
| Columns       | The column names.                                            |
| Type          | The type of statistics, including FULL, SAMPLE, and HISTOGRAM. |
| Schedule      | The type of scheduling. `ONCE` means manual and `SCHEDULE` means automatic. |
| Status        | The status of the task.                                      |
| StartTime     | The time when the task starts executing.                     |
| EndTime       | The time when the task execution ends.                       |
| Properties    | Custom parameters.                                           |
| Reason        | The reason why the task failed. NULL is returned if the execution was successful. |

## References

[ANALYZE TABLE](../data-definition/ANALYZE_TABLE.md): create a manual collection task.

[KILL ANALYZE](../data-definition/KILL_ANALYZE.md): cancel a custom collection task that is running.

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
