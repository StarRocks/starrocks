---
displayed_sidebar: docs
---

# DROP TASK

DROP TASK drops an asynchronous ETL task submitted using [SUBMIT TASK](SUBMIT_TASK.md). This feature has been supported since StarRocks v2.5.7.

> **NOTE**
>
> Dropping a task with DROP TASK simultaneously cancels the corresponding TaskRun.

## Syntax

```SQL
DROP TASK [IF EXISTS] `<task_name>` [FORCE]
```

## Parameters

| **Parameter** | **Required** | **Description**                                                                                                                                                                                                                  |
| ------------- | ------------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| IF EXISTS     | no           | If this parameter is specified, StarRocks will not throw an exception when deleting a task that does not exist. If this parameter is not specified, the system will throw an exception when deleting a task that does not exist. |
| task_name     | Yes          | The name of the task to drop. Please wrap the task name with backticks (`) to prevent any parse failure.                                                                                                                         |
| FORCE         | No           | Forces to drop the task.                                                                                                                                                                                                         |

## Usage notes

You can check the information of asynchronous tasks by querying the metadata views `tasks` and `task_runs` in Information Schema.

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

You can obtain the `task_name` of materialized view refresh tasks by using the SHOW MATERIALIZED VIEWS statement.

```SQL
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS WHERE name = '<mv_name>';
```

## Examples

```Plain
MySQL > SUBMIT /*+set_var(query_timeout=100000)*/ TASK `ctas` AS
    -> CREATE TABLE insert_wiki_edit_new
    -> AS SELECT * FROM source_wiki_edit;
+----------+-----------+
| TaskName | Status    |
+----------+-----------+
| ctas     | SUBMITTED |
+----------+-----------+
1 row in set (1.19 sec)

MySQL > DROP TASK `ctas`;
Query OK, 0 rows affected (0.35 sec)
```

Drop a non-existing task

- If the `IF EXISTS` parameter is not specified, dropping a non-existing task `test_task` will result in an error.

```Plain
MySQL > DROP TASK test_task;
Query 1 ERROR: Getting analyzing error. Detail message: Task test_task is not exist.
```

- If the `IF EXISTS` parameter is specified, dropping a non-existing `test_task` will not result in an error.

```Plain
MySQL > DROP TASK IF EXISTS test_task;
Query OK, 0 rows affected (0.00 sec)
```
