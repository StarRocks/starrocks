# DROP TASK

## Description

Drops an asynchronous ETL task submitted using [SUBMIT TASK](./SUBMIT_TASK.md). This feature has been supported since StarRocks v2.5.7.

> **NOTE**
>
> Dropping a task with DROP TASK simultaneously cancels the corresponding TaskRun.

## Syntax

```SQL
DROP TASK '<task_name>'
```

## Parameters

| **Parameter** | **Description**               |
| ------------- | ----------------------------- |
| task_name     | The name of the task to drop. |

## Usage notes

You can check the information of asynchronous tasks by querying the metadata tables `tasks` and `task_runs` in Information Schema.

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Examples

```Plain
MySQL > SUBMIT /*+set_var(query_timeout=100000)*/ TASK ctas AS
    -> CREATE TABLE insert_wiki_edit_new
    -> AS SELECT * FROM source_wiki_edit;
+----------+-----------+
| TaskName | Status    |
+----------+-----------+
| ctas     | SUBMITTED |
+----------+-----------+
1 row in set (1.19 sec)

MySQL > DROP TASK 'ctas';
Query OK, 0 rows affected (0.35 sec)
```
