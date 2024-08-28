---
displayed_sidebar: docs
---

# SUBMIT TASK

## Description

Submits an ETL statement as an asynchronous task.

You can use this statement to:

- execute long-running tasks in the background (supported from v2.5 onwards)
- schedule a task at a regular interval (supported from v3.3 onwards)

Supported statements include:

- [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) (from v3.0 onwards)
- [INSERT](../INSERT.md) (from v3.0 onwards)
- [CACHE SELECT](../../../../data_source/data_cache_warmup.md) (from v3.3 onwards)

You can view the list of tasks by querying `INFORMATION_SCHEMA.tasks`, or view the execution history of tasks by querying `INFORMATION_SCHEMA.task_runs`. For more information, see [Usage Notes](#usage-notes).

You can drop an asynchronous task using [DROP TASK](DROP_TASK.md).

## Syntax

```SQL
SUBMIT TASK <task_name> 
[SCHEDULE [START(<schedule_start>)] EVERY(INTERVAL <schedule_interval>) ]
[PROPERTIES(<"key" = "value"[, ...]>)]
AS <etl_statement>
```

## Parameters

| **Parameter**      | **Required** | **Description**                                                                                     |
| -------------      | ------------ | ---------------------------------------------------------------------------------------------------- |
| task_name          | Yes     | The name of the task.                                                                               |
| schedule_start     | No      | The start time for the scheduled task.                                                                 |
| schedule_interval  | No      | The interval at which the scheduled task is executed, with a minimum interval of 10 seconds.          |
| etl_statement      | Yes     | The ETL statement that you want to submit as an asynchronous task. StarRocks currently supports submitting asynchronous tasks for [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) and [INSERT](../../loading_unloading/INSERT.md). |

## Usage notes

This statement creates a Task, which is a template for storing a task that executes the ETL statement. You can check the information of the Task by querying the metadata view [`tasks` in Information Schema](../../../information_schema/tasks.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

After you run the Task, a TaskRun is generated accordingly. A TaskRun indicates a task that executes the ETL statement. A TaskRun has the following states:

- `PENDING`: The task waits to be run.
- `RUNNING`: The task is running.
- `FAILED`: The task failed.
- `SUCCESS`: The task runs successfully.

You can check the state of a TaskRun by querying the metadata view [`task_runs` in Information Schema](../../../information_schema/task_runs.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Configure via FE configuration items

You can configure asynchronous ETL tasks using the following FE configuration items:

| **Parameter**                | **Default value** | **Description**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400             | The period during which a Task is valid. Unit: seconds. Tasks that exceed the validity period are deleted. |
| task_check_interval_second   | 3600              | The time interval to delete invalid Tasks. Unit: seconds.    |
| task_runs_ttl_second         | 86400             | The period during which a TaskRun is valid. Unit: seconds. TaskRuns that exceed the validity period are deleted automatically. Additionally, TaskRuns in the `FAILED` and `SUCCESS` states are also deleted automatically. |
| task_runs_concurrency        | 4                 | The maximum number of TaskRuns that can be run in parallel.  |
| task_runs_queue_length       | 500               | The maximum number of TaskRuns that are pending for running. If the number exceeds the default value, the incoming tasks will be suspended. |
| task_runs_max_history_number | 10000             | The maximum number of TaskRun records to retain. |
| task_min_schedule_interval_s | 10                | The minimum interval for Task execution. Unit: seconds. |

## Examples

Example 1: Submit an asynchronous task for `CREATE TABLE tbl1 AS SELECT * FROM src_tbl`, and specify the task name as `etl0`:

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```

Example 2: Submit an asynchronous task for `INSERT INTO tbl2 SELECT * FROM src_tbl`, and specify the task name as `etl1`:

```SQL
SUBMIT TASK etl1 AS INSERT INTO tbl2 SELECT * FROM src_tbl;
```

Example 3: Submit an asynchronous task for `INSERT OVERWRITE tbl3 SELECT * FROM src_tbl`:

```SQL
SUBMIT TASK AS INSERT OVERWRITE tbl3 SELECT * FROM src_tbl;
```

Example 4: Submit an asynchronous task for `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit` without specifying the task name, and extend the query timeout to `100000` seconds using the hint:

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

Example 5: Create an asynchronous task for an INSERT OVERWRITE statement. The task will be regularly executed at an interval of 1 minute.

```SQL
SUBMIT TASK
SCHEDULE EVERY(INTERVAL 1 MINUTE)
AS
INSERT OVERWRITE insert_wiki_edit
    SELECT dt, user_id, count(*) 
    FROM source_wiki_edit 
    GROUP BY dt, user_id;
```
