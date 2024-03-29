---
displayed_sidebar: "English"
---

# SUBMIT TASK

## Description


SUBMIT TASK is used to create long-running background tasks, as well as tasks that are executed periodically.

Related operations:
- [SUBMIT TASK](./SUBMIT_TASK.md): Create a task
- [DROP TASK](./DROP_TASK.md): Delete a task
- INFORMATION_SCHEMA.tasks: Query the list of tasks
- INFORMATION_SCHEMA.task_runs: Query the execution history of tasks

Versions and feature support:
- This feature is supported starting from version 2.5.
- As of version 3.0, support for [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) and [INSERT](./INSERT.md) has been added
- Scheduled tasks are supported starting from version 3.3.

## Syntax

```SQL
SUBMIT TASK <task_name> 
[SCHEDULE [START(<schedule_start>)] EVERY(INTERVAL <schedule_interval>) ]
[PROPERTIES(<properties>)]
AS <etl_statement>

```

## Parameters

| **Parameter**      | **Optional** | **Description**                                                                                     |
| -------------      | ------------ | ---------------------------------------------------------------------------------------------------- |
| task_name          | Optional     | The name of the task.                                                                               |
| schedule_start     | Optional     | The start time for scheduled tasks.                                                                 |
| schedule_interval  | Optional     | The interval at which scheduled tasks are executed, with a minimum interval of 10 seconds.          |
| etl_statement      | Required     | The ETL statement for creating asynchronous tasks. StarRocks currently supports creating asynchronous tasks using [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) and [INSERT](./INSERT.md). |



## Usage notes

This statement creates a Task, which is a template for storing a task that executes the ETL statement. You can check the information of the Task by querying the metadata view [`tasks` in Information Schema](../../../reference/information_schema/tasks.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

After you run the Task, a TaskRun is generated accordingly. A TaskRun indicates a task that executes the ETL statement. A TaskRun has the following states:

- `PENDING`: The task waits to be run.
- `RUNNING`: The task is running.
- `FAILED`: The task failed.
- `SUCCESS`: The task runs successfully.

You can check the state of a TaskRun by querying the metadata view [`task_runs` in Information Schema](../../../reference/information_schema/task_runs.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Configure via FE configuration items

You can configure asynchronous ETL tasks using the following FE configuration items:

| **Parameter**                | **Default value** | **Description**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400            | The period during which a Task is valid. Unit: seconds. Tasks that exceed the validity period are deleted. |
| task_check_interval_second   | 14400             | The time interval to delete invalid Tasks. Unit: seconds.    |
| task_runs_ttl_second         | 86400            | The period during which a TaskRun is valid. Unit: seconds. TaskRuns that exceed the validity period are deleted automatically. Additionally, TaskRuns in the `FAILED` and `SUCCESS` states are also deleted automatically. |
| task_runs_concurrency        | 4                 | The maximum number of TaskRuns that can be run in parallel.  |
| task_runs_queue_length       | 500               | The maximum number of TaskRuns that are pending for running. If the number exceeds the default value, the incoming tasks will be suspended. |
| task_runs_max_history_number | 10000      | The maximum number of TaskRun records to retain. |
| task_min_schedule_interval_s | 10 |  The mininum interval of task execution，the default value is 10s. |

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

Example 5: Create a periodical task to write the sql result into a table
```SQL
SUBMIT TASK
SCHEDULE EVERY(INTERVAL 1 MINUTE)
AS
INSERT OVERWRITE insert_wiki_edit
    SELECT dt, user_id, count(*) 
    FROM source_wiki_edit 
    GROUP BY dt, user_id;
```
