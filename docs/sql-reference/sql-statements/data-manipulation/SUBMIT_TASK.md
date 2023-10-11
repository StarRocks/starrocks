# SUBMIT TASK

## Description

Submits an ETL statement as an asynchronous task. This feature has been supported since StarRocks v2.5.

StarRocks v3.0 supports submitting asynchronous tasks for [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) and [INSERT](../data-manipulation/insert.md).

You can drop an asynchronous task using [DROP TASK](./DROP_TASK.md).

## Syntax

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## Parameters

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| task_name     | The task name.                                               |
| etl_statement | The ETL statement that you want to submit as an asynchronous task. StarRocks currently supports submitting asynchronous tasks for [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) and [INSERT](../data-manipulation/insert.md). |

## Usage notes

This statement creates a Task, which is a template for storing a task that executes the ETL statement. You can check the information of the Task by querying the metadata table `tasks` in [Information Schema](../../../administration/information_schema.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

After you run the Task, a TaskRun is generated accordingly. A TaskRun indicates a task that executes the ETL statement. A TaskRun has the following states:

- `PENDING`: The task waits to be run.
- `RUNNING`: The task is running.
- `FAILED`: The task failed.
- `SUCCESS`: The task runs successfully.

You can check the state of a TaskRun by querying the metadata table `task_runs` in [Information Schema](../../../administration/information_schema.md).

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Configure via FE configuration items

You can configure asynchronous ETL tasks using the following FE configuration items:

| **Parameter**              | **Default value** | **Description**                                              |
| -------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second            | 259200            | The period during which a Task is valid. Unit: seconds. Tasks that exceed the validity period are deleted. |
| task_check_interval_second | 14400             | The time interval to delete invalid Tasks. Unit: seconds.    |
| task_runs_ttl_second       | 259200            | The period during which a TaskRun is valid. Unit: seconds. TaskRuns that exceed the validity period are deleted automatically. Additionally, TaskRuns in the `FAILED` and `SUCCESS` states are also deleted automatically. |
| task_runs_concurrency      | 20                | The maximum number of TaskRuns that can be run in parallel.  |
| task_runs_queue_length     | 500               | The maximum number of TaskRuns that are pending for running. If the number exceeds the default value, the incoming tasks will be suspended. |

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
