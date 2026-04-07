---
displayed_sidebar: docs
---

# ALTER TASK

Modifies an asynchronous ETL task submitted using [SUBMIT TASK](SUBMIT_TASK.md). This feature has been supported from v4.1 onwards.

You can use this statement to:

- Suspend a running task
- Resume a suspended task
- Update task properties

## Syntax

```SQL
ALTER TASK [IF EXISTS] <task_name> { RESUME | SUSPEND | SET ('key' = 'value'[, ...]) }
```

## Parameters

| **Parameter** | **Required** | **Description**                                                                                                                                                                                                         |
| ------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| IF EXISTS     | No           | If this parameter is specified, StarRocks will not throw an exception when modifying a task that does not exist. If this parameter is not specified, the system will throw an exception when modifying a task that does not exist. |
| task_name     | Yes          | The name of the task to modify.                                                                                                                                                                                         |
| RESUME        | No           | Resumes a suspended task. The task will be scheduled according to its original schedule (for periodic tasks) or be available for manual execution (for manual tasks).                                                      |
| SUSPEND       | No           | Suspends a running task. For periodic tasks, this stops the task scheduler and kills any running task runs.                                                                                                              |
| SET           | No           | Updates the properties of the task. The properties will be merged with existing properties and applied to subsequent task executions.                                                                                      |

## Usage notes

- You can check the information of asynchronous tasks by querying the metadata views `tasks` and `task_runs` in Information Schema.

  ```SQL
  SELECT * FROM INFORMATION_SCHEMA.tasks;
  SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
  SELECT * FROM information_schema.task_runs;
  SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
  ```

- The `SUSPEND` action stops the task scheduler for periodic tasks and kills any currently running task runs. The task state will be changed to `PAUSE`.

- The `RESUME` action restarts the task scheduler for periodic tasks. The task state will be changed to `ACTIVE`.

- The `SET` action updates task properties that will be applied to subsequent task executions. You can use `session.` prefix with session variables to change the task running connect context configurations.

## Examples

Example 1: Suspend a task named `etl_task`:

```SQL
ALTER TASK etl_task SUSPEND;
```

Example 2: Resume a suspended task named `etl_task`:

```SQL
ALTER TASK etl_task RESUME;
```

Example 3: Update the query timeout for a task named `etl_task`:

```SQL
ALTER TASK etl_task SET ('session.query_timeout' = '5000');
```

Example 4: Update multiple properties for a task:

```SQL
ALTER TASK etl_task SET (
    'session.query_timeout' = '5000',
    'session.enable_profile' = 'true'
);
```

Example 5: Suspend a task only if it exists (avoid error if task does not exist):

```SQL
ALTER TASK IF EXISTS etl_task SUSPEND;
```
