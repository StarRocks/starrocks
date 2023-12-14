---
displayed_sidebar: "English"
---

# STOP ROUTINE LOAD

## Description

Stop a Routine Load job.

::: warning

- A stopped Routine Load job can not be resumed.
- If you only need to pause the Routine Load job, you can execute [PAUSE ROUTINE LOAD](./PAUSE _ROUTINE_LOAD.md).

:::

## Syntax

```SQL
STOP ROUTINE LOAD FOR [db_name.]<job_name>
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       |              | The name of the database where the Routine Load job belongs. |
| job_name      | ✅            | The name of the Routine Load job.                            |

## Examples

Stop the Routine Load job `example_tbl1_ordertest1` in the database `example_db`.

```SQL
STOP ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
