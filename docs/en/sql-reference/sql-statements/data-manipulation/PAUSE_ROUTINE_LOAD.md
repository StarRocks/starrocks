---
displayed_sidebar: "English"
---

# PAUSE ROUTINE LOAD

## Description

This statement pauses a Routine Load job but does not terminate this job. You can execute [RESUME ROUTINE LOAD](./RESUME_ROUTINE_LOAD.md) to resume it. After the load job is paused, you can execute [SHOW ROUTINE LOAD](./SHOW_ROUTINE_LOAD.md) and [ALTER ROUTINE LOAD](./ALTER_ROUTINE_LOAD.md) to view and modify information about the paused load job.

> **Note**
>
> Only users with the LOAD_PRIV privilege on the table to which data is loaded have permissions to pause Routine Load jobs on this table.

## Syntax

```SQL
PAUSE ROUTINE LOAD FOR <db_name>.<job_name>;
```

## Parameters

| Parameter | Required | Description                                                  |
| --------- | -------- | ------------------------------------------------------------ |
| db_name   |          | The name of the database for which you want to pause a Routine Load job. |
| job_name  | ✅        | The name of the Routine Load job. A table may have multiple Routine Load jobs, it is recommended to set a meaningful Routine Load job name by using identifiable information, for example, Kafka topic name or time when you create the load job, to distinguish multiple routine load jobs.  The name of the Routine Load job must be unique within the same database |

## Examples

Pause the Routine Load job `example_tbl1_ordertest1` in the database `example_db`.

```sql
PAUSE ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
