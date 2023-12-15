---
displayed_sidebar: "English"
---

# RESUME ROUTINE LOAD

import RoutinelLoadPrivNote from '../assets/commonMarkdown/RoutinelLoadPrivNote.md'

## Description

Resumes a Routine load job. The job will temporarily enter **NEED_SCHEDULE** state because the job is being re-scheduled. And after some time, the job will be resumed to **RUNNING** state, continuing consuming messages from the data source and loading data. You can check the job's information with the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement.

<RoutinelLoadPrivNote />

## Syntax

```SQL
RESUME ROUTINE LOAD FOR [db_name.]<job_name>
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       |              | The name of the database to which the Routine Load job belongs. |
| job_name      | ✅            | The name of the Routine Load job.                            |

## Examples

Resume the Routine Load job `example_tbl1_ordertest1` in the database `example_db`.

```SQL
RESUME ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
