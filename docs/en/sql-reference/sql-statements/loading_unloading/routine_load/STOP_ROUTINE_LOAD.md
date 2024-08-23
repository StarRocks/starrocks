---
displayed_sidebar: docs
---

# STOP ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## Description

Stops a Routine Load job.

<RoutineLoadPrivNote />

::: warning

- A stopped Routine Load job cannot be resumed. Therefore, please proceed with caution when executing this statement.
- If you only need to pause the Routine Load job, you can execute [PAUSE ROUTINE LOAD](PAUSE_ROUTINE_LOAD.md).

:::

## Syntax

```SQL
STOP ROUTINE LOAD FOR [db_name.]<job_name>
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       |              | The name of the database to which the Routine Load job belongs. |
| job_name      | ✅            | The name of the Routine Load job.                            |

## Examples

Stop the Routine Load job `example_tbl1_ordertest1` in the database `example_db`.

```SQL
STOP ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
