---
displayed_sidebar: docs
description: "routine_load_jobs provides information about routine load jobs."
---

# routine_load_jobs

`routine_load_jobs` provides information about routine load jobs.

`routine_load_jobs` shows a routine load job to users who hold at least one privilege on its target table, or the OPERATE privilege. Unless you hold the SELECT privilege on the target table, `JOB_PROPERTIES`, `DATA_SOURCE_PROPERTIES`, `CUSTOM_PROPERTIES`, `ERROR_LOG_URLS`, `TRACKING_SQL`, and `OTHER_MSG` are NULL, and `REASONS_OF_STATE_CHANGED` keeps only the error code. The `root` user can see all routine load jobs in full. A routine load job whose target table has been dropped is shown in full only to the `root` user, and with these fields hidden to users with the OPERATE privilege.

The following fields are provided in `routine_load_jobs`:

| **Field**                | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| ID                       | ID of the routine load job.                                  |
| NAME                     | Name of the routine load job.                                |
| CREATE_TIME              | Creation time of the routine load job.                       |
| PAUSE_TIME               | Pause time of the routine load job.                          |
| END_TIME                 | End time of the routine load job.                            |
| DB_NAME                  | Name of the database to which the routine load job belongs.  |
| TABLE_NAME               | Name of the table to which the data is loaded.               |
| STATE                    | State of the routine load job.                               |
| DATA_SOURCE_TYPE         | Type of the data source.                                     |
| CURRENT_TASK_NUM         | Number of current tasks.                                     |
| JOB_PROPERTIES           | Properties of the routine load job.                          |
| DATA_SOURCE_PROPERTIES   | Properties of the data source.                               |
| CUSTOM_PROPERTIES        | Custom properties of the routine load job.                   |
| STATISTICS               | Statistics of the routine load job.                          |
| PROGRESS                 | Progress of the routine load job.                            |
| REASONS_OF_STATE_CHANGED | Reasons for state changes.                                   |
| ERROR_LOG_URLS           | URLs of error logs.                                          |
| TRACKING_SQL             | SQL statement for tracking.                                  |
| OTHER_MSG                | Other messages.                                              |
| LATEST_SOURCE_POSITION   | Latest source position in JSON format.                       |
| OFFSET_LAG               | Offset lag in JSON format.                                   |
| TIMESTAMP_PROGRESS       | Timestamp progress in JSON format.                           |
