---
displayed_sidebar: docs
---

# routine_load_jobs

`routine_load_jobs` provides information about routine load jobs.

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
