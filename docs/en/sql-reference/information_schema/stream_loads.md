---
displayed_sidebar: docs
---

# stream_loads

`stream_loads` provides information about stream load jobs.

The following fields are provided in `stream_loads`:

| **Field**             | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| LABEL                 | Label of the stream load job.                                |
| ID                    | ID of the stream load job.                                   |
| LOAD_ID               | Load ID of the stream load job.                              |
| TXN_ID                | Transaction ID of the stream load job.                       |
| DB_NAME               | Name of the database to which the stream load job belongs.   |
| TABLE_NAME            | Name of the table to which the data is loaded.               |
| STATE                 | State of the stream load job.                                |
| ERROR_MSG             | Error message if the stream load job failed.                 |
| TRACKING_URL          | URL for tracking the stream load job.                        |
| CHANNEL_NUM           | Number of channels used in the stream load job.              |
| PREPARED_CHANNEL_NUM  | Number of prepared channels in the stream load job.          |
| NUM_ROWS_NORMAL       | Number of normal rows loaded.                                |
| NUM_ROWS_AB_NORMAL    | Number of abnormal rows loaded.                              |
| NUM_ROWS_UNSELECTED   | Number of unselected rows.                                   |
| NUM_LOAD_BYTES        | Number of bytes loaded.                                      |
| TIMEOUT_SECOND        | Timeout in seconds for the stream load job.                  |
| CREATE_TIME_MS        | Creation time of the stream load job (in milliseconds).      |
| BEFORE_LOAD_TIME_MS   | Time before loading started (in milliseconds).               |
| START_LOADING_TIME_MS | Time when loading started (in milliseconds).                 |
| START_PREPARING_TIME_MS | Time when preparing started (in milliseconds).               |
| FINISH_PREPARING_TIME_MS | Time when preparing finished (in milliseconds).              |
| END_TIME_MS           | End time of the stream load job (in milliseconds).           |
| CHANNEL_STATE         | State of the channels.                                       |
| TYPE                  | Type of the stream load job.                                 |
| TRACKING_SQL          | SQL statement for tracking the stream load job.              |
