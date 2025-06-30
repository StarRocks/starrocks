---
displayed_sidebar: docs
---

# loads

`loads` provides the results of load jobs. This view is supported from StarRocks v3.1 onwards. Currently, you can only view the results of [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) and [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) jobs from this view.

The following fields are provided in `loads`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| ID                   | The unique ID assigned by StarRocks to identify the load job. |
| LABEL                | The label of the load job.                                   |
| PROFILE_ID           | Profile ID of the load job.                                  |
| DB_NAME              | The name of the database to which the destination StarRocks tables belong. |
| TABLE_NAME           | Name of the table to which the data is loaded.               |
| USER                 | User who submitted the load job.                             |
| WAREHOUSE            | Warehouse where the load job is executed.                    |
| STATE                | The state of the load job. Valid values:<ul><li>`PENDING`: The load job is created.</li><li>`QUEUEING`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`PREPARED`: The transaction has been committed.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> |
| PROGRESS             | The progress of the ETL stage and LOADING stage of the load job. |
| TYPE                 | The type of the load job. For Broker Load, the return value is `BROKER`. For INSERT, the return value is `INSERT`. |
| PRIORITY             | The priority of the load job. Valid values: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, and `LOWEST`. |
| SCAN_ROWS            | The number of data rows that are scanned.                    |
| SCAN_BYTES           | Number of data bytes scanned.                                |
| FILTERED_ROWS        | The number of data rows that are filtered out due to inadequate data quality. |
| UNSELECTED_ROWS      | The number of data rows that are filtered out due to the conditions specified in the WHERE clause. |
| SINK_ROWS            | The number of data rows that are loaded.                     |
| RUNTIME_DETAILS      | Runtime details of the load job in JSON format.              |
| CREATE_TIME          | The time at which the load job was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_START_TIME      | The start time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_COMMIT_TIME     | Time when the load job committed.                            |
| LOAD_FINISH_TIME     | The end time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| PROPERTIES           | Properties of the load job in JSON format.                   |
| ERROR_MSG            | The error message of the load job. If the load job did not encounter any error, `NULL` is returned. |
| TRACKING_SQL         | The SQL statement that can be used to query the tracking log of the load job. A SQL statement is returned only when the load job involves unqualified data rows. If the load job does not involve any unqualified data rows, `NULL` is returned. |
| REJECTED_RECORD_PATH | The path from which you can access all the unqualified data rows that are filtered out in the load job. The number of unqualified data rows logged is determined by the `log_rejected_record_num` parameter configured in the load job. You can use the `wget` command to access the path. If the load job does not involve any unqualified data rows, `NULL` is returned. |
| JOB_ID               | ID of the load job.                                          |
