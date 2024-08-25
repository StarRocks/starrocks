---
displayed_sidebar: docs
---

# loads

`loads` provides the results of load jobs. This view is supported from StarRocks v3.1 onwards. Currently, you can only view the results of [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) and [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) jobs from this view.

The following fields are provided in `loads`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | The unique ID assigned by StarRocks to identify the load job. |
| LABEL                | The label of the load job.                                   |
| DATABASE_NAME        | The name of the database to which the destination StarRocks tables belong. |
| STATE                | The state of the load job. Valid values:<ul><li>`PENDING`: The load job is created.</li><li>`QUEUEING`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`PREPARED`: The transaction has been committed.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul>For more information, see the "Asynchronous loading" section in [Loading concepts](../../loading/loading_introduction/loading_concepts.md#asynchronous-loading). |
| PROGRESS             | The progress of the ETL stage and LOADING stage of the load job. |
| TYPE                 | The type of the load job. For Broker Load, the return value is `BROKER`. For INSERT, the return value is `INSERT`. |
| PRIORITY             | The priority of the load job. Valid values: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, and `LOWEST`. |
| SCAN_ROWS            | The number of data rows that are scanned.                    |
| FILTERED_ROWS        | The number of data rows that are filtered out due to inadequate data quality. |
| UNSELECTED_ROWS      | The number of data rows that are filtered out due to the conditions specified in the WHERE clause. |
| SINK_ROWS            | The number of data rows that are loaded.                     |
| ETL_INFO             | The ETL details of the load job. A non-empty value is returned only for Spark Load. For any other types of load jobs, an empty value is returned. |
| TASK_INFO            | The task execution details of the load job, such as the `timeout` and `max_filter_ratio` settings. |
| CREATE_TIME          | The time at which the load job was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| ETL_START_TIME       | The start time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| ETL_FINISH_TIME      | The end time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_START_TIME      | The start time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_FINISH_TIME     | The end time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| JOB_DETAILS          | The details about the data loaded, such as the number of bytes and the number of files. |
| ERROR_MSG            | The error message of the load job. If the load job did not encounter any error, `NULL` is returned. |
| TRACKING_URL         | The URL from which you can access the unqualified data row samples detected in the load job. You can use the `curl` or `wget` command to access the URL and obtain the unqualified data row samples. If no unqualified data is detected, `NULL` is returned. |
| TRACKING_SQL         | The SQL statement that can be used to query the tracking log of the load job. A SQL statement is returned only when the load job involves unqualified data rows. If the load job does not involve any unqualified data rows, `NULL` is returned. |
| REJECTED_RECORD_PATH | The path from which you can access all the unqualified data rows that are filtered out in the load job. The number of unqualified data rows logged is determined by the `log_rejected_record_num` parameter configured in the load job. You can use the `wget` command to access the path. If the load job does not involve any unqualified data rows, `NULL` is returned. |
