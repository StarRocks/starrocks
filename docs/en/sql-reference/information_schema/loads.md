---
displayed_sidebar: docs
---

# loads

`loads` provides the results of load jobs. This view is supported from StarRocks v3.1 onwards.

The following fields are provided in `loads`:

| Field                | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| ID                   | Globally unique identifier.                                  |
| LABEL                | Label of the load job.                                       |
| PROFILE_ID           | The ID of the Profile, which can be analyzed via `ANALYZE PROFILE`. |
| DB_NAME              | The database to which the target table belongs.              |
| TABLE_NAME           | The target table.                                            |
| USER                 | The user who initiates the load job.                         |
| WAREHOUSE            | The warehouse to which the load job belongs.                 |
| STATE                | The state of the load job. Valid values:<ul><li>`PENDING`/`BEGIN`: The load job is created.</li><li>`QUEUEING`/`BEFORE_LOAD`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`PREPARING`: The transaction is being pre-committed.</li><li>`PREPARED`: The transaction has been pre-committed.</li><li>`COMMITED`: The transaction has been committed.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> |
| PROGRESS             | The progress of the ETL stage and LOADING stage of the load job. |
| TYPE                 | The type of the load job. For Broker Load, the return value is `BROKER`. For INSERT, the return value is `INSERT`. For Stream Load, the return value is `STREAM`. For Routine Load Load, the return value is `ROUTINE`. |
| PRIORITY             | The priority of the load job. Valid values: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, and `LOWEST`. |
| SCAN_ROWS            | The number of data rows that are scanned.                    |
| SCAN_BYTES           | The number of bytes that are scanned.                        |
| FILTERED_ROWS        | The number of data rows that are filtered out due to inadequate data quality. |
| UNSELECTED_ROWS      | The number of data rows that are filtered out due to the conditions specified in the WHERE clause. |
| SINK_ROWS            | The number of data rows that are loaded.                     |
| RUNTIME_DETAILS      | Load runtime metadata. For details, see [RUNTIME_DETAILS](#runtime_details). |
| CREATE_TIME          | The time at which the load job was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_START_TIME      | The start time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_COMMIT_TIME     | The time at which the loading transaction was committed. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_FINISH_TIME     | The end time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| PROPERTIES           | The static properties of the load job. For details, see [PROPERTIES](#properties). |
| ERROR_MSG            | The error message of the load job. If the load job did not encounter any error, `NULL` is returned. |
| TRACKING_SQL         | The SQL statement that can be used to query the tracking log of the load job. A SQL statement is returned only when the load job involves unqualified data rows. If the load job does not involve any unqualified data rows, `NULL` is returned. |
| REJECTED_RECORD_PATH | The path from which you can access all the unqualified data rows that are filtered out in the load job. The number of unqualified data rows logged is determined by the `log_rejected_record_num` parameter configured in the load job. You can use the `wget` command to access the path. If the load job does not involve any unqualified data rows, `NULL` is returned. |

## RUNTIME_DETAILS

- Universal metrics:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | Globally unique ID of the load execution plan.               |
| txn_id               | Load transaction ID.                                         |

- Specific metrics for Broker Load, INSERT INTO, and Spark Load:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL Details. This field is only valid for Spark Load jobs. For other types of load jobs, the value will be empty. |
| etl_start_time       | The start time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| etl_start_time       | The end time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| unfinished_backends  | List of BEs with unfinished executions.                      |
| backends             | List of BEs participating in execution.                      |
| file_num             | Number of files read.                                        |
| file_size            | Total size of files read.                                    |
| task_num             | Number of subtasks.                                          |

- Specific metrics for Routine Load:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | The interval for Routine Load to be scheduled.               |
| wait_slot_time       | Time elapsed while the Routine Load task waits for execution slots. |
| check_offset_time    | Time consumed when checking offset information during Routine Load task scheduling. |
| consume_time         | Time consumed by the Routine Load task to read upstream data. |
| plan_time            | Time for generating the execution plan.                      |
| commit_publish_time  | Time consumed to execute the COMMIT RPC.                     |

- Specific metrics for Stream Load:

| Metric                 | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | Timeout for load tasks.                                    |
| begin_txn_ms           | Time consumed to begin the transaction.                    |
| plan_time_ms           | Time for generating the execution plan.                    |
| receive_data_time_ms   | Time for receiving data.                                   |
| commit_publish_time_ms | Time consumed to execute the COMMIT RPC.                   |
| client_ip              | Client IP address.                                         |

## PROPERTIES

- Specific properties for Broker Load, INSERT INTO, and Spark Load:

| Property               | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | Timeout for load tasks.                                    |
| max_filter_ratio       | Maximum ratio of data rows that are filtered out due to inadequate data quality. |

- Specific properties for Routine Load:

| Property               | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load job name.                                     |
| task_num               | Number of subtasks actually executed in parallel.          |
| timeout                | Timeout for load tasks.                                    |
