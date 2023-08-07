# Information Schema

The StarRocks `information_schema` is a database within each StarRocks instance. `information_schema` contains several read-only, system-defined tables which store extensive metadata information of all objects that the StarRocks instance maintains.

## View metadata via Information Schema

You can view the metadata information within a StarRocks instance by querying the content of tables in `information_schema`.

The following example views metadata information about a table named `sr_member` in StarRocks by querying the table `tables`.

```Plain
mysql> SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'sr_member'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: sr_hub
     TABLE_NAME: sr_member
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: NULL
     TABLE_ROWS: 6
 AVG_ROW_LENGTH: 542
    DATA_LENGTH: 3255
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2022-11-17 14:32:30
    UPDATE_TIME: 2022-11-17 14:32:55
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: NULL
  TABLE_COMMENT: OLAP
1 row in set (1.04 sec)
```

## Information Schema tables

StarRocks have optimized the metadata information provided by the following tables in `information_schema`:

| **Information Schema table name** | **Description**                                              |
| --------------------------------- | ------------------------------------------------------------ |
<<<<<<< HEAD
| tables                            | Provides general metadata information of tables.             |
| tables_config                     | Provides additional table metadata information that is unique to StarRocks. |
=======
| [tables](#tables)                            | Provides general metadata information of tables.             |
| [tables_config](#tables_config)                     | Provides additional table metadata information that is unique to StarRocks. |
| [load_tracking_logs](#load_tracking_logs)                | Provides error information (if any) of load jobs. |
| [loads](#loads)                             | Provides the results of load jobs. This table is supported from v3.1 onwards. Currently, you can only view the results of [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) and [Insert](../sql-reference/sql-statements/data-manipulation/insert.md) jobs from this table.                 |

### loads

The following fields are provided in `loads`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JOB_ID               | The unique ID assigned by StarRocks to identify the load job. |
| LABEL                | The label of the load job.                                   |
| DATABASE_NAME        | The name of the database to which the destination StarRocks tables belong. |
| STATE                | The state of the load job. Valid values:<ul><li>`PENDING`: The load job is created.</li><li>`QUEUEING`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`PREPARED`: The transaction has been committed.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul>For more information, see [Asynchronous loading](https://docs.starrocks.io/en-us/main/loading/Loading_intro#asynchronous-loading). |
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
>>>>>>> 769b8fee9a ([Doc] add table links to Information Schema (#28752))

### tables

The following fields are provided in `tables`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | Name of the catalog that stores the table.                   |
| TABLE_SCHEMA    | Name of the database that stores the table.                  |
| TABLE_NAME      | Name of the table.                                           |
| TABLE_TYPE      | Type of the table. Valid values: "BASE TABLE" or "VIEW".     |
| ENGINE          | Engine type of the table. Valid values: "StarRocks", "MySQL", "MEMORY" or an empty string. |
| VERSION         | Applies to a feature not available in StarRocks.             |
| ROW_FORMAT      | Applies to a feature not available in StarRocks.             |
| TABLE_ROWS      | Row count of the table.                                      |
| AVG_ROW_LENGTH  | Average row length (size) of the table. It is equivalent to `DATA_LENGTH` / `TABLE_ROWS`. Unit: Byte. |
| DATA_LENGTH     | Data length (size) of the table. Unit: Byte.                 |
| MAX_DATA_LENGTH | Applies to a feature not available in StarRocks.             |
| INDEX_LENGTH    | Applies to a feature not available in StarRocks.             |
| DATA_FREE       | Applies to a feature not available in StarRocks.             |
| AUTO_INCREMENT  | Applies to a feature not available in StarRocks.             |
| CREATE_TIME     | The time when the table is created.                          |
| UPDATE_TIME     | The last time when the table is updated.                     |
| CHECK_TIME      | The last time when a consistency check is performed on the table. |
| TABLE_COLLATION | The default collation of the table.                          |
| CHECKSUM        | Applies to a feature not available in StarRocks.             |
| CREATE_OPTIONS  | Applies to a feature not available in StarRocks.             |
| TABLE_COMMENT   | Comment on the table.                                        |

### tables_config

The following fields are provided in `tables_config`:

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | Name of the database that stores the table.                  |
| TABLE_NAME       | Name of the table.                                           |
| TABLE_ENGINE     | Engine type of the table.                                    |
| TABLE_MODEL      | Table type. Valid values: "DUP_KEYS", "AGG_KEYS", "UNQ_KEYS" or "PRI_KEYS". |
| PRIMARY_KEY      | The primary key of a Primary Key table or a Unique Key table. An empty string is returned if the table is not a Primary Key table or a Unique Key table. |
| PARTITION_KEY    | The partitioning columns of the table.                       |
| DISTRIBUTE_KEY   | The bucketing columns of the table.                          |
| DISTRIBUTE_TYPE  | The data distribution method of the table.                   |
| DISTRIBUTE_BUCKET | Number of buckets in the table.                              |
| SORT_KEY         | Sort keys of the table.                                      |
| PROPERTIES       | Properties of the table.                                     |
| TABLE_ID         | ID of the table.                                             |
