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
| tables                            | Provides general metadata information of tables.             |
| tables_config                     | Provides additional table metadata information that is unique to StarRocks. |
| load_tracking_logs                | Provides error information (if any) of load jobs. |

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

## load_tracking_logs

This feature is supported since StarRocks v3.0.

The following fields are provided in `load_tracking_logs`:

| **Field**     | **Description**                                                                       |
|---------------|---------------------------------------------------------------------------------------|
| JOB_ID        | The ID of the load job.                                                               |
| LABEL         | The label of the load job.                                                            |
| DATABASE_NAME | The database that the load job belongs to.                                            |
| TRACKING_LOG  | Error logs (if any) of the load job.                                                  |
| Type          | The type of the load job. Valid values: BROKER, INSERT, ROUTINE_LOAD and STREAM_LOAD. |
