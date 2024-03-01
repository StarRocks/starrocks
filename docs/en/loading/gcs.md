---
displayed_sidebar: "English"
toc_max_heading_level: 4
keywords: ['Broker Load']
---

# Load data from GCS

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

import PipeAdvantages from '../assets/commonMarkdown/pipeAdvantages.md'

StarRocks provides the following options for loading data from GCS:

- Synchronous loading using [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../sql-reference/sql-functions/table-functions/files.md)
- Asynchronous loading using [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)

Each of these options has its own advantages, which are detailed in the following sections.

In most cases, we recommend that you use the INSERT+`FILES()` method, which is much easier to use.

However, the INSERT+`FILES()` method currently supports only the Parquet and ORC file formats. Therefore, if you need to load data of other file formats such as CSV, or [perform data changes such as DELETE during data loading](../loading/Load_to_Primary_Key_tables.md), you can resort to Broker Load.

## Before you begin

### Make source data ready

Make sure the source data you want to load into StarRocks is properly stored in a GCS bucket. You may also consider where the data and the database are located, because data transfer costs are much lower when your bucket and your StarRocks cluster are located in the same region.

In this topic, we provide you with a sample dataset in a GCS bucket, `gs://starrocks-samples/user-behavior-10-million-rows.parquet`. You can access that dataset with any valid credentials as the object is readable by any GCP user.

### Check privileges

<InsertPrivNote />

### Gather authentication details

The examples in this topic use service account-based authentication. To practice IAM user-based authentication, you need to gather information about the following GCS resources:

- The GCS bucket that stores your data.
- The GCS object key (object name) if accessing a specific object in the bucket. Note that the object key can include a prefix if your GCS objects are stored in sub-folders.
- The GCS region to which the GCS bucket belongs.
- The `private_ key_id`, `private_key`, and `client_email` of your Google Cloud service account

For information about all the authentication methods available, see [Authenticate to Google Cloud Storage](../integrations/authenticate_to_gcs.md).

## Use INSERT+FILES()

This method is available from v3.2 onwards and currently supports only the Parquet and ORC file formats.

### Advantages of INSERT+FILES()

`FILES()` can read the file stored in cloud storage based on the path-related properties you specify, infer the table schema of the data in the file, and then return the data from the file as data rows.

With `FILES()`, you can:

- Query the data directly from GCS using [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md).
- Create and load a table using [CREATE TABLE AS SELECT](../sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md) (CTAS).
- Load the data into an existing table using [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md).

### Typical examples

#### Querying directly from GCS using SELECT

Querying directly from GCS using SELECT+`FILES()` can give a good preview of the content of a dataset before you create a table. For example:

- Get a preview of the dataset without storing the data.
- Query for the min and max values and decide what data types to use.
- Check for `NULL` values.

The following example queries the sample dataset `gs://starrocks-samples/user-behavior-10-million-rows.parquet`:

```SQL
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user-behavior-10-million-rows.parquet",
    "format" = "parquet",
    -- highlight-start
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY----- ----END PRIVATE KEY-----"
    -- highlight-end
)
LIMIT 3;
```

:::note
Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.
:::

The system returns a query result similar to the following:

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
| 543711 |  829192 |    2355072 | pv           | 2017-11-27 08:22:37 |
| 543711 | 2056618 |    3645362 | pv           | 2017-11-27 10:16:46 |
| 543711 | 1165492 |    3645362 | pv           | 2017-11-27 10:17:00 |
+--------+---------+------------+--------------+---------------------+
```

> **NOTE**
>
> Notice that the column names as returned above are provided by the Parquet file.

#### Creating and loading a table using CTAS

This is a continuation of the previous example. The previous query is wrapped in CREATE TABLE AS SELECT (CTAS) to automate the table creation using schema inference. This means StarRocks will infer the table schema, create the table you want, and then load the data into the table. The column names and types are not required to create a table when using the `FILES()` table function with Parquet files as the Parquet format includes the column names.

:::note

The syntax of CREATE TABLE when using schema inference does not allow setting the number of replicas, so set it before creating the table. The example below is for a system with one replica:

```SQL
ADMIN SET FRONTEND CONFIG ('default_replication_num' = "1");
```

:::

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Use CTAS to create a table and load the data of the sample dataset `gs://starrocks-samples/user-behavior-10-million-rows.parquet` into the table:

```SQL
CREATE TABLE user_behavior_inferred AS
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user-behavior-10-million-rows.parquet",
    "format" = "parquet",
    -- highlight-start
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY----- ----END PRIVATE KEY-----"
    -- highlight-end
);
```

:::note
Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.
:::

After creating the table, you can view its schema by using [DESCRIBE](../sql-reference/sql-statements/Utility/DESCRIBE.md):

```SQL
DESCRIBE user_behavior_inferred;
```

The system returns a query result similar to the following:

```Plain
+--------------+------------------+------+-------+---------+-------+
| Field        | Type             | Null | Key   | Default | Extra |
+--------------+------------------+------+-------+---------+-------+
| UserID       | bigint           | YES  | true  | NULL    |       |
| ItemID       | bigint           | YES  | true  | NULL    |       |
| CategoryID   | bigint           | YES  | true  | NULL    |       |
| BehaviorType | varchar(1048576) | YES  | false | NULL    |       |
| Timestamp    | varchar(1048576) | YES  | false | NULL    |       |
+--------------+------------------+------+-------+---------+-------+
```

Query the table to verify that the data has been loaded into it. Example:

```SQL
SELECT * from user_behavior_inferred LIMIT 3;
```

The following query result is returned, indicating that the data has been successfully loaded:

```Plain
+--------+--------+------------+--------------+---------------------+
| UserID | ItemID | CategoryID | BehaviorType | Timestamp           |
+--------+--------+------------+--------------+---------------------+
|     84 | 162325 |    2939262 | pv           | 2017-12-02 05:41:41 |
|     84 | 232622 |    4148053 | pv           | 2017-11-27 04:36:10 |
|     84 | 595303 |     903809 | pv           | 2017-11-26 08:03:59 |
+--------+--------+------------+--------------+---------------------+
```

#### Loading into an existing table using INSERT

You may want to customize the table that you are inserting into, for example, the:

- column data type, nullable setting, or default values
- key types and columns
- data partitioning and bucketing

> **NOTE**
>
> Creating the most efficient table structure requires knowledge of how the data will be used and the content of the columns. This topic does not cover table design. For information about table design, see [Table types](../table_design/StarRocks_table_design.md).

In this example, we are creating a table based on knowledge of how the table will be queried and the data in the Parquet file. The knowledge of the data in the Parquet file can be gained by querying the file directly in GCS.

- Since a query of the dataset in GCS indicates that the `Timestamp` column contains data that matches a VARCHAR data type, and StarRocks can cast from VARCHAR to DATETIME, the data type is changed to DATETIME in the following DDL.
- By querying the data in GCS, you can find that there are no `NULL` values in the dataset, so the DDL could also set all columns as non-nullable.
- Based on knowledge of the expected query types, the sort key and bucketing column are set to the column `UserID`. Your use case might be different for this data, so you might decide to use `ItemID` in addition to or instead of `UserID` for the sort key.

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Create a table by hand:

```SQL
CREATE TABLE user_behavior_declared
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

Display the schema so that you can compare it with the inferred schema produced by the `FILES()` table function:

```sql
DESCRIBE user_behavior_declared;
```

```plaintext
+--------------+----------------+------+-------+---------+-------+
| Field        | Type           | Null | Key   | Default | Extra |
+--------------+----------------+------+-------+---------+-------+
| UserID       | int            | NO   | true  | NULL    |       |
| ItemID       | int            | NO   | false | NULL    |       |
| CategoryID   | int            | NO   | false | NULL    |       |
| BehaviorType | varchar(65533) | NO   | false | NULL    |       |
| Timestamp    | datetime       | NO   | false | NULL    |       |
+--------------+----------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

:::tip

Compare the schema you just created with the schema inferred earlier using the `FILES()` table function. Look at:

- data types
- nullable
- key fields

To better control the schema of the destination table and for better query performance, we recommend that you specify the table schema by hand in production environments.

:::

After creating the table, you can load it with INSERT INTO SELECT FROM FILES():

```SQL
INSERT INTO user_behavior_declared
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user-behavior-10-million-rows.parquet",
    "format" = "parquet",
    -- highlight-start
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY----- ----END PRIVATE KEY-----"
    -- highlight-end
);
```

:::note
Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.
:::

After the load is complete, you can query the table to verify that the data has been loaded into it. Example:

```SQL
SELECT * from user_behavior_declared LIMIT 3;
```

The system returns a query result similar to the following, indicating that the data has been successfully loaded:

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```

## Use Broker Load

An asynchronous Broker Load process handles making the connection to GCS, pulling the data, and storing the data in StarRocks.

This method supports the following file formats:

- Parquet
- ORC
- CSV
- JSON (supported from v3.2.3 onwards)

### Advantages of Broker Load

- Broker Load runs in the background and clients don't need to stay connected for the job to continue.
- Broker Load is preferred for long running jobs, the default timeout is 4 hours.
- In addition to Parquet and ORC file format, Broker Load supports CSV file format and JSON file format (JSON file format is supported from v3.2.3 onwards).

### Data flow

![Workflow of Broker Load](../assets/broker_load_how-to-work_en.png)

1. The user creates a load job.
2. The frontend (FE) creates a query plan and distributes the plan to the backend nodes (BE).
3. The backend (BE) nodes pull the data from the source and load the data into StarRocks.

### Typical example

Create a table, start a load process that pulls the sample dataset `gs://starrocks-samples/user-behavior-10-million-rows.parquet` from GCS, and verify the progress and success of the data loading.

#### Create a database and a table

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Create a table by hand:

```SQL
CREATE TABLE user_behavior
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

#### Start a Broker Load

Run the following command to start a Broker Load job that loads data from the sample dataset `gs://starrocks-samples/user-behavior-10-million-rows.parquet` to the `user_behavior` table:

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("gs://starrocks-samples/user-behavior-10-million-rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
    -- highlight-start
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY----- ----END PRIVATE KEY-----"
    -- highlight-end
 )
PROPERTIES
(
    "timeout" = "72000"
);
```

:::note
Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.
:::

This job has four main sections:

- `LABEL`: A string used when querying the state of the load job.
- `LOAD` declaration: The source URI, source data format, and destination table name.
- `BROKER`: The connection details for the source.
- `PROPERTIES`: The timeout value and any other properties to apply to the load job.

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md).

#### Check load progress

You can query the progress of INSERT jobs from the [`loads`](../reference/information_schema/loads.md) view in the StarRocks Information Schema. This feature is supported from v3.1 onwards.

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior' \G
```

Check the `STATE`, `PROGRESS`, and `SCAN_ROWS`:

```Plain
*************************** 1. row ***************************
              JOB_ID: 10148
               LABEL: user_behavior
       DATABASE_NAME: mydatabase
       # highlight-start
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 10000000
       # highlight-end
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 10000000
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):72000; max_filter_ratio:0.0
         CREATE_TIME: 2024-03-01 14:51:26
      ETL_START_TIME: 2024-03-01 14:51:30
     ETL_FINISH_TIME: 2024-03-01 14:51:30
     LOAD_START_TIME: 2024-03-01 14:51:30
    LOAD_FINISH_TIME: 2024-03-01 14:51:40
         JOB_DETAILS: {"All backends":{"2f99dcc2-13b8-4792-ad49-61ca67a707d6":[10004]},"FileNumber":1,"FileSize":136901706,"InternalTableLoadBytes":311746399,"InternalTableLoadRows":10000000,"ScanBytes":136901706,"ScanRows":10000000,"TaskNumber":1,"Unfinished backends":{"2f99dcc2-13b8-4792-ad49-61ca67a707d6":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
1 row in set (0.03 sec)                                   |            |            |                    |
```

After you confirm that the load job has finished, you can check a subset of the destination table to see if the data has been successfully loaded. Example:

```SQL
SELECT * from user_behavior LIMIT 3;
```

The system returns a query result similar to the following, indicating that the data has been successfully loaded:

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```
## Use Pipe

Starting from v3.2, StarRocks provides the Pipe loading method, which currently supports only the Parquet and ORC file formats.

### Advantages of Pipe

<PipeAdvantages menu=" object storage like Google GCS uses ETag "/>

### Differences between Pipe and INSERT+FILES()

A Pipe job is split into one or more transactions based on the size and number of rows in each data file. Users can query the intermediate results during the loading process. In contrast, an INSERT+`FILES()` job is processed as a single transaction, and users are unable to view the data during the loading process.

### File loading sequence

For each Pipe job, StarRocks maintains a file queue, from which it fetches and loads data files as micro-batches. Pipe does not ensure that the data files are loaded in the same order as they are uploaded. Therefore, newer data may be loaded prior to older data.

### Typical example

#### Create a database and a table

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Create a table by hand:

```SQL
CREATE TABLE user_behavior_from_pipe
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

#### Start a Pipe job

Run the following command to start a Pipe job that loads data from the sample dataset `gs://starrocks-samples/user-behavior-10-million-rows/*` to the `user_behavior_from_pipe` table. This pipe job uses both micro batches, and continuous loading (described above) pipe-specific features.

The other examples in this guide load a single Parquet file with 10 million rows. For the pipe example, the same dataset is split into 57 separate files, and these are all stored in one GCS folder. Note in the `CREATE PIPE` command below the `path` is the URI for a folder and rather than providing a filename the URI ends in `/*`. By setting `AUTO_INGEST` and specifying a folder rather than an individual file the pipe job will poll the folder for new files and ingest them as they are added to the folder.

```SQL
CREATE PIPE user_behavior_pipe
PROPERTIES
(
    "AUTO_INGEST" = "TRUE"
)
AS
INSERT INTO user_behavior_from_pipe
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user-behavior-10-million-rows/*",
    "format" = "parquet",
    -- highlight-start
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY----- ----END PRIVATE KEY-----"
    -- highlight-end
);
```

:::note
Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.
:::

This job has four main sections:

- `pipe_name`: The name of the pipe. The pipe name must be unique within the database to which the pipe belongs.
- `INSERT_SQL`: The INSERT INTO SELECT FROM FILES statement that is used to load data from the specified source data file to the destination table.
- `PROPERTIES`: A set of optional parameters that specify how to execute the pipe. These include `AUTO_INGEST`, `POLL_INTERVAL`, `BATCH_SIZE`, and `BATCH_FILES`. Specify these properties in the `"key" = "value"` format.

For detailed syntax and parameter descriptions, see [CREATE PIPE](../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md).

#### Check load progress

- Query the progress of the Pipe job by using [SHOW PIPES](../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) in the current database to which the Pipe job belongs.

  ```SQL
  SHOW PIPES WHERE NAME = 'user_behavior_pipe' \G
  ```

  The following result is returned:

  :::tip
  In the output shown below the pipe is in the `RUNNING` state. A pipe will stay in the `RUNNING` state until you manually stop it. The output also shows the number of files loaded (57) and the last time that a file was loaded.
  :::

  ```SQL
  *************************** 1. row ***************************
  DATABASE_NAME: mydatabase
        PIPE_ID: 10476
      PIPE_NAME: user_behavior_pipe
      -- highlight-start
          STATE: RUNNING
     TABLE_NAME: mydatabase.user_behavior_from_pipe
    LOAD_STATUS: {"loadedFiles":57,"loadedBytes":295345637,"loadingFiles":0,"lastLoadedTime":"2024-02-28 22:14:19"}
      -- highlight-end
     LAST_ERROR: NULL
   CREATED_TIME: 2024-02-28 22:13:41
  1 row in set (0.02 sec)
  ```

#### Check file status

You can query the load status of the files loaded from the [`pipe_files`](../reference/information_schema/pipe_files.md) view in the StarRocks Information Schema.

```SQL
SELECT * FROM information_schema.pipe_files WHERE pipe_name = 'user_behavior_pipe' \G
```

The output shows information like this for each of the files ingested:

```SQL
*************************** 56. row ***************************
   DATABASE_NAME: mydatabase
         PIPE_ID: 10160
       PIPE_NAME: user_behavior_pipe
       FILE_NAME: gs://starrocks-samples/user-behavior-10-million-rows/data_0_0_5.parquet
    FILE_VERSION: 1709302156665
       FILE_SIZE: 5205174
   LAST_MODIFIED: 2024-03-01 14:09:16
      LOAD_STATE: FINISHED
     STAGED_TIME: 2024-03-01 15:03:27
 START_LOAD_TIME: 2024-03-01 15:03:28
FINISH_LOAD_TIME: 2024-03-01 15:04:04
       ERROR_MSG:
```

#### Manage Pipe jobs

You can alter, suspend or resume, drop, or query the pipes you have created and retry to load specific data files. For more information, see [ALTER PIPE](../sql-reference/sql-statements/data-manipulation/ALTER_PIPE.md), [SUSPEND or RESUME PIPE](../sql-reference/sql-statements/data-manipulation/SUSPEND_or_RESUME_PIPE.md), [DROP PIPE](../sql-reference/sql-statements/data-manipulation/DROP_PIPE.md), [SHOW PIPES](../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md), and [RETRY FILE](../sql-reference/sql-statements/data-manipulation/RETRY_FILE.md).