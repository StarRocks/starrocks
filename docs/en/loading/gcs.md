---
displayed_sidebar: "English"
toc_max_heading_level: 4
---

# Load data from GCS

import LoadMethodIntro from '../assets/commonMarkdown/loadMethodIntro.md'

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks provides the following options for loading data from GCS:

<LoadMethodIntro />

## Before you begin

### Make source data ready

Make sure the source data you want to load into StarRocks is properly stored in a GCS bucket. You may also consider where the data and the database are located, because data transfer costs are much lower when your bucket and your StarRocks cluster are located in the same region.

In this topic, we provide you with a sample dataset in a GCS bucket, `gs://``starrocks-samples``/user_behavior_ten_million_rows.parquet`. You can access that dataset with any valid credentials as the object is readable by any GCP user.

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

The following example queries the sample dataset `gs://starrocks-samples/user_behavior_ten_million_rows.parquet`:

```SQL
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
)
LIMIT 3;
```

> **NOTE**
>
> Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.

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

> **NOTE**
>
> The syntax of CREATE TABLE when using schema inference does not allow setting the number of replicas. If you are using a StarRocks shared-nothing cluster, set the number of replicas before creating the table. The example below is for a system with three replicas:
>
> ```SQL
> ADMIN SET FRONTEND CONFIG ('default_replication_num' = "3");
> ```

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Use CTAS to create a table and load the data of the sample dataset `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` into the table:

```SQL
CREATE TABLE user_behavior_inferred AS
SELECT * FROM FILES
(
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
);
```

> **NOTE**
>
> Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.

After creating the table, you can view its schema by using [DESCRIBE](../sql-reference/sql-statements/Utility/DESCRIBE.md):

```SQL
DESCRIBE user_behavior_inferred;
```

The system returns a query result similar to the following:

```Plain
+--------------+-----------+------+-------+---------+-------+
| Field        | Type      | Null | Key   | Default | Extra |
+--------------+-----------+------+-------+---------+-------+
| UserID       | bigint    | YES  | true  | NULL    |       |
| ItemID       | bigint    | YES  | true  | NULL    |       |
| CategoryID   | bigint    | YES  | true  | NULL    |       |
| BehaviorType | varbinary | YES  | false | NULL    |       |
| Timestamp    | varbinary | YES  | false | NULL    |       |
+--------------+-----------+------+-------+---------+-------+
```

Compare the inferred schema with the schema created by hand:

- data types
- nullable
- key fields

To better control the schema of the destination table and for better query performance, we recommend that you specify the table schema by hand in production environments.

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

- Since a query of the dataset in GCS indicates that the `Timestamp` column contains data that matches a VARBINARY data type, the column type is specified in the following DDL.
- By querying the data in GCS, you can find that there are no `NULL` values in the dataset, so the DDL does not set any columns as nullable.
- Based on knowledge of the expected query types, the sort key and bucketing column are set to the column `UserID`. Your use case might be different for this data, so you might decide to use `ItemID` in addition to or instead of `UserID` for the sort key.

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Create a table by hand (we recommend that the table have the same schema as the Parquet file you want to load from GCS):

```SQL
CREATE TABLE user_behavior_declared
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp varbinary
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

After creating the table, you can load it with INSERT INTO SELECT FROM FILES():

```SQL
INSERT INTO user_behavior_declared
  SELECT * FROM FILES
  (
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
);
```

> **NOTE**
>
> Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.

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

#### Check load progress

You can query the progress of INSERT jobs from the [`loads`](../reference/information_schema/loads.md) view in the StarRocks Information Schema. This feature is supported from v3.1 onwards. Example:

```SQL
SELECT * FROM information_schema.loads ORDER BY JOB_ID DESC;
```

If you have submitted multiple load jobs, you can filter on the `LABEL` associated with the job. Example:

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'insert_f3fc2298-a553-11ee-92f4-00163e0842bd' \G
*************************** 1. row ***************************
              JOB_ID: 10193
               LABEL: insert_f3fc2298-a553-11ee-92f4-00163e0842bd
       DATABASE_NAME: mydatabase
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: INSERT
            PRIORITY: NORMAL
           SCAN_ROWS: 10000000
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 10000000
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):300; max_filter_ratio:0.0
         CREATE_TIME: 2023-12-28 15:37:38
      ETL_START_TIME: 2023-12-28 15:37:38
     ETL_FINISH_TIME: 2023-12-28 15:37:38
     LOAD_START_TIME: 2023-12-28 15:37:38
    LOAD_FINISH_TIME: 2023-12-28 15:39:35
         JOB_DETAILS: {"All backends":{"f3fc2298-a553-11ee-92f4-00163e0842bd":[10120]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":581730322,"InternalTableLoadRows":10000000,"ScanBytes":581574034,"ScanRows":10000000,"TaskNumber":1,"Unfinished backends":{"f3fc2298-a553-11ee-92f4-00163e0842bd":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
```

For information about the fields provided in the `loads` view, see [`loads`](../reference/information_schema/loads.md).

> **NOTE**
>
> INSERT is a synchronous command. If an INSERT job is still running, you need to open another session to check its execution status.

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

Create a table, start a load process that pulls the sample dataset `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` from GCS, and verify the progress and success of the data loading.

#### Create a database and a table

Create a database and switch to it:

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

Create a table by hand (we recommend that the table has the same schema as the Parquet file that you want to load from GCS):

```SQL
CREATE TABLE user_behavior
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp varbinary
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

#### Start a Broker Load

Run the following command to start a Broker Load job that loads data from the sample dataset `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` to the `user_behavior` table:

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("gs://starrocks-samples/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
 
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
 )
PROPERTIES
(
    "timeout" = "72000"
);
```

> **NOTE**
>
> Substitute the credentials in the above command with your own credentials. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user.

This job has four main sections:

- `LABEL`: A string used when querying the state of the load job.
- `LOAD` declaration: The source URI, source data format, and destination table name.
- `BROKER`: The connection details for the source.
- `PROPERTIES`: The timeout value and any other properties to apply to the load job.

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md).

#### Check load progress

You can query the progress of INSERT jobs from the [`loads`](../reference/information_schema/loads.md) view in the StarRocks Information Schema. This feature is supported from v3.1 onwards.

```SQL
SELECT * FROM information_schema.loads;
```

For information about the fields provided in the `loads` view, see [`loads`](../reference/information_schema/loads.md).

If you have submitted multiple load jobs, you can filter on the `LABEL` associated with the job. Example:

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

In the output below there are two entries for the load job `user_behavior`:

- The first record shows a state of `CANCELLED`. Scroll to `ERROR_MSG`, and you can see that the job has failed due to `listPath failed`.
- The second record shows a state of `FINISHED`, which means that the job has succeeded.

```Plain
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |mydatabase   |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |mydatabase   |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
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
