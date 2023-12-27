---
displayed_sidebar: "English"
---

# Load data from GCS

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks provides two options for loading data from GCS:

1. Asynchronous loading using Broker Load
2. Synchronous loading using the `FILES()` table function

Small datasets are often loaded synchronously using the `FILES()` table function, and large datasets are often loaded asynchronously using Broker Load. The two methods have different advantages and are described below.

<InsertPrivNote />

## Gather connection details

> **NOTE**
>
> The examples use service account key authentication. Other authentication methods are available and linked at the bottom of this page.
>
> This guide uses a dataset hosted by StarRocks. The dataset is readable by any authenticated GCP user, so
you can use your credentials to read the Parquet file used below.

Loading data from GCS requires having the:

- GCS bucket
- GCS object keys (object names) if accessing a specific object in the bucket. Note that the object key can include a prefix if your GCS objects are stored in sub-folders. The full syntax is linked in **more information**.
- GCS region
- Service account Access key and secret

## Using Broker Load

An asynchronous Broker Load process handles making the connection to GCS, pulling the data, and storing the data in StarRocks.

### Advantages of Broker Load

- Broker Load supports data transformation, UPSERT, and DELETE operations during loading.
- Broker Load runs in the background and clients don't need to stay connected for the job to continue.
- Broker Load is preferred for long running jobs, the default timeout is 4 hours.
- In addition to Parquet and ORC file format, Broker Load supports CSV files.

### Data flow

![Workflow of Broker Load](../assets/broker_load_how-to-work_en.png)

1. The user creates a load job.
2. The frontend (FE) creates a query plan and distributes the plan to the backend nodes (BE).
3. The backend (BE) nodes pull the data from the source and load the data into StarRocks.

### Typical example

Create a table, start a load process that pulls a Parquet file from GCS, and verify the progress and success of the data loading.

> **NOTE**
>
> The examples use a sample dataset in Parquet format, if you want to load a CSV or ORC file, that information is linked at the bottom of this page.

#### Create a table

Create a database for your table:

```SQL
CREATE DATABASE IF NOT EXISTS project;
USE project;
```

Create a table. This schema matches a sample dataset in a GCS bucket hosted in a StarRocks account.

```SQL
DROP TABLE IF EXISTS user_behavior;

CREATE TABLE `user_behavior` (
    `UserID` int(11),
    `ItemID` int(11),
    `CategoryID` int(11),
    `BehaviorType` varchar(65533),
    `Timestamp` datetime
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
DISTRIBUTED BY HASH(`UserID`)
PROPERTIES (
    "replication_num" = "1"
);
```

> **NOTE**
>
> The examples in this document have the property `replication_num` set to `1` so that they can be run on a simple single BE system. If you are using three or more BEs, then remove the `PROPERTIES` section of the DDL.

#### Start a Broker Load

This job has four main sections:

- `LABEL`: A string used when querying the state of a `LOAD` job.
- `LOAD` declaration: The source URI, destination table, and the source data format.
- `BROKER`: The connection details for the source.
- `PROPERTIES`: Timeout value and any other properties to apply to this job.

> **NOTE**
>
> The dataset used in these examples is hosted in a GCS bucket in a StarRocks account. Any valid service account email, key, and secret can be used, as the object is readable by any GCP authenticated user. Substitute your credentials for the placeholders in the commands below.

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

#### Check progress

Query the `information_schema.loads` table to track progress. If you have multiple `LOAD` jobs running you can filter on the `LABEL` associated with the job. In the output below there are two entries for the load job `user_behavior`. The first record shows a state of `CANCELLED`; scroll to the end of the output, and you see that `listPath failed`. The second record shows success with a valid AWS IAM access key and secret.

```SQL
SELECT * FROM information_schema.loads;
```

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

```plaintext
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |project      |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |project      |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
```

You can also check a subset of the data at this point.

```SQL
SELECT * from user_behavior LIMIT 10;
```

```plaintext
UserID|ItemID|CategoryID|BehaviorType|Timestamp          |
------+------+----------+------------+-------------------+
171146| 68873|   3002561|pv          |2017-11-30 07:11:14|
171146|146539|   4672807|pv          |2017-11-27 09:51:41|
171146|146539|   4672807|pv          |2017-11-27 14:08:33|
171146|214198|   1320293|pv          |2017-11-25 22:38:27|
171146|260659|   4756105|pv          |2017-11-30 05:11:25|
171146|267617|   4565874|pv          |2017-11-27 14:01:25|
171146|329115|   2858794|pv          |2017-12-01 02:10:51|
171146|458604|   1349561|pv          |2017-11-25 22:49:39|
171146|458604|   1349561|pv          |2017-11-27 14:03:44|
171146|478802|    541347|pv          |2017-12-02 04:52:39|
```

## Using the `FILES()` table function

### `FILES()` advantages

`FILES()` can infer the data types of the columns of the Parquet data and generate the schema for a StarRocks table. This provides the ability to query the file directly from S3 with a `SELECT` or to have StarRocks automatically create a table for you based on the Parquet file schema.

> **NOTE**
>
> Schema inference is a new feature in version 3.1 and is provided for Parquet format only and nested types are not yet supported.

### Typical examples

There are three examples using the `FILES()` table function:

- Querying the data directly from S3
- Creating and loading the table using schema inference
- Creating a table by hand and then loading the data

#### Querying directly from S3

Querying directly from S3 using `FILES()` can gives a good preview of the content of a dataset before you create a table. For example:

- Get a preview of the dataset without storing the data.
- Query for the min and max values and decide what data types to use.
- Check for nulls.

```sql
SELECT * FROM FILES(
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
) LIMIT 10;
```

> **NOTE**
>
> Notice that the column names are provided by the Parquet file.

```plaintext
UserID|ItemID |CategoryID|BehaviorType|Timestamp          |
------+-------+----------+------------+-------------------+
     1|2576651|    149192|pv          |2017-11-25 01:21:25|
     1|3830808|   4181361|pv          |2017-11-25 07:04:53|
     1|4365585|   2520377|pv          |2017-11-25 07:49:06|
     1|4606018|   2735466|pv          |2017-11-25 13:28:01|
     1| 230380|    411153|pv          |2017-11-25 21:22:22|
     1|3827899|   2920476|pv          |2017-11-26 16:24:33|
     1|3745169|   2891509|pv          |2017-11-26 19:44:31|
     1|1531036|   2920476|pv          |2017-11-26 22:02:12|
     1|2266567|   4145813|pv          |2017-11-27 00:11:11|
     1|2951368|   1080785|pv          |2017-11-27 02:47:08|
```

#### Creating a table with schema inference

This is a continuation of the previous example; the previous query is wrapped in `CREATE TABLE` to automate the table creation using schema inference. The column names and types are not required to create a table when using the `FILES()` table function with Parquet files as the Parquet format includes the column names and types and StarRocks will infer the schema.

> **NOTE**
>
> The syntax of `CREATE TABLE` when using schema inference does not allow setting the number of replicas, so set it before creating the table. The example below is for a system with a single replica:
>
> `ADMIN SET FRONTEND CONFIG ('default_replication_num' ="1");`

```sql
CREATE DATABASE IF NOT EXISTS project;
USE project;

CREATE TABLE `user_behavior_inferred` AS
SELECT * FROM FILES(
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
);
```

```SQL
DESCRIBE user_behavior_inferred;
```

```plaintext
Field       |Type            |Null|Key  |Default|Extra|
------------+----------------+----+-----+-------+-----+
UserID      |bigint          |YES |true |       |     |
ItemID      |bigint          |YES |true |       |     |
CategoryID  |bigint          |YES |true |       |     |
BehaviorType|varchar(1048576)|YES |false|       |     |
Timestamp   |varchar(1048576)|YES |false|       |     |
```

> **NOTE**
>
> Compare the inferred schema with the schema created by hand:
>
> - data types
> - nullable
> - key fields

```SQL
SELECT * from user_behavior_inferred LIMIT 10;
```

```plaintext
UserID|ItemID|CategoryID|BehaviorType|Timestamp          |
------+------+----------+------------+-------------------+
171146| 68873|   3002561|pv          |2017-11-30 07:11:14|
171146|146539|   4672807|pv          |2017-11-27 09:51:41|
171146|146539|   4672807|pv          |2017-11-27 14:08:33|
171146|214198|   1320293|pv          |2017-11-25 22:38:27|
171146|260659|   4756105|pv          |2017-11-30 05:11:25|
171146|267617|   4565874|pv          |2017-11-27 14:01:25|
171146|329115|   2858794|pv          |2017-12-01 02:10:51|
171146|458604|   1349561|pv          |2017-11-25 22:49:39|
171146|458604|   1349561|pv          |2017-11-27 14:03:44|
171146|478802|    541347|pv          |2017-12-02 04:52:39|
```

#### Loading into an existing table

You may want to customize the table that you are inserting into, for example the:

- column data type, nullable setting, or default values
- key types and columns
- distribution
- etc.

> **NOTE**
>
> Creating the most efficient table structure requires knowledge of how the data will be used and the content of the columns. This document does not cover table design, there is a link in **more information** at the end of the page.

In this example we are creating a table based on knowledge of how the table will be queried and the data in the Parquet file. The knowledge of the data in the Parquet file can be gained by querying the file directly in S3.

- Since a query of the file in S3 indicates that the `Timestamp` column contains data that matches a `datetime` data type the column type is specified in the following DDL.
- By querying the data in S3 you can find that there are no null values in the dataset, so the DDL does not set any columns as nullable.
- Based on knowledge of the expected query types, the sort key and bucketing column are set to the column `UserID` (your use case might be different for this data, you might decide to use `ItemID` in addition to or instead of `UserID` for the sort key:

```SQL
CREATE TABLE `user_behavior_declared` (
    `UserID` int(11),
    `ItemID` int(11),
    `CategoryID` int(11),
    `BehaviorType` varchar(65533),
    `Timestamp` datetime
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
DISTRIBUTED BY HASH(`UserID`)
PROPERTIES (
    "replication_num" = "1"
);
```

After creating the table, you can load it with `INSERT INTO` … `SELECT FROM FILES()`:

```SQL
INSERT INTO user_behavior_declared
  SELECT * FROM FILES(
    "path" = "gs://starrocks-samples/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
);
```

## More information

- This document only covered service account key authentication. For other options please see [authenticate to GCS resources](../integrations/authenticate_to_gcs.md).
- For more details on synchronous and asynchronous data loading please see the [overview of data loading](../loading/Loading_intro.md) documentation.
- Learn about how Broker Load supports data transformation during loading at [Transform data at loading](../loading/Etl_in_loading.md) and [Change data through loading](../loading/Load_to_Primary_Key_tables.md).
- Learn more about [table design](../table_design/StarRocks_table_design.md).
- Broker Load provides many more configuration and use options than those in the above examples, the details are in [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)
