# Load data from Amazon S3

StarRocks provides two options for loading data from S3:

1. Asynchronous loading using Broker Load (`WITH BROKER`)
2. Synchronous loading using the `FILES()` table function

Small datasets are often loaded synchronously using the `FILES()` table function, and large datasets are often loaded asynchronously using Broker Load. The two methods have different advantages and are described below.

> Note
>
> You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Using Broker Load

An asynchronous Broker Load process handles making the connection to S3, pulling the data, and storing the data in StarRocks.

### Advantages of Broker Load

- Broker Load supports data transformation, UPSERT, and DELETE operations during loading.
- Broker Load runs in the background and clients don't need to stay connected for the job to continue.
- Broker Load is preferred for long running jobs, the default timeout is 4 hours.
- In addition to Parquet and ORC file format, Broker Load supports CSV files.

### Data flow

![Workflow of Broker Load](../assets/broker_load_how-to-work_en.png)

1. The user creates a load job
2. The frontend (FE) creates a query plan and distributes the plan to the backend nodes (BE)
3. The backend (BE) nodes pull the data from the source and load the data into StarRocks

### Typical example

Create a table, start a load process that pulls a Parquet file from S3, and verify the progress and success of the data loading.

> Note
>
> The examples use a sample dataset in Parquet format, if you want to load a CSV or ORC file, that information is linked at the bottom of this page.

#### Create a table

Create a database for your table:

```SQL
CREATE DATABASE IF NOT EXISTS project;
USE project;
```

Create a table. This schema matches a sample dataset that you can load from a StarRocks S3 bucket.

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

#### Gather connection details

> Note
>
> The examples use IAM-based authentication. Other authentication methods are available and linked at the bottom of this page.

Loading data from S3 requires having the:

- S3 bucket
- S3 object keys (object names) if accessing a specific object in the bucket
- S3 region
- Access key and secret

#### Start a Broker Load

This `LOAD` job has four main sections:

- `LABEL`: A string used when querying the state of a `LOAD` job.
- `LOAD` declaration: The source URI, destination table, and the source data format.
- `BROKER`: The connection details for the source.
- `PROPERTIES`: Timeout value and any other properties to apply to this job.

> Tip
>
> The dataset used in these examples is available in a StarRocks S3 bucket any valid `aws.s3.access_key` and `aws.s3.secret_key`. Substitute your credentials for `AAA` and `BBB` in the commands below.

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("s3://starrocks-datasets/user_behavior_sample_data.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
    "aws.s3.enable_ssl" = "true",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.region" = "us-west-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
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

## Using the `FILES()` table function

### `FILES()` advantages

`FILES()` can infer the data types of the columns of the Parquet data and generate the schema for a StarRocks table. This provides the ability to query the file directly from S3 with a `SELECT` or to create the table structure in StarRocks as part of the data loading process.

### Typical examples

There are three examples using the `FILES()` table function:

- Querying the data directly from S3
- Creating and loading the table using schema inference
- Creating a table by hand and then loading the data

> Tip
>
> The dataset used in these examples is available in a StarRocks S3 bucket any valid `aws.s3.access_key` and `aws.s3.secret_key`. Substitute your credentials for `AAA` and `BBB` in the commands below.

#### Querying directly from S3

```sql
SELECT * FROM FILES(
    "path" = "s3://starrocks-datasets/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-west-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
) LIMIT 10;
```

> Tip
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

> Note
>
> The syntax of `CREATE TABLE` when using schema inference does not allow setting the number of replicas, so set it before creating the table. The example below is for a system with a single replica:
>
> `ADMIN SET FRONTEND CONFIG ('default_replication_num' ="1");`

```sql
CREATE DATABASE IF NOT EXISTS project;
USE project;

CREATE TABLE `user_behavior_inferred` ( AS
SELECT * FROM FILES(
    "path" = "s3://starrocks-datasets/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-west-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
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

> Note
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

By querying the data directly from S3 it is possible to decide the:

- column data types
- column names
- columns to use in the key

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

After creating the table, you can load it with `INSERT INTO` ... `SELECT FROM FILES()`:

```SQL
INSERT INTO user_behavior_declared
  SELECT * FROM FILES(
    "path" = "s3://starrocks-datasets/user_behavior_sample_data.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-west-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
);
```

## More information

- For more details on synchronous and asynchronous data loading please see the [overview of data loading](../loading/Loading_intro.md) documentation.
- Learn about how the broker supports data transformation during loading at [Transform data at loading](../loading/Etl_in_loading.md) and [Change data through loading](../loading/Load_to_Primary_Key_tables.md).
- This document only covered authentication with AWS IAM roles using key and secret authentication. For other options please see [authenticate to AWS resources](../integrations/authenticate_to_aws_resources.md).
