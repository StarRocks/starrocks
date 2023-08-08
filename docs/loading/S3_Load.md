# Load data from Amazon S3

## Overview

StarRocks provides two options for loading data from S3:

1. Asynchronous loading using a *broker*
2. Synchronous loading using the `FILES()` table function

Small datasets are often loaded synchronously using the `FILES()` table function, and large datasets are often loaded asynchronously using a broker. The two methods have different advantages and are described below.

## Asynchronous loading using a broker

An asynchronous *broker* process handles making the connection to S3, pulling the data, and storing the data in the StarRocks datastore.

### Advantages of asynchronous loading

- Broker Load supports data transformation, UPSERT, and DELETE operations during loading.
- Because asynchronous jobs run in the background a client does not need to stay connected for the job to continue. Check the progress of a load job by connecting a client and querying the load job state.
- Asynchronous loading is preferred for long running jobs. The default timeout for a broker load job is 4 hours.
- In addition to Parquet and ORC file format, the asynchronous broker supports CSV files.

### Data flow

1. The user creates a load job
1. The frontend (FE) creates a query plan and distributes the plan to the backend nodes (BE) 
1. The backend (BE) nodes pull the data from the source and load the data into StarRocks

![Workflow of Broker Load](../assets/broker_load_how-to-work_en.png)

### Typical example

Create a table, start a load process that pulls a Parquet file from S3, and verify the progress and success of the data loading.

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
    `UserID` int(11) NULL COMMENT "",
    `ItemID` int(11) NULL COMMENT "",
    `CategoryID` int(11) NULL COMMENT "",
    `BehaviorType` varchar(65533) NULL COMMENT "",
    `Timestamp` datetime NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`UserID`) BUCKETS 1 
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "false",
    "replicated_storage" = "true",
    "compression" = "LZ4"
);
```

#### Gather connection details

Loading data from S3 requires having the:

- S3 endpoint
- S3 region
- Access key and secret

#### Start an asynchronous load

This `LOAD` job has four main sections:

- `LABEL`: A string used to query the state of a `LOAD` job.
- `LOAD` declaration: The source URI, destination table, and the source data format.
- `BROKER`: The connection details for the source.
- `PROPERTIES`: Timeout value and any other properties to apply to this job.

> Tip
>
> The dataset used in these examples is available in a StarRocks S3 bucket any valid `aws.s3.access_key` and `aws.s3.secret_key`. Substitute your credentials for `AAA` and `BBB` in the commands below.

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("s3a://starrocks-datasets/user_behavior_sample_data.parquet")
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

Use the `SHOW LOAD` query to track progress. If you have multiple `LOAD` jobs running you can specify the `LABEL` associated with the job. In the output below there are two entries for the load job `user_behavior`. The first record shows a failure; scroll to the end of the table, and you see that access was forbidden. The second record succeeded with a valid AWS IAM access key and secret.

```SQL
SHOW LOAD;
```

```SQL
SHOW LOAD WHERE label = 'user_behavior';
```

|JobId|Label|State|Progress|Type|Priority|ScanRows|FilteredRows|UnselectedRows|SinkRows|EtlInfo|TaskInfo|ErrorMsg|CreateTime|EtlStartTime|EtlFinishTime|LoadStartTime|LoadFinishTime|TrackingSQL|JobDetails|
|----|------|-----|--------|----|--------|--------|------------|--------------|--------|-------|--------|--------|----------|------------|-------------|-------------|--------------|-----------|----------|
|10104|user_behavior|CANCELLED|ETL:N/A; LOAD:N/A|BROKER|NORMAL  |0       |0           |0             |0     |      |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|type:ETL_RUN_FAIL; msg:unknown error when get file status: s3a://starrocks-datasets/user_behavior_sample_data.parquet: getFileStatus on s3a://starrocks-datasets/user_behavior_sample_data.parquet: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidde|2023-08-04 15:58:51|                   |                   |                   |2023-08-04 15:58:57|           |{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}} |
|10110|user_behavior|LOADING  |ETL:100%; LOAD:0%|BROKER|NORMAL  |0|0|0|0||resource:N/A; timeout(s):72000; max_filter_ratio:0.0||2023-08-04 16:22:27|2023-08-04 16:22:30|2023-08-04 16:22:30|2023-08-04 16:22:30|||{"All backends":{"563ec0b9-1bd6-4a39-aa30-1332fc0fee04":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"563ec0b9-1bd6-4a39-aa30-1332fc0fee|

## Synchronous loading using the `FILES()` table function

### Advantages of the `FILES()` table function

- `FILES()` can infer the data types of the columns of the Parquet data and generate the schema for a StarRocks table. This provides the ability to query the file directly from S3 with a `SELECT` or to create the table structure in StarRocks as part of the data loading process.

### Typical examples

There are three examples using the `FILES()` table function:
- Querying the data directly from S3
- Creating and loading the table using schema inference
- Creating a table by hand and then loading the data

> Tip
>
> The dataset used in these examples is available in a StarRocks S3 bucket any valid `aws.s3.access_key` and `aws.s3.secret_key`. Substitute your credentials for `AAA` and `BBB` in the commands below.

#### Querying the data directly from S3

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

#### Creating and loading the table using schema inference

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

#### Creating a table by hand and then loading the data

By querying the data directly from S3 it is possible to decide the:

- column data types
- column names
- columns to use in the key

This may be the schema that you decide to use, although you may find that there are no nulls in the data:

```SQL
CREATE TABLE `user_behavior_declared` (
    `UserID` int(11) NULL COMMENT "",
    `ItemID` int(11) NULL COMMENT "",
    `CategoryID` int(11) NULL COMMENT "",
    `BehaviorType` varchar(65533) NULL COMMENT "",
    `Timestamp` datetime NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`UserID`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`UserID`) BUCKETS 1 
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "false",
    "replicated_storage" = "true",
    "compression" = "LZ4"
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

- For more details on synchronous and asynchronous data loading please see the [overview of data laoding](/docs/loading/Loading_intro.md) documentation.

- Learn about how the broker supports data transformation during loading at [Transform data at loading](../loading/Etl_in_loading.md) and [Change data through loading](../loading/Load_to_Primary_Key_tables.md).

- You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

- You can use the [SHOW BROKER](../sql-reference/sql-statements/Administration/SHOW%20BROKER.md) statement to check for brokers that are deployed in your StarRocks cluster. If no brokers are deployed, you can deploy brokers by following the instructions provided in [Deploy a broker](../deployment/deploy_broker.md).
