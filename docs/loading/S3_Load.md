# Load data from Amazon S3

## Overview

StarRocks provides two options for loading data from S3:

1. Asynchronous loading using a *broker*
2. Synchronous loading using the `FILES()` table function

Small datasets are often loaded synchronously using the `FILES()` table function, and large datasets are often loaded asynchronously using a broker. The two methods have different advantages and are described below.

## Asynchronous loading using a broker

An asynchronous *broker* process handles making the connection to S3, pulling the data, and storing the data in the StarRocks datastore.

### Advantages of asynchronous loading

- Because asynchronous jobs run in the background a client does not need to stay connected for the job to continue. Check the progress of a load job by connecting a client and querying the load job state.
- Asynchronous loading is preferred for long running jobs. The default timeout for a broker load job is 4 hours.

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
CREATE DATABASE project;
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
> The sample dataset is the StarRocks bucket is available with any valid `aws.s3.access_key` and `aws.s3.secret_key`; in the `BROKER` section replace the `AAA` and `BBB` placeholders with your credentials.

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

`FILES()` can infer the data types of the columns of the Parquet data and generate the schema for a StarRocks table. This provides the ability to query the file directly from S3 with a `SELECT` or to create the table structure in StarRocks as part of the data loading process.

### Data flow

Multiple examples:
1. SELECT
2. infer
3. Create table and then insert from FILES()

### Typical example

Multiple examples:
1. SELECT
2. infer
3. Create table and then insert from FILES()

Create a table, start a load process that pulls a Parquet file from S3, and verify the progress and success of the data loading.

#### Create a table

Create a database for your table:

```SQL
CREATE DATABASE project;
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
> The sample dataset is the StarRocks bucket is available with any valid `aws.s3.access_key` and `aws.s3.secret_key`; in the `BROKER` section replace the `AAA` and `BBB` placeholders with your credentials.

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











## Details

## How it works

After you submit a load job to an FE, the FE generates a query plan, splits the query plan into portions based on the number of available BEs and the size of the data file you want to load, and then assigns each portion of the query plan to an available BE. During the load, each involved BE pulls the data of the data file from your external storage system, pre-processes the data, and then loads the data into your StarRocks cluster. After all BEs finish their portions of the query plan, the FE determines whether the load job is successful.

The following figure shows the workflow of a Broker Load job.

![Workflow of Broker Load](../assets/broker_load_how-to-work_en.png)

StarRocks supports using a [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) to load large amounts of data from SOURCE into StarRocks.

Broker Load runs in asynchronous loading mode. After you submit a load job, StarRocks asynchronously runs the job. You can use `SELECT * FROM information_schema.loads` to query the job result. This feature is supported from v3.1 onwards. For more information, see the "[View a load job](#view-a-load-job)" section of this topic.

Broker Load ensures the transactional atomicity of each load job that is run to load multiple data files, which means that the loading of multiple data files in one load job must all succeed or fail. It never happens that the loading of some data files succeeds while the loading of the other files fails.

Additionally, Broker Load supports data transformation at data loading and supports data changes made by UPSERT and DELETE operations during data loading. For more information, see [Transform data at loading](../loading/Etl_in_loading.md) and [Change data through loading](../loading/Load_to_Primary_Key_tables.md).

> **NOTICE**
>
> You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Background information

In v2.4 and earlier, StarRocks depends on brokers to set up connections between your StarRocks cluster and your external storage system when it runs a Broker Load job. Therefore, you need to input `WITH BROKER "<broker_name>"` to specify the broker you want to use in the load statement. This is called "broker-based loading." A broker is an independent, stateless service that is integrated with a file-system interface. With brokers, StarRocks can access and read data files that are stored in your external storage system, and can use its own computing resources to pre-process and load the data of these data files.

From v2.5 onwards, StarRocks no longer depends on brokers to set up connections between your StarRocks cluster and your external storage system when it runs a Broker Load job. Therefore, you no longer need to specify a broker in the load statement, but you still need to retain the `WITH BROKER` keyword. This is called "broker-free loading."

> **NOTE**
>
> You can use the [SHOW BROKER](../sql-reference/sql-statements/Administration/SHOW%20BROKER.md) statement to check for brokers that are deployed in your StarRocks cluster. If no brokers are deployed, you can deploy brokers by following the instructions provided in [Deploy a broker](../deployment/deploy_broker.md).

## Supported data file formats

Broker Load supports the following data file formats:

- CSV

- Parquet

- ORC

> **NOTE**
>
> For CSV data, take note of the following points:
>
> - You can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.
> - Null values are denoted by using `\N`. For example, a data file consists of three columns, and a record from that data file holds data in the first and third columns but no data in the second column. In this situation, you need to use `\N` in the second column to denote a null value. This means the record must be compiled as `a,\N,b` instead of `a,,b`. `a,,b` denotes that the second column of the record holds an empty string.

