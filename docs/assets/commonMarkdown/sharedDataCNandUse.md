### Configure CN nodes for shared-data StarRocks

**Before starting CNs**, add the following configuration items in the CN configuration file **cn.conf**:

```Properties
starlet_port = <starlet_port>
storage_root_path = <storage_root_path>
```

#### starlet_port

The CN heartbeat service port for the StarRocks shared-data cluster. Default value: `9070`.

#### storage_root_path

The storage volume directory that the local cached data depends on and the medium type of the storage. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`. Default value: `${STARROCKS_HOME}/storage`.

> **NOTE**
>
> The data is cached under the directory **<storage_root_path\>/starlet_cache**.

## Use your shared-data StarRocks cluster

The usage of shared-data StarRocks clusters is also similar to that of a classic StarRocks cluster, except that the shared-data cluster uses storage volumes and cloud-native tables to store data in object storage.

### Create default storage volume

You can use the built-in storage volumes that StarRocks automatically creates, or you can manually create and set the default storage volume. This section describes how to manually create and set the default storage volume.

> **NOTE**
>
> If your shared-data StarRocks cluster is upgraded from v3.0, you do not need to define a default storage volume because StarRocks created one with the object storage-related properties you specified in the FE configuration file **fe.conf**. You can still create new storage volumes with other object storage resources and set the default storage volume differently.

To give your shared-data StarRocks cluster permission to store data in your object storage, you must reference a storage volume when you create databases or cloud-native tables. A storage volume consists of the properties and credential information of the remote data storage. If you have deployed a new shared-data StarRocks cluster and disallow StarRocks to create a built-in storage volume (by specifying `enable_load_volume_from_conf` as `false`), you must define a default storage volume before you can create databases and tables in the cluster.

The following example creates a storage volume `def_volume` for an AWS S3 bucket `defaultbucket` with the IAM user-based credential (Access Key and Secret Key), enables the storage volume, and sets it as the default storage volume:

```SQL
CREATE STORAGE VOLUME def_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);

SET def_volume AS DEFAULT STORAGE VOLUME;
```

For more information on how to create a storage volume for other object storages and set the default storage volume, see [CREATE STORAGE VOLUME](../sql-reference/sql-statements/Administration/CREATE%20STORAGE%20VOLUME.md) and [SET DEFAULT STORAGE VOLUME](../sql-reference/sql-statements/Administration/SET%20DEFAULT%20STORAGE%20VOLUME.md).

### Create a database and a cloud-native table

After you created a default storage volume, you can then create a database and a cloud-native table using this storage volume.

Currently, shared-data StarRocks clusters support the following table types:

- Duplicate Key table
- Aggregate table
- Unique Key table
- Primary Key table (Currently, the primary key persistent index is not supported.)

The following example creates a database `cloud_db` and a table `detail_demo` based on Duplicate Key table type, enables the local disk cache, sets the hot data validity duration to one month, and disables asynchronous data ingestion into object storage:

```SQL
CREATE DATABASE cloud_db;
USE cloud_db;
CREATE TABLE IF NOT EXISTS detail_demo (
    recruit_date  DATE           NOT NULL COMMENT "YYYY-MM-DD",
    region_num    TINYINT        COMMENT "range [-128, 127]",
    num_plate     SMALLINT       COMMENT "range [-32768, 32767] ",
    tel           INT            COMMENT "range [-2147483648, 2147483647]",
    id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    password      LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    name          CHAR(20)       NOT NULL COMMENT "range char(m),m in (1-255) ",
    profile       VARCHAR(500)   NOT NULL COMMENT "upper limit value 65533 bytes",
    ispass        BOOLEAN        COMMENT "true/false")
DUPLICATE KEY(recruit_date, region_num)
DISTRIBUTED BY HASH(recruit_date, region_num)
PROPERTIES (
    "storage_volume" = "def_volume",
    "datacache.enable" = "true",
    "datacache.partition_duration" = "1 MONTH",
    "enable_async_write_back" = "false"
);
```

> **NOTE**
>
> The default storage volume is used when you create a database or a cloud-native table in a shared-data StarRocks cluster if no storage volume is specified.

In addition to the regular table PROPERTIES, you need to specify the following PROPERTIES when creating a table for shared-data StarRocks cluster:

#### datacache.enable

Whether to enable the local disk cache.

- `true` (Default) When this property is set to `true`, the data to be loaded is simultaneously written into the object storage and the local disk (as the cache for query acceleration).
- `false` When this property is set to `false`, the data is loaded only into the object storage.

> **NOTE**
>
> To enable the local disk cache, you must specify the directory of the disk in the CN configuration item `storage_root_path`.

#### datacache.partition_duration

The validity duration of the hot data. When the local disk cache is enabled, all data is loaded into the cache. When the cache is full, StarRocks deletes the less recently used data from the cache. When a query needs to scan the deleted data, StarRocks checks if the data is within the duration of validity. If the data is within the duration, StarRocks loads the data into the cache again. If the data is not within the duration, StarRocks does not load it into the cache. This property is a string value that can be specified with the following units: `YEAR`, `MONTH`, `DAY`, and `HOUR`, for example, `7 DAY` and `12 HOUR`. If it is not specified, all data is cached as the hot data.

> **NOTE**
>
> This property is available only when `datacache.enable` is set to `true`.

#### enable_async_write_back

Whether to allow data to be written into object storage asynchronously. Default: `false`.
- `true` When this property is set to `true`, the load task returns success as soon as the data is written into the local disk cache, and the data is written into the object storage asynchronously. This allows better loading performance, but it also risks data reliability under potential system failures.
- `false` (Default) When this property is set to `false`, the load task returns success only after the data is written into both object storage and the local disk cache. This guarantees higher availability but leads to lower loading performance.

### View table information

You can view the information of tables in a specific database using `SHOW PROC "/dbs/<db_id>"`. See [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW%20PROC.md) for more information.

Example:

```Plain
mysql> SHOW PROC "/dbs/xxxxx";
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| 12003   | detail_demo | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 8            | UNPARTITIONED | s3://xxxxxxxxxxxxxx/1/12003/ |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
```

The `Type` of a table in shared-data StarRocks cluster is `CLOUD_NATIVE`. In the field `StoragePath`, StarRocks returns the object storage directory where the table is stored.

### Load data into a shared-data StarRocks cluster

Shared-data StarRocks clusters support all loading methods provided by StarRocks. See [Overview of data loading](../loading/Loading_intro.md) for more information.

### Query in a shared-data StarRocks cluster

Tables in a shared-data StarRocks cluster support all types of queries provided by StarRocks. See StarRocks [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) for more information.
