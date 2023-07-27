# CREATE TABLE

## Description

Creates a new table in StarRocks.

> **NOTE**
>
> This operation requires the CREATE TABLE privilege on the destination database.

## Syntax

```Plain%20Text
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
[distribution_desc]
[rollup_index]
[ORDER BY (column_definition1,...)]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

## Parameters

### column_definition

Syntax:

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

**col_name**: Column name.

**col_type**: Column type. Specific column information, such as types and ranges:

- TINYINT (1 byte): Ranges from -2^7 + 1 to 2^7 - 1.
- SMALLINT (2 bytes): Ranges from -2^15 + 1 to 2^15 - 1.
- INT (4 bytes): Ranges from -2^31 + 1 to 2^31 - 1.
- BIGINT (8 bytes): Ranges from -2^63 + 1 to 2^63 - 1.
- LARGEINT (16 bytes): Ranges from -2^127 + 1 to 2^127 - 1.
- FLOAT (4 bytes): Supports scientific notation.
- DOUBLE (8 bytes): Supports scientific notation.
- DECIMAL[(precision, scale)] (16 bytes)

  - Default value: DECIMAL(10, 0)
  - precision: 1 ~ 38
  - scale: 0 ~ precision
  - Integer part: precision - scale

    Scientific notation is not supported.

- DATE (3 bytes): Ranges from 0000-01-01 to 9999-12-31.
- DATETIME (8 bytes): Ranges from 0000-01-01 00:00:00 to 9999-12-31 23:59:59.
- CHAR[(length)]: Fixed length string. Range: 1 ~ 255. Default value: 1.
- VARCHAR[(length)]: A variable-length string. The default value is 1. Unit: bytes. In versions earlier than StarRocks 2.1, the value range of `length` is 1–65533. [Preview] In StarRocks 2.1 and later versions, the value range of `length` is 1–1048576.
- HLL (1~16385 bytes): For HLL type, there's no need to specify length or default value. The length will be controlled within the system according to data aggregation. HLL column can only be queried or used by [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md), [Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md), and [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md).
- BITMAP: Bitmap type does not require specified length or default value. It represents a set of unsigned bigint numbers. The largest element could be up to 2^64 - 1.

**agg_type**: aggregation type. If not specified, this column is key column.
If specified, it is value column. The aggregation types supported are as follows:

- SUM, MAX, MIN, REPLACE
- HLL_UNION (only for HLL type)
- BITMAP_UNION(only for BITMAP)
- REPLACE_IF_NOT_NULL: This means the imported data will only be replaced when it is of non-null value. If it is of null value, StarRocks will retain the original value.

> NOTE
>
> - When the column of aggregation type BITMAP_UNION is imported, its original data types must be TINYINT, SMALLINT, INT, and BIGINT.
> - If NOT NULL is specified by REPLACE_IF_NOT_NULL column when the table was created, StarRocks will still convert the data to NULL without sending an error report to the user. With this, the user can import selected columns.

This aggregation type applies ONLY to the Aggregate table whose key_desc type is AGGREGATE KEY.

**NULL | NOT NULL**: Whether the column is allowed to be `NULL`. By default, `NULL` is specified for all columns in a table that uses the Duplicate Key, Aggregate, or Unique Key table. In a table that uses the Primary Key table, by default, value columns are specified with `NULL`, whereas key columns are specified with `NOT NULL`. If `NULL` values are included in the raw data, present them with `\N`. StarRocks treats `\N` as `NULL` during data loading.

**DEFAULT "default_value"**: the default value of a column. When you load data into StarRocks, if the source field mapped onto the column is empty, StarRocks automatically fills the default value in the column. You can specify a default value in one of the following ways:

- **DEFAULT current_timestamp**: Use the current time as the default value. For more information, see [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md).
- **DEFAULT <default_value>**: Use a given value of the column data type as the default value. For example, if the data type of the column is VARCHAR, you can specify a VARCHAR string, such as beijing, as the default value, as presented in `DEFAULT "beijing"`. Note that default values cannot be any of the following types: ARRAY, BITMAP, JSON, HLL, and BOOLEAN.
- **DEFAULT (\<expr\>)**: Use the result returned by a given function as the default value. Only the [uuid()](../../sql-functions/utility-functions/uuid.md) and [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) expressions are supported.

**AUTO_INCREMENT**: specifies an `AUTO_INCREMENT` column. The data types of `AUTO_INCREMENT` columns must be BIGINT. Auto-incremented IDs start from 1 and increase at a step of 1. For more information about `AUTO_INCREMENT` columns, see [AUTO_INCREMENT](../../sql-statements/auto_increment.md). Since v3.0, StarRocks supports `AUTO_INCREMENT` columns.

**AS generation_expr**: specifies the generated column and its expression. [The generated column](../generated_columns.md) can be used to precompute and store the results of expressions, which significantly accelerates queries with the same complex expressions. Since v3.1, StarRocks supports generated columns.

### index_definition

You can only create bitmap indexes when you create tables. For more information about parameter descriptions and usage notes, see [Bitmap indexing](../../../using_starrocks/Bitmap_index.md#create-a-bitmap-index).

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

### ENGINE type

Default value: olap. If this parameter is not specified, an OLAP table (StarRocks native table) is created by default.

Optional value: mysql, elasticsearch, hive, jdbc (2.3 and later), iceberg, and hudi (2.2 and later). If you want to create an external table to query external data sources, specify `CREATE EXTERNAL TABLE` and set `ENGINE` to any of these values. You can refer to [External table](../../../data_source/External_table.md) for more information.

- For MySQL, specify the following properties:

    ```Plain%20Text
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
    )
    ```

    Note:

    "table_name" in MySQL should indicate the real table name. In contrast, "table_name" in CREATE TABLE statement indicates the name of this mysql table on StarRocks. They can either be different or the same.

    The aim of creating MySQL tables in StarRocks is to access MySQL database. StarRocks itself does not maintain or store any MySQL data.

- For Elasticsearch, specify the following properties:

    ```Plain%20Text
    PROPERTIES (

    "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

  - `hosts`: the URL that is used to connect your Elasticsearch cluster. You can specify one or more URLs.
  - `user`: the account of the root user that is used to log in to your Elasticsearch cluster for which basic authentication is enabled.
  - `password`: the password of the preceding root account.
  - `index`: the index of the StarRocks table in your Elasticsearch cluster. The index name is the same as the StarRocks table name. You can set this parameter to the alias of the StarRocks table.
  - `type`: the type of index. The default value is `doc`.

- For Hive, specify the following properties:

    ```Plain%20Text
    PROPERTIES (

        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    ```

    Here, database is the name of the corresponding database in Hive table. Table is the name of Hive table. hive.metastore.uris is the server address.

- For JDBC, specify the following properties:

    ```Plain%20Text
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` is the JDBC resource name and `table` is the destination table.

- For Iceberg, specify the following properties:

   ```Plain%20Text
    PROPERTIES (
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table"
    )
    ```

    `resource` is the Iceberg resource name. `database` is the Iceberg database. `table` is the Iceberg table.

- For Hudi, specify the following properties:

  ```Plain%20Text
    PROPERTIES (
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
    )
    ```

### key_desc

Syntax:

```SQL
key_type(k1[,k2 ...])
```

Data is sequenced in specified key columns and has different attributes for different key types:

- AGGREGATE KEY: Identical content in key columns will be aggregated into value columns according to the specified aggregation type. It usually applies to business scenarios such as financial statements and multi-dimensional analysis.
- UNIQUE KEY/PRIMARY KEY: Identical content in key columns will be replaced in value columns according to the import sequence. It can be applied to make addition, deletion, modification and query on key columns.
- DUPLICATE KEY: Identical content in key columns, which also exists in StarRocks at the same time. It can be used to store detailed data or data with no aggregation attributes. **DUPLICATE KEY is the default type. Data will be sequenced according to key columns.**

> **NOTE**
>
> Value columns do not need to specify aggregation types when other key_type is used to create tables with the exception of AGGREGATE KEY.

### COMMENT

You can add a table comment when you create a table, optional. Note that COMMENT must be placed after `key_desc`. Otherwise, the table cannot be created.

From v3.1 onwards, you can modify the table comment suing `ALTER TABLE <table_name> COMMENT = "new table comment"`.

### partition_desc

Partition description can be used in the following ways:

#### Create partitions dynamically

[Dynamic partitioning](../../../table_design/dynamic_partitioning.md) provides a time-to-live (TTL) management for partitions. StarRocks automatically creates new partitions in advance and removes expired partitions to ensure data freshness. To enable this feature, you can configure Dynamic partitioning related properties at table creation.

#### Create partitions one by one

**Specify only the upper bound for a partition**

Syntax:

```Plain%20Text
PARTITION BY RANGE (k1, k2, ...)
(
    PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
    PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
    ...
)
```

Note:

Please use specified key columns and specified value ranges for partitioning.

- Partition name only supports [A-z0-9_]
- Columns in Range partition only support the following types: TINYINT, SAMLLINT, INT, BIGINT, LARGEINT, DATE, and DATETIME.
- Partitions are left closed and right open. The left boundary of the first partition is of minimum value.
- NULL value is stored only in partitions that contain minimum values. When the partition containing the minimum value is deleted, NULL values can no longer be imported.
- Partition columns can either be single columns or multiple columns. The partition values are the default minimum values.

Please note:

- Partitions are often used for managing data related to time.
- When data backtracking is needed, you may want to consider emptying the first partition for adding partitions later when necessary.

**Specify both the lower and upper bounds for a partition**

Syntax:

```SQL
PARTITION BY RANGE (k1, k2, k3, ...)
(
    PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
    PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, )),
    "k3-upper1-2", ...
)
```

Note:

- Fixed Range is more flexible than LESS THAN. You can customize the left and right partitions.
- Fixed Range is the same as LESS THAN in the other aspects.

#### Create multiple partitions at a time

Syntax

- If the partitioning column is of an date type.

    ```Plain%20Text
    PARTITION BY RANGE (datekey) (
        START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 day)
    )
    ```

- If the partitioning column is of an integer type.

    ```Plain%20Text
    PARTITION BY RANGE (datekey) (
        START ("1") END ("5") EVERY (1)
    )
    ```

Description

You can specify the lower and upper bounds in `START` and `END` and the time unit or partitioning granunity in `EVERY` to create multiple partitions at a time.

- The partitioning column can be of a date or integer type, and the data type of values in `START()` and `END()`, and `EVERY()` must be the same as the data type of partitioning column.
- The partitioning column is of a data type, you need to use the `INTERVAL` keyword to specify the date interval. You can specify the time unit as hour (since v3.0), day, week, month, or year. The naming conventions of partitions are the same as those for dynamic partitions.

For more information, see [Data distribution](../../../table_design/Data_distribution.md).

### distribution_desc

StarRocks supports hash bucketing and random bucketing. If you do not configure bucketing, StarRocks uses random bucketing and automatically sets the number of buckets by default.

- Random bucketing (since v3.1)

  For data in a partition, StarRocks distributes the data randomly across all buckets, which is not based on specific column values. And if you want StarRocks to automatically determine the number of buckets, you do not need to specify any bucketing configurations. If you choose to manually specify the number of buckets, the syntax is as follows:

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```
  
  However, note that the query performance provided by random bucketing may not be ideal when you query massive amounts of data and frequently use certain columns as conditional columns. In this scenario, it is recommended to use hash bucketing. Because only a small number of buckets need to be scanned and computed, significantly improving query performance.

  **Precautions**
  - You can only use random bucketing to create Duplicate Key tables.
  - You can not specify a [Colocation Group](../../../using_starrocks/Colocate_join.md) for a table bucketed randomly.
  - [Spark Load](../../../loading/SparkLoad.md) cannot be used to load data into tables bucketed randomly.
  - Since StarRocks v2.5.7, you do not need to set the number of buckets when you create a table. StarRocks automatically sets the number of buckets. If you want to set this parameter, see [Determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets).

  For more information, see [Random bucketing](../../../table_design/Data_distribution.md#random-bucketing-since-v31).

- Hash bucketing

  Syntax:

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  Data in partitions can be subdivided into buckets based on the hash values of the bucketing columns and the number of buckets. We recommend that you choose the column that meets the following two requirements as the bucketing column.

  - High cardinality column such as ID
  - Column that is often used as a filter in queries

  If such a column does not exist, you can determine the bucketing column according to the complexity of queries.

  - If the query is complex, we recommend that you select a high cardinality column as the bucketing column to ensure balanced data distribution among buckets and improve cluster resource utilization.
  - If the query is relatively simple, we recommend that you select the column that is often used as the query condition as the bucketing column to improve query efficiency.

  If partition data cannot be evenly distributed into each bucket by using one bucketing column, you can choose multiple bucketing columns (at most three). For more information, see [Choose bucketing columns](../../../table_design/Data_distribution.md).

  **Precautions**:

  - **When you create a table, you must specify its bucketing columns**.
  - The values of bucketing columns cannot be updated.
  - Bucketing columns cannot be modified after they are specified.
  - Since StarRocks v2.5.7, you do not need to set the number of buckets when you create a table. StarRocks automatically sets the number of buckets. If you want to set this parameter, see [Determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets).

### ORDER BY

Since version 3.0, the primary key and sort key are decoupled in the Primary Key table. The sort key is specified by the `ORDER BY` keyword and can be the permutation and combination of any columns.

> **NOTICE**
>
> If the sort key is specified, the prefix index is built according to the sort key; if the sort key is not specified, the prefix index is built according to the primary key.

### PROPERTIES

#### Specify storage medium, storage cooldown time, replica number

If the engine type is `olap`, you can specify storage medium, storage cooldown time, and replica number when you create a table.

  > **NOTE**
  >
  > `storage_cooldown_time` can be configured only when `storage_medium` is set to `SSD`. If you want to set `storage_medium` to SSD, make sure that your cluster uses SSD disks, that is, `storage_root_path` reported by BEs includes SSD. For more information about `storage_root_path`, see [Configuration](../../../administration/Configuration.md#configure-be-static-parameters).

```Plain%20Text
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    [ "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss", ]
    [ "replication_num" = "3" ]
)
```

**storage_medium**: the initial storage medium, which can be set to SSD or HDD.

> **NOTE**
>
> - From 2.5.1, the system automatically infers storage medium based on BE disk type if `storage_medium` is not explicitly specified. Inference mechanism: If `storage_root_path` reported by BEs contain only SSD, the system automatically sets this parameter to SSD. If `storage_root_path` reported by BEs contain only HDD, the system automatically sets this parameter to HDD. If `storage_root_path` reported by BEs contain both SSD and HDD, the system automatically sets this parameter to SSD. From 2.3.10, 2.4.5, 2.5.4 onwards, if `storage_root_path` reported by BEs contain both SSD and HDD and the property `storage_cooldown_time` is specified, `storage_medium` is set to SSD; if the property `storage_cooldown_time` is not specified, `storage_medium` is set to HDD.
> - If the FE configuration item `enable_strict_storage_medium_check` is set to `true`, the system strictly checks BE disk type when you create a table. If the storage medium you specified in CREATE TABLE is inconsistent with BE disk type, an error "Failed to find enough host in all backends with storage medium is SSD|HDD." is returned and table creation fails. If `enable_strict_storage_medium_check` is set to `false`, the system ignores this error and forcibly creates the table. However, cluster disk space may be unevenly distributed after data is loaded.

**storage_cooldown_time**: the storage cooldown time for a partition. If the storage medium is SSD, SSD is switched to HDD after the time specified by this parameter. Format: "yyyy-MM-dd HH:mm:ss". The specified time must be later than the current time. If this parameter is not explicitly specified, storage cooldown is not performed by default.

**replication_num**: number of replicas in the specified partition. Default number: 3.

If the table has only one partition, the properties belong to the table. If the table has two levels of partitions, the properties belong to each partition. You can also specify different properties for different partitions by using ALTER TABLE ADD PARTITION or ALTER TABLE MODIFY PARTITION.

#### Add bloom filter index for a column

If the Engine type is olap, you can specify a column to adopt bloom filter indexes.

The following limits apply when you use bloom filter index:

- You can create bloom filter indexes for all columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create bloom filter indexes for key columns.
- TINYINT, FLOAT, DOUBLE, and DECIMAL columns do not support creating bloom filter indexes.
- Bloom filter indexes can only improve the performance of queries that contain the `in` and `=` operators, such as `Select xxx from table where x in {}` and `Select xxx from table where column = xxx`. More discrete values in this column will result in more precise queries.

For more information, see [Bloom filter indexing](../../../using_starrocks/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

#### Use Colocate Join

If you want to use Colocate Join attributes, specify it in `properties`.

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

#### Configure dynamic partitions

If you want to use dynamic partition attributes, please specify it in properties.

```SQL
PROPERTIES (

    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
```

**`PROPERTIES`**

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| dynamic_partition.enable    | No       | Whether to enable dynamic partitioning. Valid values: `TRUE` and `FALSE`. Default value: `TRUE`. |
| dynamic_partition.time_unit | Yes      | The time granularity for dynamically created  partitions. It is a required parameter. Valid values: `DAY`, `WEEK`, and `MONTH`. The time granularity determines the suffix format for dynamically created partitions.<br/>  - If the value is `DAY`, the suffix format for dynamically created partitions is `yyyyMMdd`. An example partition name suffix is `20200321`.<br/>  - If the value is `WEEK`, the suffix format for dynamically created partitions is `yyyy_ww`, for example `2020_13` for the 13th week of 2020.<br/>  - If the value is `MONTH`, the suffix format for dynamically created partitions is `yyyyMM`, for example `202003`. |
| dynamic_partition.start     | No       | The starting offset of dynamic partitioning. The value of this parameter must be a negative integer. The partitions before this offset will be deleted based on the current day, week, or month which is determined by `dynamic_partition.time_unit`. The default value is `Integer.MIN_VALUE`, namely, -2147483648, which means that historical partitions will not be deleted. |
| dynamic_partition.end       | Yes      | The end offset of dynamic partitioning. The value of this parameter must be a positive integer. The partitions from the current day, week, or month to the end offset will be created in advance. |
| dynamic_partition.prefix    | No       | The prefix added to the names of dynamic partitions. Default value: `p`. |
| dynamic_partition.buckets   | No       | The number of buckets per dynamic partition. The default value is the same as the number of buckets determined by the reserved word `BUCKETS` or automatically set by StarRocks. |

#### Set data compression algorithm

You can specify a data compression algorithm for a table by adding property `compression` when you create a table.

The valid values of `compression` are:

- `LZ4`: the LZ4 algorithm.
- `ZSTD`: the Zstandard algorithm.
- `ZLIB`: the zlib algorithm.
- `SNAPPY`: the Snappy algorithm.

For more information about how to choose a suitable data compression algorithm, see [Data compression](../../../table_design/data_compression.md).

#### Set write quorum for data loading

If your StarRocks cluster has multiple data replicas, you can set different write quorum for tables, that is, how many replicas are required to return loading success before StarRocks can determine the loading task is successful. You can specify write quorum by adding the property `write_quorum` when you create a table. This property is supported from v2.5.

The valid values of `write_quorum` are:

- `MAJORITY`: Default value. When the **majority** of data replicas return loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.
- `ONE`: When **one** of the data replicas returns loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.
- `ALL`: When **all** of the data replicas return loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.

> **CAUTION**
>
> - Setting a low write quorum for loading increases the risk of data inaccessibility and even loss. For example, you load data into a table with one write quorum in a StarRocks cluster of two replicas, and the data was successfully loaded into only one replica. Despite that StarRocks determines the loading task succeeded, there is only one surviving replica of the data. If the server which stores the tablets of loaded data goes down, the data in these tablets becomes inaccessible. And if the disk of the server is damaged, the data is lost.
> - StarRocks returns the loading task status only after all data replicas have returned the status. StarRocks will not return the loading task status when there are replicas whose loading status is unknown. In a replica, loading timeout is also considered as loading failed.

#### Specify data writing and replication mode among replicas

If your StarRocks cluster has multiple data replicas, you can specify the `replicated_storage` parameter in `PROPERTIES` to configure the data writing and replication mode among replicas.

- `true` (**default value**) indicates "single leader replication", which means data is written only to the primary replica. Other replicas synchronize data from the primary replica. This mode significantly reduces CPU cost caused by data writing to multiple replicas. It is supported from v2.5.
- `false` indicates "leaderless replication", which means data is directly written to multiple replicas, without differentiating primary and secondary replicas. The CPU cost is multiplied by the number of replicas.

In most cases, using the default value gains better data writing performance. If you want to change the data writing and replication mode among replicas, run the ALTER TABLE command. Example:

```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
```

#### Create rollup in bulk

You can create rollup in bulk when you create a table.

Syntax:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

#### Define Unique Key constraints and Foreign Key constraints for View Delta Join query rewrite

To enable query rewrite in the View Delta Join scenario, you must define the Unique Key constraints `unique_constraints` and Foreign Key constraints `foreign_key_constraints` for the table to be joined in the Delta Join. See [Asynchronous materialized view - Rewrite queries in View Delta Join scenario](../../../using_starrocks/Materialized_view.md#rewrite-queries-in-view-delta-join-scenario) for further information.

```SQL
PROPERTIES (
    "unique_constraints" = "<unique_key>[, ...]",
    "foreign_key_constraints" = "
    (<child_column>[, ...]) 
    REFERENCES 
    [catalog_name].[database_name].<parent_table_name>(<parent_column>[, ...])
    [;...]
    "
)
```

- `child_column`: the Foreign Key of the table. You can define multiple `child_column`.
- `catalog_name`: the name of the catalog where the table to join resides. The default catalog is used if this parameter is not specified.
- `database_name`: the name of the database where the table to join resides. The current database is used if this parameter is not specified.
- `parent_table_name`: the name of the table to join.
- `parent_column`: the column to be joined. They must be the Primary Keys or Unique Keys of the corresponding tables.

> **CAUTION**
>
> - The Unique Key constraints and Foreign Key constraints are only used for query rewrite. The Foreign Key constraint checks are not guaranteed when data is loaded into the table. You must ensure the data loaded into the table meets the constraints.
> - The primary keys of a Primary Key table or the unique keys of a Unique Key table are, by default, the corresponding `unique_constraints`. You do not need to set it manually.
> - The `child_column` in a table's `foreign_key_constraints` must be referenced to a `unique_key` in another table's `unique_constraints`.
> - The number of `child_column` and `parent_column` must agree.
> - The data types of the `child_column` and the corresponding `parent_column` must match.

#### Create cloud-native tables for StarRocks Shared-data cluster

To [use your StarRocks Shared-data cluster](../../../deployment/deploy_shared_data.md#use-your-shared-data-starrocks-cluster), you must create cloud-native tables with the following properties:

```SQL
PROPERTIES (
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "enable_async_write_back" = "{ true | false }"
)
```

- `datacache.enable`: Whether to enable the local disk cache. Default: `true`.

  - When this property is set to `true`, the data to be loaded is simultaneously written into the object storage and the local disk (as the cache for query acceleration).
  - When this property is set to `false`, the data is loaded only into the object storage.

  > **NOTE**
  >
  > To enable the local disk cache, you must specify the directory of the disk in the BE configuration item `storage_root_path`. For more information, see [BE Configuration items](../../../administration/Configuration.md#be-configuration-items).

- `datacache.partition_duration`: The validity duration of the hot data. When the local disk cache is enabled, all data is loaded into the cache. When the cache is full, StarRocks deletes the less recently used data from the cache. When a query needs to scan the deleted data, StarRocks checks if the data is within the duration of validity. If the data is within the duration, StarRocks loads the data into the cache again. If the data is not within the duration, StarRocks does not load it into the cache. This property is a string value that can be specified with the following units: `YEAR`, `MONTH`, `DAY`, and `HOUR`, for example, `7 DAY` and `12 HOUR`. If it is not specified, all data is cached as the hot data.

  > **NOTE**
  >
  > This property is available only when `datacache.enable` is set to `true`.

- `enable_async_write_back`: Whether to allow data to be written into object storage asynchronously. Default: `false`.

  - When this property is set to `true`, the load task returns success as soon as the data is written into the local disk cache, and the data is written into the object storage asynchronously. This allows better loading performance, but it also risks data reliability under potential system failures.
  - When this property is set to `false`, the load task returns success only after the data is written into both object storage and the local disk cache. This guarantees higher availability but leads to lower loading performance.

## Examples

### Create an Aggregate table that uses Hash bucketing and columnar storage

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Create an Aggregate table and set the storage medium and cooldown time

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Or

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### Create a Duplicate Key table that uses Range partition, Hash bucketing, and column-based storage, and set the storage medium and cooldown time

LESS THAN

```SQL
CREATE TABLE example_db.table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD", 
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Note:

This statement will create three data partitions:

```SQL
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

Data outside these ranges will be not be loaded.

Fixed Range

```SQL
CREATE TABLE table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1, k2, k3)
(
    PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
    PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD"
);
```

### Create a MySQL external table

```SQL
CREATE EXTERNAL TABLE example_db.table_mysql
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
)
```

### Create a table that contains HLL columns

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Create a table containing BITMAP_UNION aggregation type

The original data type of `v1` and `v2` columns must be TINYINT, SMALLINT, or INT.

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Create two tables that support Colocate Join

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);

CREATE TABLE `t2` 
(
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### Create a table with bitmap index

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Create a dynamic partition table

The dynamic partitioning function must be enabled ("dynamic_partition.enable" = "true") in FE configuration. For more information, see [Configure dynamic partitions](#configure-dynamic-partitions).

This example creates partitions for the next three days and deletes partitions created three days ago. For example, if today is 2020-01-08, partitions with the following names will be created: p20200108, p20200109, p20200110, p20200111, and their ranges are:

```Plain%20Text
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```SQL
CREATE TABLE example_db.dynamic_partition
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
```

### Create a Hive external table

Before you create a Hive external table, you must have created a Hive resource and database. For more information, see [External table](../../../data_source/External_table.md#hive-external-table).

```SQL
CREATE EXTERNAL TABLE example_db.table_hive
(
    k1 TINYINT,
    k2 VARCHAR(50),
    v INT
)
ENGINE=hive
PROPERTIES
(
    "resource" = "hive0",
    "database" = "hive_db_name",
    "table" = "hive_table_name"
);
```

### Create a Primary Key table and specify the sort key

Suppose that you need to analyze user behavior in real time from dimensions such as users' address and last active time. When you create a table, you can define the `user_id` column as the primary key and define the combination of the `address` and `last_active` columns as the sort key.

```SQL
create table users (
    user_id bigint NOT NULL,
    name string NOT NULL,
    email string NULL,
    address string NULL,
    age tinyint NULL,
    sex tinyint NULL,
    last_active datetime,
    property0 tinyint NOT NULL,
    property1 tinyint NOT NULL,
    property2 tinyint NOT NULL,
    property3 tinyint NOT NULL
) 
PRIMARY KEY (`user_id`)
DISTRIBUTED BY HASH(`user_id`)
ORDER BY(`address`,`last_active`)
PROPERTIES(
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

## References

- [SHOW CREATE TABLE](../data-manipulation/SHOW%20CREATE%20TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW%20TABLES.md)
- [USE](USE.md)
- [ALTER TABLE](ALTER%20TABLE.md)
- [DROP TABLE](DROP%20TABLE.md)
