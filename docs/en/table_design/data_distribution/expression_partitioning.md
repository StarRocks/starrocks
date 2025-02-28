---
displayed_sidebar: docs
description: Partition data in StarRocks
sidebar_position: 10
---

# Expression partitioning (recommended)

Since v3.0, StarRocks has supported expression partitioning (previously known as automatic partitioning), which is more flexible and user-friendly. This partitioning method is suitable for most scenarios such as querying and managing data based on continuous time ranges or ENUM values.

You only need to specify a simple partition expression at table creation. During data loading, StarRocks will automatically create partitions based on the data and the rule defined in the partition expression. You no longer need to manually create numerous partitions at table creation, nor configure dynamic partition properties.

From v3.4 onwards, expression partitioning is further optimized to unify all partitioning strategies and supported more complex solutions. It is recommended in most cases, and will replace the other partitioning strategies in future releases.

## Partitioning based on a simple time function expression

If you frequently query and manage data based on continuous time ranges, you only need to specify a date type (DATE or DATETIME) column as the partition column and specify year, month, day, or hour as the partition granularity in the time function expression. StarRocks will automatically create partitions and set the partitions' start and end dates or datetime based on the loaded data and partition expression.

However, in some special scenarios, such as partitioning historical data into partitions by month and recent data into partitions by day, you must use [range partitioning](./Data_distribution.md#range-partitioning) to create partitions.

### Syntax

```sql
PARTITION BY expression
...
[ PROPERTIES( { 'partition_live_number' = 'xxx' | 'partition_retention_condition' = 'expr' } ) ]

expression ::=
    { date_trunc ( <time_unit> , <partition_column> ) |
      time_slice ( <partition_column> , INTERVAL <N> <time_unit> [ , boundary ] ) }
```

### Parameters

#### `expression`

**Required**: YES<br/>
**Description**: A simple time function expression that use the [date_trunc](../../sql-reference/sql-functions/date-time-functions/date_trunc.md) or [time_slice](../../sql-reference/sql-functions/date-time-functions/time_slice.md) functions. If you use the function `time_slice`, you do not need to pass the `boundary` parameter. It is because in this scenario, the default and valid value for this parameter is `floor`, and the value cannot be `ceil`. <br/>

#### `time_unit`

**Required**: YES<br/>
**Description**: The partition granularity, which can be `hour`, `day`, `month`, or `year`. `week` partition granularity is not supported. If partition granularity is `hour`, the partition column must be of the DATETIME data type and cannot be of the DATE data type. <br/>

#### `partition_column` 

**Required**: YES<br/>
**Description**: The name of the partition column.<br/><ul><li>The partition column can only be of the DATE or DATETIME data type. The partition column allows `NULL` values.</li><li>The partition column can be of the DATE or DATETIME data type if the `date_trunc` function is used. The partition column must be of the DATETIME data type if the `time_slice` function is used. </li><li>If the partition column is of the DATE data type, the supported range is [0000-01-01 ~ 9999-12-31]. If the partition column is of the DATETIME data type, the supported range is [0000-01-01 01:01:01 ~ 9999-12-31 23:59:59].</li><li>Currently, you can specify only one partition column; multiple partition columns are not supported.</li></ul> <br/>

#### `partition_live_number` 

**Required**: NO<br/>
**Description**: The number of the most recent partitions to be retained. Partitions are sorted in chronological order, **with the current date as a benchmark**; partitions older than the current date minus `partition_live_number` are deleted. StarRocks schedules tasks to manage the number of partitions, and the scheduling interval can be configured through the FE dynamic parameter `dynamic_partition_check_interval_seconds`, which defaults to 600 seconds (10 minutes). Suppose that the current date is April 4, 2023, `partition_live_number` is set to `2`, and the partitions include `p20230401`, `p20230402`, `p20230403`, `p20230404`. The partitions `p20230403` and `p20230404` are retained, and other partitions are deleted. If dirty data is loaded, such as data from the future dates April 5 and April 6, partitions include `p20230401`, `p20230402`, `p20230403`, `p20230404`, and `p20230405`, and `p20230406`. Then partitions `p20230403`, `p20230404`, `p20230405`, and `p20230406` are retained, and the other partitions are deleted. <br/>

#### `partition_retention_condition`

From v3.4.1 onwards, StarRocks native tables support Common Partition Expression TTL.

`partition_retention_condition`: The expression that declares the partitions to be retained dynamically. Partitions that do not meet the condition in the expression will be dropped regularly. Example: `'partition_retention_condition' = 'dt >= CURRENT_DATE() - INTERVAL 3 MONTH'`.
- The expression can only contain partition columns and constants. Non-partition columns are not supported.
- Common Partition Expression applies to List partitions and Range partitions differently:
  - For tables with List partitions, StarRocks supports deleting partitions filtered by the Common Partition Expression.
  - For tables with Range partitions, StarRocks can only filter and delete partitions using the partition pruning capability of FE. Partitions correspond to predicates that are not supported by partition pruning cannot be filtered and deleted.

### Usage notes

- During data loading, StarRocks automatically creates some partitions based on the loaded data, but if the load job fails for some reason, the partitions that are automatically created by StarRocks cannot be automatically deleted.
- StarRocks sets the default maximum number of automatically created partitions for one load to 4096, which can be configured by the FE parameter `auto_partition_max_creation_number_per_load`. This parameter can prevent you from accidentally creating too many partitions.
- The naming rule for partitions is consistent with the naming rule for dynamic partitioning.

### Examples

Example 1: Suppose you frequently query data by day. You can use the partition expression `date_trunc()` and set the partition column as `event_day` and the partition granularity as `day` at table creation. Data is automatically partitioned based on dates during loading. Data of the same day is stored in one partition and partition pruning can be used to significantly improve query efficiency.

```SQL
CREATE TABLE site_access1 (
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day, site_id);
```

For example, when the following two data rows are loaded, StarRocks will automatically create two partitions, `p20230226` and `p20230227`, with ranges [2023-02-26 00:00:00, 2023-02-27 00:00:00) and [2023-02-27 00:00:00, 2023-02-28 00:00:00) respectively. If subsequent loaded data falls within these ranges, they are automatically routed to the corresponding partitions.

```SQL
-- insert two data rows
INSERT INTO site_access1  
    VALUES ("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
           ("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- view partitions
mysql > SHOW PARTITIONS FROM site_access1;
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range                                                                                                | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| 17138       | p20230226     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-26 00:00:00]; ..types: [DATETIME]; keys: [2023-02-27 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | false      | 0        |
| 17113       | p20230227     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-27 00:00:00]; ..types: [DATETIME]; keys: [2023-02-28 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | false      | 0        |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
2 rows in set (0.00 sec)
```

Example 2: If you want to implement partition lifecycle management, which is to retain only a certain number of recent partitions and delete historical partitions, you can use the `partition_live_number` property to specify the number of partitions to retain.

```SQL
CREATE TABLE site_access2 (
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
) 
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES(
    "partition_live_number" = "3" -- only retains the most recent three partitions
);
```

Example 3: Suppose you frequently query data by week. You can use the partition expression `time_slice()` and set the partition column as `event_day` and the partition granularity to seven days at table creation. Data of one week is stored in one partition and partition pruning can be used to significantly improve query efficiency.

```SQL
CREATE TABLE site_access(
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY time_slice(event_day, INTERVAL 7 day)
DISTRIBUTED BY HASH(event_day, site_id)
```

## Partitioning based on the column expression (since v3.1)

If you frequently query and manage data of a specific type, you only need to specify the column representing the type as the partition column. StarRocks will automatically create partitions based on the partition column values of the loaded data.

However, in some special scenarios, such as when the table contains a column `city`, and you frequently query and manage data based on countries and cities. You must use [list partitioning](./list_partitioning.md) to store data of multiple cities within the same country in one partition.

### Syntax

```sql
PARTITION BY expression
...

expression ::=
    partition_columns 
    
partition_columns ::=
    <column>, [ <column> [,...] ]
```

### Parameters

#### `partition_columns`

**Required**: YES<br/>
**Description**: The names of partition columns.<br/> <ul><li>The partition column values can be string (BINARY not supported), date or datetime, integer, and boolean values. The partition column allows `NULL` values.</li><li> Each partition can only contain data with the same value in the partition column. To include data with different values in a partition column in a partition, see [List partitioning](./list_partitioning.md).</li></ul> <br/>


### Usage notes

- During data loading, StarRocks automatically creates some partitions based on the loaded data, but if the load job fails for some reason, the partitions that are automatically created by StarRocks cannot be automatically deleted.
- StarRocks sets the default maximum number of automatically created partitions for one load to 4096, which can be configured by the FE parameter `auto_partition_max_creation_number_per_load`. This parameter can prevent you from accidentally creating too many partitions.
- The naming rule for partitions: if multiple partition columns are specified, the values of different partition columns are connected with an underscore `_` in the partition name, and the format is `p<value in partition column 1>_<value in partition column 2>_...`. For example, if two columns `dt` and `province` are specified as partition columns, both of which are string types, and a data row with values `2022-04-01` and `beijing` is loaded, the corresponding partition automatically created is named `p20220401_beijing`.

### Examples

Example 1: Suppose you frequently query details of the data center billing based on time ranges and specific cities. At table creation, you can use a partition expression to specify the first partition columns as `dt` and `city`. This way, data belonging to the same date and city are routed into the same partition, and partition pruning can be used to significantly improve query efficiency.

```SQL
CREATE TABLE t_recharge_detail1 (
    id bigint,
    user_id bigint,
    recharge_money decimal(32,2), 
    city varchar(20) not null,
    dt varchar(20) not null
)
DUPLICATE KEY(id)
PARTITION BY dt,city
DISTRIBUTED BY HASH(`id`);
```

Insert a single data row into the table.

```SQL
INSERT INTO t_recharge_detail1 
    VALUES (1, 1, 1, 'Houston', '2022-04-01');
```

View the partitions. The result shows that StarRocks automatically creates a partition `p20220401_Houston1` based on the loaded data. During subsequent loading, data with the values `2022-04-01` and `Houston` in the partition columns `dt` and `city` are stored in this partition.

:::tip
Each partition can only contain data with the specified one value for the partition column. To specify multiple values for a partition column in a partition, see [List partitions](./list_partitioning.md).
:::

```SQL
MySQL > SHOW PARTITIONS from t_recharge_detail1\G
*************************** 1. row ***************************
             PartitionId: 16890
           PartitionName: p20220401_Houston
          VisibleVersion: 2
      VisibleVersionTime: 2023-07-19 17:24:53
      VisibleVersionHash: 0
                   State: NORMAL
            PartitionKey: dt, city
                    List: (('2022-04-01', 'Houston'))
         DistributionKey: id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 2.5KB
              IsInMemory: false
                RowCount: 1
1 row in set (0.00 sec)
```

## Partitioning based on a complex time function expression (since v3.4)

From v3.4.0 onwards, expression partitioning supports any expressions that return DATE or DATETIME types to accommodate to even more complex partitioning scenarios.

For example, you can define a Unix timestamp column, and use from_unixtime() directly against the column in the partition expression to define the partition key, instead of define a generated DATE or DATETIME column with the function. For more about the usage, see [Examples](#examples-2).

### Examples

Example 1: Suppose you assign a Unix timestamp to each data row and frequently query data by day. You can use the timestamp column with the function from_unixtime() in the expression to define the timestamp as the partition column and the partition granularity to a day at table creation. Data of each day is stored in one partition and partition pruning can be used to improve query efficiency.

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d');
```

Example 2: Suppose you assign an irregular STRING type timestamp to each data row and frequently query data by day. You can use the timestamp column with the functions cast() and date_parse() in the expression to transform the timestamp into the DATE type, set it as the partition column, and the partition granularity to a day at table creation. Data of each day is stored in one partition and partition pruning can be used to improve query efficiency.

```SQL
CREATE TABLE orders_new (
    ts STRING NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY CAST(DATE_PARSE(CAST(ts AS VARCHAR(100)),'%Y%m%d') AS DATE);
```

### Usage notes

Partition pruning is applicable to cases of partitioning based on a complex time function expression:

- If the partition clause is `PARTITION BY from_unixtime(ts)`, queries with filters in the format `ts>1727224687` can be pruned to corresponding partitions.
- If the partition clause is `PARTITION BY CAST(DATE_PARSE(CAST(ts AS VARCHAR(100)),'%Y%m%d') AS DATE)`, queries with filters in the format `ts = "20240506"` can be pruned.
- The above cases are also applicable to [Partitioning based on mixed expression](#partitioning-based-on-the-mixed-expression-since-v34).

## Partitioning based on the mixed expression (since v3.4)

From v3.4.0 onwards, expression partitioning supports multiple partition columns with one of them being a time function expression.

### Examples

Example 1: Suppose you assign a Unix timestamp to each data row and frequently query data by day and a specific city. You can use the timestamp column (with the from_unixtime() function) and the city column as the partition columns at table creation. Data of each day in each city is stored in one partition and partition pruning can be used to improve query efficiency.

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d'), city;
```

## Manage partitions

### Load data into partitions

During data loading, StarRocks will automatically create partitions based on the loaded data and partition rule defined by the partition expression.

Note that if you use expression partitioning at table creation and need to use [INSERT OVERWRITE](../../loading/InsertInto.md#overwrite-data-via-insert-overwrite-select) to overwrite data in a specific partition, whether the partition has been created or not, you currently need to explicitly provide a partition range in `PARTITION()`. This is different from [Range Partitioning](./Data_distribution.md#range-partitioning) or [List Partitioning](./list_partitioning.md), which allow you only to provide the partition name in `PARTITION (<partition_name>)`.

If you use a time function expression at table creation and want to overwrite data in a specific partition, you need to provide the starting date or datetime of that partition (the partition granularity configured at table creation). If the partition does not exist, it can be automatically created during data loading.

```SQL
INSERT OVERWRITE site_access1 PARTITION(event_day='2022-06-08 20:12:04')
    SELECT * FROM site_access2 PARTITION(p20220608);
```

If you use column expression at table creation and want to overwrite data in a specific partition, you need to provide the partition column values that the partition contains. If the partition does not exist, it can be automatically created during data loading.

```SQL
INSERT OVERWRITE t_recharge_detail1 PARTITION(dt='2022-04-02',city='texas')
    SELECT * FROM t_recharge_detail2 PARTITION(p20220402_texas);
```

### View partitions

When you want to view specific information about automatically created partitions, you need to use the `SHOW PARTITIONS FROM <table_name>` statement. The `SHOW CREATE TABLE <table_name>` statement only returns the syntax for expression partitioning that is configured at table creation.

```SQL
MySQL > SHOW PARTITIONS FROM t_recharge_detail1;
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName     | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | List                        | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| 16890       | p20220401_Houston | 2              | 2023-07-19 17:24:53 | 0                  | NORMAL | dt, city     | (('2022-04-01', 'Houston')) | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 2.5KB    | false      | 1        |
| 17056       | p20220402_texas   | 2              | 2023-07-19 17:27:42 | 0                  | NORMAL | dt, city     | (('2022-04-02', 'texas'))   | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 2.5KB    | false      | 1        |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
2 rows in set (0.00 sec)
```

## Limits

- Since v3.1.0, StarRocks's shared-data mode supports the [time function expression](#partitioning-based-on-a-simple-time-function-expression). And since v3.1.1, StarRocks's shared-data mode further supports the [column expression](#partitioning-based-on-the-column-expression-since-v31).
- Currently, using CTAS to create tables configured expression partitioning is not supported.
- Currently, using Spark Load to load data to tables that use expression partitioning is not supported.
- When the `ALTER TABLE <table_name> DROP PARTITION <partition_name>` statement is used to delete a partition created by using the column expression, data in the partition is directly removed and cannot be recovered.
- From v3.4.0, v3.3.8, v3.2.13, and v3.1.16 onwards, StarRocks supports [backing up and restoring](../../administration/management/Backup_and_restore.md) tables created with the expression partitioning strategy.
