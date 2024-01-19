---
displayed_sidebar: "English"
---

# Automatic partitioning

import Replicanum from '../assets/commonMarkdown/replicanum.md'

This topic describes how to create a table that supports automatic partitioning. This topic also describes the usage notes and limits of automatic partitioning.

## Introduction

To make partitions creation more easy to use and flexible, StarRocks supports the partitioning expression and automatic partitioning since version 3.0. You only need to specify a partition column of the DATE or DATETIME data type and a partition granularity (year, month, day, or hour) in the partition expression, which includes a time function. With this implicit partitioning method implemented by using a expression, you do not need to create a large number of partitions in advance. StarRocks automatically creates partitions when new data is written. It is recommended that you prioritize using automatic partitioning.

## Enable automatic partitioning

### Syntax

The PARTITION BY clause contains a function expression that specifies the partition granularity and partition column for automatic partitioning.

```SQL
PARTITION BY date_trunc(<time_unit>,<partition_column_name>)
...
[PROPERTIES("partition_live_number" = "xxx")];
```

Or

```SQL
PARTITION BY time_slice(<partition_column_name>,INTERVAL N <time_unit>[, boundary]))
...
[PROPERTIES("partition_live_number" = "xxx")];
```

### Parameters

- Functions: Currently, only the [date_trunc](../sql-reference/sql-functions/date-time-functions/date_trunc.md) and [time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md) functions are supported. If you use the function `time_slice`, you do not need to pass the `boundary` parameter. It is because in this scenario, the default and valid value for this parameter is `floor`, and the value can not be `ceil`.
- `time_unit`: the partition granularity, which can be `hour`, `day`, `month` or `year`. The `week` partition granularity is not supported. If the partition granularity is `hour`, the partition column must be of the DATETIME data type and cannot be of the DATE data type.
- `partition_column_name`: the name of the partition column. The partition type is RANGE, and therefore the partition column can only be of the DATE or DATETIME data type. Currently, you can specify only one partition column and multiple partition columns are not supported. If the `date_trunc` function is used, the partition column can be of the DATE or DATETIME data type. If the `time_slice` function is used, the partition column must be of the DATETIME data type. The partition column allows `NULL` values. If the partition column is of the DATE data type, the supported range is [0000-01-01 ~ 9999-12-31]. If the partition column is of the DATETIME data type, the supported range is [0000-01-01 01:01:01 ~ 9999-12-31 23:59:59].
- `partition_live_number`: the number of the most recent partitions to be retained. "Recent" refers to that the partitions are sorted in chronological order, with the current date as a benchmark, the number of partitions that counted backwards are kept, and the rest of the partitions are deleted. StarRocks schedules tasks to manage the number of partitions, and the scheduling interval can be configured through the FE dynamic parameter `dynamic_partition_check_interval_seconds`, which defaults to 600 seconds (10 minutes). Suppose that the current date is April 4, 2023, `partition_live_number` is set to `2`, and the partitions include p20230401, p20230402, p20230403, p20230404. The partitions p20230403, p20230404 are retained and other partitions are deleted. If dirty data is loaded, such as data from the future dates April 5 and April 6, partitions include p20230401, p20230402, p20230403, p20230404, p20230405, and p20230406. Then partitions p20230403, p20230404, p20230405, p20230406 are retained and the other partitions are deleted.

## Examples

Example 1: Use the `date_trunc` function to create a table that supports automatic partitioning. Set the partition granularity to `day` and the partition column to `event_day`.

```SQL
CREATE TABLE site_access (
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

<Replicanum />

When the following two data rows are inserted, StarRocks automatically creates two partitions, `p20230226` and `p20230227`, whose ranges are [2023-02-26 00:00:00, 2023-02-27 00:00:00) and [2023-02-27 00:00:00, 2023-02-28 00:00:00) respectively.

```SQL
-- insert two data rows
INSERT INTO site_access VALUES 
("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- view partitions
SHOW PARTITIONS FROM site_access\G
*************************** 1. row ***************************
             PartitionId: 135846228
           PartitionName: p20230226
          VisibleVersion: 2
      VisibleVersionTime: 2023-03-22 14:50:17
      VisibleVersionHash: 0
                   State: NORMAL
            PartitionKey: event_day
                   Range: [types: [DATETIME]; keys: [2023-02-26 00:00:00]; ..types: [DATETIME]; keys: [2023-02-27 00:00:00]; )
         DistributionKey: event_day, site_id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 0B
              IsInMemory: false
                RowCount: 0
*************************** 2. row ***************************
             PartitionId: 135846215
           PartitionName: p20230227
          VisibleVersion: 2
      VisibleVersionTime: 2023-03-22 14:50:17
      VisibleVersionHash: 0
                   State: NORMAL
            PartitionKey: event_day
                   Range: [types: [DATETIME]; keys: [2023-02-27 00:00:00]; ..types: [DATETIME]; keys: [2023-02-28 00:00:00]; )
         DistributionKey: event_day, site_id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 0B
              IsInMemory: false
                RowCount: 0
2 rows in set (0.00 sec)
```

Example 2: Use the `date_trunc` function to create a table that supports automatic partitioning. Set the partition granularity to `month` and the partition column to `event_day`. Additionally, create some historical partitions before loading data and specify that the table only retains the most recent three partitions.

```SQL
CREATE TABLE site_access(
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
) 
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('month', event_day)(
    START ("2022-06-01") END ("2022-12-01") EVERY (INTERVAL 1 month)
)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES("partition_live_number" = "3");
```

<Replicanum />

Example 3: Use the `time_slice` function to create a table that supports automatic partitioning. Set the partition granularity to 7 days and the partition column to `event_day`.

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
DISTRIBUTED BY HASH(event_day, site_id);
```

<Replicanum />

## Usage notes

- StarRocks automatically creates partitions and sets the start time and end time of the partitions based on the loaded data and the automatic partitioning rule configured at table creation. For example, if the value of the partition column for the data row is `2015-06-05`, and the partition granularity is `month`, then a partition named `p201506` is created with a range of [2015-06-01, 2015-07-01) rather than [2015-06-05, 2015-07-05).
- For tables that support automatic partitioning, StarRocks sets the default maximum number of automatically created partitions to 4096, which can be configured by the FE parameter `max_automatic_partition_number`. This parameter can prevent you from accidentally creating too many partitions, such as when specifying a partition column of DATETIME type with a too-fine partition granularity like `hour`, which can generate a large number of partitions.
- During data loading, StarRocks automatically creates some partitions based on the loaded data, but if the load job fails for some reason, the partitions that are automatically created by StarRocks cannot be automatically deleted.
- Note that the `PARTITION BY` clause is only used to calculate the partition range for the loaded data and does not change the values of the data. For example, if the original data is `2023-02-27 21:06:54`, the function expression in `PARTITION BY date_trunc('day', event_day)` computes it as 2023-02-27 00:00:00 and infers that it belongs to the partition range [2023-02-27 00:00:00, 2023-02-28 00:00:00), but the data is still written as `2023-02-27 21:06:54`. If you want the written data's value to be the same as the start time of the partition range, you need to use the function specified in the `PARTITION BY` clause, such as `date_trunc`, on the `event_day` column when creating a load job.
- The naming rules for automatic partitioning are consistent with the naming rules for dynamic partitioning.

## Limits

- Only the range partitioning type is supported, whereas the list partitioning type is not supported.
- When a table that supports automatic partitioning is created, it is generally not recommended to create partitions in advance. If you need to create partitions in advance, you can create multiple partitions all at a time, as shown in the preceding Example 2. The statement in Example 2 has the following limits:
  - The granularity of the partitions created in advance must be consistent with that of the automatically created partitions.
  - When you configure automatic partitioning, you can only use the function `date_trunc` rather than `time_slice`.
  - The syntax for creating multiple partitions all at a time only supports an interval of `1`.
  - After a table that supports automatic partitioning is created, you can use `ALTER TABLE ADD PARTITION` to add partitions for that table. And the statement `ALTER TABLE ADD PARTITION` also has the above limits.
- Currently, StarRocks's shared-data mode does not support this feature.
- Currently, using CTAS to create a table that supports automatic partitioning is not supported.
- Currently, using Spark Load to load data to tables that support automatic partitioning is not supported.
- If you use automatic partitioning, you can only roll back your StarRocks cluster to version 2.5.4 or later.
- To view the specific information of automatically created partitions, use the SHOW PARTITIONS FROM statement, rather than the SHOW CREATE TABLE statement.
