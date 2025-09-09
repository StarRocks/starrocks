---
displayed_sidebar: docs
sidebar_position: 30
---

# Range partitioning (Legacy)

Range partitioning is suitable for storing simple contiguous data, such as time series data, or continuous numerical data.

Based on range partitioning, you can create partitions using the [dynamic partitioning strategy](#dynamic-partitioning), which allows you to manage the Time-to-live (TTL) of partitions.

:::note

Please note that from v3.4 onwards, [expression partitioning](./expression_partitioning.md) is further optimized to unify all partitioning strategies and supported more complex solutions. It is recommended in most cases, and will replace the range partitioning strategy in future releases.

:::

## Introduction

**Range partitioning is appropriate for frequently queried data based on continuous date/numerical ranges. Additionally, it can be applied in some special cases where historical data needs to be partitioned by month, and recent data needs to be partitioned by day.**

You need to explicitly define the data partitioning columns and establish the mapping relationship between partitions and ranges of partitioning column values. During data loading, StarRocks assigns the data to the corresponding partitions based on the ranges to which the data partitioning column values belong.

As for the data type of the partitioning columns, before v3.3.0, range partitioning only supports partitioning columns of date and integer types. Since v3.3.0, three specific time functions can be used as partition columns. When explicitly defining the mapping relationship between partitions and ranges of partitioning column values, you need to first use a specific time function to convert partitioning column values of timestamps or strings into date values, and then divide the partitions based on the converted date values.

:::info

- If the partitioning column value is a timestamp, you need to use the from_unixtime or from_unixtime_ms function to convert a timestamp to a date value when dividing the partitions. When the from_unixtime function is used, the partitioning column only supports INT and BIGINT types. When the from_unixtime_ms function is used, the partitioning column only supports BIGINT type.
- If the partitioning column value is a string (STRING, VARCHAR, or CHAR type), you need to use the str2date function to convert the string to a date value when dividing the partitions.

:::

## Usage

### Syntax

```sql
PARTITION BY RANGE ( partition_columns | function_expression ) ( single_range_partition | multi_range_partitions )

partition_columns ::= 
    <column> [, ...]

function_expression ::= 
      from_unixtime(column) 
    | from_unixtime_ms(column) 
    | str2date(column) 

single_range_partition ::=
    PARTITION <partition_name> VALUES partition_key_desc

partition_key_desc ::=
          LESS THAN { MAXVALUE | value_list }
        | value_range

-- value_range is a left-closed, right-open interval. Example: `[202201, 202212)`.
-- You must explicitly specify the bracket `[` in the half-closed interval.
value_range ::= 
    [value_list, value_list)

value_list ::=
    ( <value> [, ...] )


multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <int_value> time_unit )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <int_value> ) } -- The partition column values still need to be enclosed in double quotes even if the partition column values specified by START and END are integers. However, the interval values in the EVERY clause do not need to be enclosed in double quotes.

time_unit ::=
    HOUR | DAY | WEEK | MONTH | YEAR 
```

### Parameters

| **Parameters**        | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| `partition_columns`   | The names of the partitioning columns. The partitioning column values can be string (BINARY not supported), date or datetime, or integer.   |
| `function_expression` | The function expression that transforms the partitioning column into specific data types. Supported functions: from_unixtime, from_unixtime_ms, and str2date.<br />**NOTE**<br />Range partitioning supports one and only one function expression. |
| `partition_name`      | Partition name. It is recommended to set appropriate partition names based on the business scenario to differentiate the data in different partitions. |

### Example

1. Create partitions by manually defining value ranges based on date type partitioing column.

   ```SQL
   PARTITION BY RANGE(date_col)(
       PARTITION p1 VALUES LESS THAN ("2020-01-31"),
       PARTITION p2 VALUES LESS THAN ("2020-02-29"),
       PARTITION p3 VALUES LESS THAN ("2020-03-31")
   )
   ```

2. Create partitions by manually defining value ranges based on integer type partitioing column.

   ```SQL
   PARTITION BY RANGE (int_col) (
       PARTITION p1 VALUES LESS THAN ("20200131"),
       PARTITION p2 VALUES LESS THAN ("20200229"),
       PARTITION p3 VALUES LESS THAN ("20200331")
   )
   ```

3. Create multiple partition with the same date interval.

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
   )
   ```

4. Create multiple partition with different date intervals.

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
       START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
       START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
   )
   ```

5. Create multiple partition with the same integer interval.

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1)
   )
   ```

6. Create multiple partition with different integer intervals.

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1),
       START ("10") END ("100") EVERY (10)
   )
   ```

7. Use from_unixtime to transform timestamp (string) type partition column into date type.

   ```SQL
   PARTITION BY RANGE(from_unixtime(timestamp_col)) (
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

8. Use str2date to transform string type partition column into date type.

   ```SQL
   PARTITION BY RANGE(str2date(string_col, '%Y-%m-%d'))(
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

## Dynamic partitioning

StarRocks supports dynamic partitioning, which can automatically manage the time to life (TTL) of partitions, such as partitioning new input data in tables and deleting expired partitions. This feature significantly reduces maintenance costs.

### Enable dynamic partitioning

Take table `site_access` as an example. To enable dynamic partitioning, you need to configure the PROPERTIES parameter. For information about the configuration items, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
    PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
    PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
    PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
    PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.history_partition_num" = "0"
);
```

**`PROPERTIES`**:

| parameter                               | required | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------------| -------- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dynamic_partition.enable                | No       | Enables dynamic partitioning. Valid values are `TRUE` and `FALSE`. The default value is `TRUE`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| dynamic_partition.time_unit             | Yes      | The time granularity for dynamically created partitions. It is a required parameter. Valid values are `HOUR`, `DAY`, `WEEK`, `MONTH`, and `YEAR`. The time granularity determines the suffix format for dynamically created partitions.<ul><li>If the value is `HOUR`, the partitioning column can only be of DATETIME type, and can not be of DATE type. The suffix format for dynamically created partitions is yyyyMMddHH, for example, `2020032101`.</li><li>If the value is `DAY`, the suffix format for dynamically created partitions is yyyyMMdd. An example partition name suffix is `20200321`.</li><li>If the value is `WEEK`, the suffix format for dynamically created partitions is yyyy_ww, for example `2020_13` for the 13th week of 2020.</li><li>If the value is `MONTH`, the suffix format for dynamically created partitions is yyyyMM, for example `202003`.</li><li>If the value is `YEAR`, the suffix format for dynamically created partitions is yyyy, for example `2020`.</li></ul> |
| dynamic_partition.time_zone             | No       | The time zone for dynamic partitions, which is by default the same as the system time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| dynamic_partition.start                 | No       | The starting offset of dynamic partitioning. The value of this parameter must be a negative integer. The partitions before this offset will be deleted based on the current day, week, or month which is determined by the value of the parameter `dynamic_partition.time_unit`. The default value is `Integer.MIN_VALUE`, namely, -2147483648, which means that the history partitions will not be deleted.                                                                                                                                                                                                                                                                                                                                                                  |
| dynamic_partition.end                   | Yes      | The end offset of dynamic partitioning. The value of this parameter must be a positive integer. The partitions from the current day, week, or month to the end offset will be created in advance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| dynamic_partition.prefix                | No       | The prefix added to the names of dynamic partitions. The default value is `p`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.buckets               | No       | The number of buckets per dynamic partition. The default value is the same as the number of buckets determined by the reserved word BUCKETS or automatically set by StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.history_partition_num | No       | The number of historical partitions created by the dynamic partitioning mechanism, with a default value of `0`. When the value is greater than 0, historical partitions are created in advance. From v2.5.2, StarRocks supports this parameter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| dynamic_partition.start_day_of_week     | No       | When `dynamic_partition.time_unit` is `WEEK`, this parameter is used to specify the first day of each week. Valid values: `1` to `7`. `1` means Monday and `7` means Sunday. The default value is `1`, which means that every week starts on Monday.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.start_day_of_month    | No       | When `dynamic_partition.time_unit` is `MONTH`, this parameter is used to specify the first day of each month. Valid values: `1` to `28`. `1` means the 1st of every month and `28` means the 28th of every month. The default value is `1`, which means that every month starts on the 1st. The first day can not be the 29th, 30th, or 31st.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.replication_num       | No       | The number of replicas for tablets in dynamically created partitions. The default value is the same as the number of replicas configured at table creation.  |

:::note

When the partition column is the INT type, its format must be `yyyyMMdd`, regardless of the partition time granularity.

:::

**FE configuration:**

`dynamic_partition_check_interval_seconds`: the interval for scheduling dynamic partitioning. The default value is 600s, which means that the partition situation is checked every 10 minutes to see whether the partitions meet the dynamic partitioning conditions specified in `PROPERTIES`. If not, the partitions will be created and deleted automatically.

### View dynamic partitions

After you enable dynamic partitions for a table, the input data is continuously and automatically partitioned. You can view the current partitions by using the following statement. For example, if the current date is 2020-03-25, you can only see partitions in the time range from 2020-03-25 to 2020-03-28.

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

If you want to create historical partitions when creating a table, you need to specify `dynamic_partition.history_partition_num` to define the number of historical partitions to be created. For example, if you set `dynamic_partition.history_partition_num` to `3` during table creation and the current date is 2020-03-25, you will only see partitions in the time range from 2020-03-22 to 2020-03-28.

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

### Modify properties of dynamic partitioning

You can use the [ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) statement to modify properties of dynamic partitioning, such as disabling dynamic partitioning. Take the following statement as an example.

```SQL
ALTER TABLE site_access 
SET("dynamic_partition.enable"="false");
```

:::note
- To check the properties of dynamic partitioning of a table, execute the [SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) statement.
- You can also use the ALTER TABLE statement to modify other properties of a table.
:::
