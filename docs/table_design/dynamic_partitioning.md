# Dynamic partitioning

StarRocks supports dynamic partitioning, which can automatically manage the time to life (TTL) of partitions, such as partitioning new input data in tables and deleting expired partitions. This feature significantly reduces maintenance costs.

## Enable dynamic partitioning

Take table `site_access` as an example. To enable dynamic partitioning, you need to configure the PROPERTIES parameter. For information about the configuration items, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

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
| dynamic_partition.time_unit             | Yes      | The time granularity for dynamically created partitions. It is a required parameter. Valid values are `HOUR`, `DAY`, `WEEK`, `MONTH`, and `YEAR`. The time granularity determines the suffix format for dynamically created partitions.<ul><li>If the value is `DAY`, the suffix format for dynamically created partitions is yyyyMMdd. An example partition name suffix is `20200321`.</li><li>If the value is `WEEK`, the suffix format for dynamically created partitions is yyyy_ww, for example `2020_13` for the 13th week of 2020.</li><li>If the value is `MONTH`, the suffix format for dynamically created partitions is yyyyMM, for example `202003`.</li><li>If the value is `YEAR`, the suffix format for dynamically created partitions is yyyy, for example `2020`.</li></ul> |
| dynamic_partition.time_zone             | No       | The time zone for dynamic partitions, which is by default the same as the system time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| dynamic_partition.start                 | No       | The starting offset of dynamic partitioning. The value of this parameter must be a negative integer. The partitions before this offset will be deleted based on the current day, week, or month which is determined by the value of the parameter `dynamic_partition.time_unit`. The default value is `Integer.MIN_VALUE`, namely, -2147483648, which means that the history partitions will not be deleted.                                                                                                                                                                                                                                                                                                                                                                  |
| dynamic_partition.end                   | Yes      | The end offset of dynamic partitioning. The value of this parameter must be a positive integer. The partitions from the current day, week, or month to the end offset will be created in advance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| dynamic_partition.prefix                | No       | The prefix added to the names of dynamic partitions. The default value is `p`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.buckets               | No       | The number of buckets per dynamic partition. The default value is the same as the number of buckets determined by the reserved word BUCKETS or automatically set by StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.history_partition_num | No       | The number of historical partitions created by the dynamic partitioning mechanism, with a default value of `0`. When the value is greater than 0, historical partitions are created in advance. From v2.5.2, StarRocks supports this parameter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| dynamic_partition.start_day_of_week     | No       | When `dynamic_partition.time_unit` is `WEEK`, this parameter is used to specify the first day of each week. Valid values: `1` to `7`. `1` means Monday and `7` means Sunday. The default value is `1`, which means that every week starts on Monday.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.start_day_of_month    | No       | When `dynamic_partition.time_unit` is `MONTH`, this parameter is used to specify the first day of each month. Valid values: `1` to `28`. `1` means the 1st of every month and `28` means the 28th of every month. The default value is `1`, which means that every month starts on the 1st. The first day can not be the 29th, 30th, or 31st.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.replication_num       | No       | The number of replicas for tablets in dynamically created partitions. The default value is the same as the number of replicas configured at table creation.  |

**FE configuration:**

`dynamic_partition_check_interval_seconds`: the interval for scheduling dynamic partitioning. The default value is 600s, which means that the partition situation is checked every 10 minutes to see whether the partitions meet the dynamic partitioning conditions specified in `PROPERTIES`. If not, the partitions will be created and deleted automatically.

## View partitions

After you enable dynamic partitions for a table, the input data is continuously and automatically partitioned. You can view the current partitions by using the following statement. For example, if the current date is 2020-03-25, you can only see partitions in the time range from 2020-03-22 to 2020-03-28.

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

## Modify properties of dynamic partitioning

You can use the [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement to modify properties of dynamic partitioning, such as disabling dynamic partitioning. Take the following statement as an example.

```SQL
ALTER TABLE site_access 
SET("dynamic_partition.enable"="false");
```

> Note:
>
> - To check the properties of dynamic partitioning of a table, execute the [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md) statement.
> - You can also use the ALTER TABLE statement to modify other properties of a table.
