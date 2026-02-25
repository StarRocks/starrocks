---
displayed_sidebar: docs
sidebar_position: 30
---

# Range 分区（不推荐）

Range 分区适用于存储简单的连续数据，如时间序列数据或连续的数值数据。

基于 Range 分区，您可以使用[动态分区策略](#动态分区)创建分区，该策略允许您管理分区的生命周期（TTL）。

:::note

从 v3.4 开始，[表达式分区](./expression_partitioning.md)方式进一步优化，以统一所有分区策略，并支持更复杂的解决方案。在大多数情况下，建议您使用表达式分区。表达式分区将在未来版本中逐渐取代 Range 分区策略。

:::

## 介绍

**Range 分区适用于基于连续日期/数值范围的频繁查询数据。此外，它还可以应用于一些特殊情况下，例如需要按月分区历史数据，而按天分区最近数据。**

您需要显式定义数据分区列，并建立分区与分区列值范围之间的映射关系。在数据导入期间，StarRocks 会根据数据分区列值所属的范围将数据分配到相应的分区。

关于分区列的数据类型，在 v3.3.0 之前，Range 分区仅支持日期和整数类型的分区列。从 v3.3.0 开始，可以使用三个特定的时间函数作为分区列。在显式定义分区与分区列值范围之间的映射关系时，您需要首先使用特定的时间函数将时间戳或字符串类型的分区列值转换为日期值，然后根据转换后的日期值划分分区。

:::info

- 如果分区列值是时间戳，您需要使用 from_unixtime 或 from_unixtime_ms 函数将时间戳转换为日期值以划分分区。当使用 from_unixtime 函数时，分区列仅支持 INT 和 BIGINT 类型。当使用 from_unixtime_ms 函数时，分区列仅支持 BIGINT 类型。
- 如果分区列值是字符串（STRING、VARCHAR 或 CHAR 类型），您需要使用 str2date 函数将字符串转换为日期值以划分分区。

:::

## 用法

### 语法

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

-- value_range 是一个左闭右开的区间。例如：`[202201, 202212)`。
-- 您必须显式指定半闭区间中的括号 `[`。
value_range ::= 
    [value_list, value_list)

value_list ::=
    ( <value> [, ...] )


multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <int_value> time_unit )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <int_value> ) } -- 即使 START 和 END 指定的分区列值是整数，分区列值仍需用双引号括起来。但 EVERY 子句中的间隔值不需要用双引号括起来。

time_unit ::=
    HOUR | DAY | WEEK | MONTH | YEAR 
```

### 参数

| **参数**              | **描述**                                              |
| --------------------- | ----------------------------------------------------- |
| `partition_columns`   | 分区列的名称。分区列值可以是字符串（不支持 BINARY）、日期或日期时间、或整数。   |
| `function_expression` | 将分区列转换为特定数据类型的函数表达式。支持的函数：from_unixtime, from_unixtime_ms, 和 str2date。<br />**注意**<br />Range 分区仅支持一个函数表达式。 |
| `partition_name`      | 分区名称。建议根据业务场景设置适当的分区名称，以区分不同分区中的数据。 |

### 示例

1. 通过手动定义基于日期类型分区列的值范围创建分区。

   ```SQL
   PARTITION BY RANGE(date_col)(
       PARTITION p1 VALUES LESS THAN ("2020-01-31"),
       PARTITION p2 VALUES LESS THAN ("2020-02-29"),
       PARTITION p3 VALUES LESS THAN ("2020-03-31")
   )
   ```

2. 通过手动定义基于整数类型分区列的值范围创建分区。

   ```SQL
   PARTITION BY RANGE (int_col) (
       PARTITION p1 VALUES LESS THAN ("20200131"),
       PARTITION p2 VALUES LESS THAN ("20200229"),
       PARTITION p3 VALUES LESS THAN ("20200331")
   )
   ```

3. 创建具有相同日期间隔的多个分区。

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
   )
   ```

4. 创建具有不同日期间隔的多个分区。

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
       START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
       START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
   )
   ```

5. 创建具有相同整数间隔的多个分区。

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1)
   )
   ```

6. 创建具有不同整数间隔的多个分区。

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1),
       START ("10") END ("100") EVERY (10)
   )
   ```

7. 使用 from_unixtime 将时间戳（字符串）类型分区列转换为日期类型。

   ```SQL
   PARTITION BY RANGE(from_unixtime(timestamp_col)) (
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

8. 使用 str2date 将字符串类型分区列转换为日期类型。

   ```SQL
   PARTITION BY RANGE(str2date(string_col, '%Y-%m-%d'))(
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

## 动态分区

StarRocks 支持动态分区，可以自动管理分区的生命周期（TTL），例如对表中新输入的数据进行分区和删除过期的分区。此功能显著降低了维护成本。

### 启用动态分区

以表 `site_access` 为例。要启用动态分区，您需要配置 PROPERTIES 参数。有关配置项的信息，请参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

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

| 参数                                   | 是否必需 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|----------------------------------------| -------- |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dynamic_partition.enable               | 否       | 启用动态分区。有效值为 `TRUE` 和 `FALSE`。默认值为 `TRUE`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| dynamic_partition.time_unit            | 是       | 动态创建分区的时间粒度。此参数为必需参数。有效值为 `HOUR`、`DAY`、`WEEK`、`MONTH` 和 `YEAR`。时间粒度决定了动态创建分区的后缀格式。<ul><li>如果值为 `HOUR`，分区列只能是 DATETIME 类型，不能是 DATE 类型。动态创建分区的后缀格式为 yyyyMMddHH，例如，`2020032101`。</li><li>如果值为 `DAY`，动态创建分区的后缀格式为 yyyyMMdd。示例分区名称后缀为 `20200321`。</li><li>如果值为 `WEEK`，动态创建分区的后缀格式为 yyyy_ww，例如 `2020_13` 表示 2020 年的第 13 周。</li><li>如果值为 `MONTH`，动态创建分区的后缀格式为 yyyyMM，例如 `202003`。</li><li>如果值为 `YEAR`，动态创建分区的后缀格式为 yyyy，例如 `2020`。</li></ul> |
| dynamic_partition.time_zone            | 否       | 动态分区的时区，默认与系统时区相同。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| dynamic_partition.start                | 否       | 动态分区的起始偏移量。此参数的值必须为负整数。根据参数 `dynamic_partition.time_unit` 的值确定的当前天、周或月之前的分区将被删除。默认值为 `Integer.MIN_VALUE`，即 -2147483648，表示不会删除历史分区。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| dynamic_partition.end                  | 是       | 动态分区的结束偏移量。此参数的值必须为正整数。从当前天、周或月到结束偏移量的分区将被提前创建。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| dynamic_partition.prefix               | 否       | 添加到动态分区名称的前缀。默认值为 `p`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.buckets              | 否       | 每个动态分区的桶数。默认值与保留字 BUCKETS 确定的桶数或 StarRocks 自动设置的桶数相同。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.history_partition_num| 否       | 动态分区机制创建的历史分区数量，默认值为 `0`。当值大于 0 时，历史分区将被提前创建。从 v2.5.2 开始，StarRocks 支持此参数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| dynamic_partition.start_day_of_week    | 否       | 当 `dynamic_partition.time_unit` 为 `WEEK` 时，此参数用于指定每周的第一天。有效值：`1` 到 `7`。`1` 表示星期一，`7` 表示星期日。默认值为 `1`，表示每周从星期一开始。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.start_day_of_month   | 否       | 当 `dynamic_partition.time_unit` 为 `MONTH` 时，此参数用于指定每月的第一天。有效值：`1` 到 `28`。`1` 表示每月的第一天，`28` 表示每月的第 28 天。默认值为 `1`，表示每月从第 1 天开始。第一天不能是 29 日、30 日或 31 日。                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.replication_num      | 否       | 动态创建分区中 tablet 的副本数量。默认值与表创建时配置的副本数量相同。  |

:::note

当分区列为 INT 类型时，其格式必须为 `yyyyMMdd`，无论分区时间粒度如何。

:::

**FE 配置：**

`dynamic_partition_check_interval_seconds`：调度动态分区的间隔。默认值为 600 秒，这意味着每 10 分钟检查一次分区情况，以查看分区是否符合 `PROPERTIES` 中指定的动态分区条件。如果不符合，将自动创建和删除分区。

### 查看动态分区

在为表启用动态分区后，输入数据会被持续自动分区。您可以使用以下语句查看当前分区。例如，如果当前日期是 2020-03-25，您只能看到时间范围从 2020-03-25 到 2020-03-28 的分区。

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

如果您希望在创建表时创建历史分区，您需要指定 `dynamic_partition.history_partition_num` 来定义要创建的历史分区数量。例如，如果您在创建表时将 `dynamic_partition.history_partition_num` 设置为 `3`，并且当前日期是 2020-03-25，您将只能看到时间范围从 2020-03-22 到 2020-03-28 的分区。

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

### 修改动态分区的属性

您可以使用 [ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 语句修改动态分区的属性，例如禁用动态分区。以下语句为示例。

```SQL
ALTER TABLE site_access 
SET("dynamic_partition.enable"="false");
```

:::note
- 要检查表的动态分区属性，请执行 [SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) 语句。
- 您还可以使用 ALTER TABLE 语句修改表的其他属性。
:::