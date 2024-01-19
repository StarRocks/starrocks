---
displayed_sidebar: "Chinese"
---

# 自动创建分区

import Replicanum from '../assets/commonMarkdown/replicanum.md'

本文介绍如何创建支持自动建分区的表、相关使用说明和限制。

## 功能介绍

为了提升分区创建的易用性和灵活性，StarRocks 自 3.0 版本起支持分区表达式和导入自动创建分区的功能。您只需要在包含时间函数的分区表达式中，指定一个 DATE 或者 DATETIME 类型的分区列，以及指定分区粒度（年、月、日或小时）。借助这种使用表达式的隐式分区方式，您不需要预先创建出大量分区，StarRocks 会写入新数据时自动创建对应分区。推荐您在创建分区时优先使用自动创建分区。

## 使用方式

### 语法

`PARTITION BY` 子句中包含一个函数表达式，指定了自动创建分区时分区粒度和分区列。

```SQL
PARTITION BY date_trunc(<time_unit>,<partition_column_name>)
...
[PROPERTIES("partition_live_number" = "xxx")];
```

或者

```SQL
PARTITION BY time_slice(<partition_column_name>,INTERVAL N <time_unit>[, boundary]))
...
[PROPERTIES("partition_live_number" = "xxx")];
```

### 参数解释

- 函数：目前仅支持 [date_trunc](../sql-reference/sql-functions/date-time-functions/date_trunc.md) 和 [time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md) 函数。并且如果您使用 `time_slice` 函数，则可以不传入参数 `boundary`，因为在该场景中该参数默认且仅支持为 `floor`，不支持为 `ceil`。
- `time_unit` ：分区粒度，目前仅支持为 `hour`、`day`、`month` 或 `year`，暂时不支持为 `week`。如果分区粒度为 `hour`，则仅支持分区列为 DATETIME 类型，不支持为 DATE 类型。
- `partition_column_name`：分区列。由于分区类型为 RANGE 类型，因此分区列仅支持为 DATE 或 DATETIME 类型，不支持为其它类型。目前仅支持指定一个分区列，不支持指定多个分区列。
  - 如果使用 `date_trunc` 函数，则分区列支持为 DATE 或 DATETIME 类型。如果使用 `time_slice` 函数，则分区列仅支持为 DATETIME 类型。
  - 分区列的值支持为 `NULL`。如果分区列是 DATE 类型，则数值范围为 [0000-01-01 ~ 9999-12-31]。如果分区列是 DATETIME 类型，则数值范围为 [0000-01-01 01:01:01 ~ 9999-12-31 23:59:59]。
- `partition_live_number`：保留最近多少数量的分区。最近是指分区按时间的先后顺序进行排序，以当前的时间为基准，然后从后往前数的分区的个数会保留，其余分区会删除。后台会定时调度任务来管理分区数量，调度间隔可以通过 FE 动态参数 `dynamic_partition_check_interval_seconds` 配置，默认为 600 秒，即 10 分钟。假设当前为 2023 年 4 月 4 日，`partition_live_number` 设置为 `2`，分区包含 p20230401、p20230402、p20230403、p20230404，则分区 p20230403、p20230404 会保留，其他分区会删除。如果导入了脏数据，比如未来时间 4 月 5 日和 6 日的数据，导致分区包含 p20230401、p20230402、p20230403、p20230404、p20230405、p20230406，则分区 p20230403、p20230404、p20230405、p20230406 会保留，其他分区会删除。

## 示例

示例一：使用 date_trunc 函数创建一张支持自动创建分区的表，分区粒度为 `day`，分区列为 `event_day` 。

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

导入如下两行数据，则 StarRocks 会根据导入数据自动创建两个分区 `p20230226`、`p20230227`，范围分别为 [2023-02-26 00:00:00,2023-02-27 00:00:00)、[2023-02-27 00:00:00,2023-02-28 00:00:00)。

```SQL
-- 导入两行数据
INSERT INTO site_access VALUES 
("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- 查询分区
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

示例二：使用 date_trunc 函数创建一张支持自动创建分区的表，分区粒度为 `month`，分区列为 `event_day`，此外在导入数据前批量创建一些历史分区。并且还指定该表只保留最近 3 个分区。

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

示例三：使用 time_slice 函数创建一张支持自动创建分区的表，分区粒度为七天，分区列为 `event_day`。

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

## 使用说明

- StarRocks 会根据导入的数据和建表时设置的自动创建分区规则，来自动创建分区并且设置分区的起止时间。 例如导入数据分区列的值为 `2015-06-05`，分区粒度为 `month`，则创建一个名为 `p201506` 的分区，范围为 [2015-06-01, 2015-07-01)，并不会是 [2015-06-05,2015-07-05)。
- 对于自动创建分区的表，StarRocks 自动创建分区数量上限默认为 4096，由 FE 配置参数 `max_automatic_partition_number` 决定。该参数可以防止您由于误操作而创建大量分区，例如使用了分区列为 DATETIME 类型，设置过细的分区粒度 `hour`，则可能产生大量分区。
- 在导入的过程中 StarRocks 根据导入数据已经自动创建了一些分区，但是由于某些原因导入任务最终失败，则在当前版本中，已经自动创建的分区并不会由于导入失败而自动删除。
- 值得注意的是，PARTITION BY 子句只是用来计算分区范围，不会改变写入数据的值。 例如导入的原始数据是 `2023-02-27 21:06:54`，经过 PARTITION BY 子句的函数表达式 `date_trunc('day', event_day)` 计算后为 `2023-02-27 00:00:00`，则所属分区的范围是 [2023-02-27 00:00:00,2023-02-28 00:00:00)，但是写入的数据仍是 `2023-02-27 21:06:54`，如果希望写入的数据和分区范围的开始时间一致，则需要在导入时在 `event_day` 列上使用用于自动创建分区的函数（例如 `date_trunc`）。
- 导入时自动创建分区的命名规则与动态分区的命名规则一致。

## 使用限制

- 仅支持分区类型为 RANGE 类型，目前不支持为 LIST 类型。
- 如果在建表时启用了自动创建分区，我们通常不建议再提前创建分区，如果您确实有提前创建分区的需求，则仅支持批量创建分区，参考上述示例二的语句。但是该语句存在如下限制：
  - 批量创建分区和自动创建分区的粒度必须一致。
  - 自动创建分区时仅支持使用函数 `date_trunc`，不支持 `time_slice`。
  - 批量创建分区的语法中 `INTERVAL` 仅支持为 `1`。
  - 批量创建的分区开始和结束时间不支持非标准格式。比如批量创建分区的粒度是月，则不支持 `START` 和 `END` 定义非整月范围，比如不支持 `START ("2022-06-06")`，因为其定义的分区开始日期不是 6 月 1 日。
  - 自动创建分区的表创建完成后，您可以使用 `ALTER TABLE ADD PARTITION` 增加分区，但是该语句也存在上述限制。
- StarRocks 存算分离模式暂时不支持该功能。
- 暂时不支持使用 CTAS 创建自动创建分区的表。
- 暂时不支持使用 Spark Load 导入数据至自动创建分区的表。
- 如果使用自动创建分区，则仅支持回滚到 2.5.4 及以后的版本。
- 对于自动创建分区的表，如果需要查看具体的分区情况，请使用 SHOW PARTITIONS FROM 语句。不支持使用 SHOW CREATE TABLE 语句，查看具体的分区情况。
