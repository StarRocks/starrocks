---
displayed_sidebar: docs
description: Partition data in StarRocks
sidebar_position: 10
---

# 表达式分区（推荐）

自 v3.0 起，StarRocks 支持表达式分区（此前称为自动分区），它更灵活、更易用。这种分区方法适用于大多数场景，例如基于连续时间范围或 ENUM 值查询和管理数据。

您只需在建表时指定一个简单的分区表达式。在数据加载期间，StarRocks 将根据数据和分区表达式中定义的规则自动创建分区。您不再需要在建表时手动创建大量分区，也不需要配置动态分区属性。

从 v3.4 起，表达式分区得到进一步优化，统一了所有分区策略并支持更复杂的解决方案。在大多数情况下推荐使用，并将在未来版本中取代其他分区策略。

从 v3.5 起，StarRocks 支持基于时间函数合并表达式分区，以优化存储效率和查询性能。有关详细信息，请参阅 [合并表达式分区](#merge-expression-partitions)。

## 基于简单时间函数表达式的分区

如果您经常基于连续时间范围查询和管理数据，您只需指定一个日期类型（`DATE` 或 `DATETIME`）列作为分区列，并在时间函数表达式中指定年、月、日或小时作为分区粒度。StarRocks 将根据加载的数据和分区表达式自动创建分区，并设置分区的开始和结束日期或日期时间。

但是，在某些特殊场景下，例如将历史数据按月分区，将近期数据按天分区，您必须使用 [范围分区](./Data_distribution.md#range-partitioning) 来创建分区。

:::note
`PARTITION BY date_trunc(column)` 和 `PARTITION BY time_slice(column)` 尽管是表达式分区的格式，但被视为范围分区。因此，您可以使用 `ALTER TABLE ... ADD PARTITION` 语句为使用此类分区策略的表添加新分区。
:::

### 语法

```sql
PARTITION BY expression
...
[ PROPERTIES( { 'partition_live_number' = 'xxx' | 'partition_retention_condition' = 'expr' } ) ]

expression ::=
    { date_trunc ( <time_unit> , <partition_column> ) |
      time_slice ( <partition_column> , INTERVAL <N> <time_unit> [ , boundary ] ) }
```

### 参数

#### `expression`

**必填**：是\
**说明**：一个简单的时间函数表达式，使用 [`date_trunc`](../../sql-reference/sql-functions/date-time-functions/date_trunc.md) 或 [`time_slice`](../../sql-reference/sql-functions/date-time-functions/time_slice.md) 函数。如果您使用 `time_slice` 函数，则无需传递 `boundary` 参数。因为在这种情况下，此参数的默认有效值为 `floor`，且该值不能为 `ceil`。

#### `time_unit`

**必填**：是\
**说明**：分区粒度，可以是 `hour`、`day`、`week`、`month` 或 `year`。如果分区粒度是 `hour`，则分区列必须是 `DATETIME` 数据类型，不能是 `DATE` 数据类型。

#### `partition_column`

**必填**：是\
**说明**：分区列的名称。

- 分区列只能是 `DATE` 或 `DATETIME` 数据类型。分区列允许 `NULL` 值。
- 如果使用 `date_trunc` 函数，分区列可以是 `DATE` 或 `DATETIME` 数据类型。如果使用 `time_slice` 函数，分区列必须是 `DATETIME` 数据类型。
- 如果分区列是 `DATE` 数据类型，支持的范围是 `[0000-01-01 ~ 9999-12-31]`。如果分区列是 `DATETIME` 数据类型，支持的范围是 `[0000-01-01 01:01:01 ~ 9999-12-31 23:59:59]`
- 目前，您只能指定一个分区列；不支持多个分区列。

#### `partition_live_number`

**必填**：否\
**说明**：要保留的最新分区的数量。分区按时间顺序排序，**以当前日期为基准**；早于当前日期减去 `partition_live_number` 的分区将被删除。StarRocks 会调度任务来管理分区数量，调度间隔可以通过 FE 动态参数 `dynamic_partition_check_interval_seconds` 进行配置，默认为 600 秒（10 分钟）。假设当前日期是 2023 年 4 月 4 日，`partition_live_number` 设置为 `2`，并且分区包括 `p20230401`、`p20230402`、`p20230403`、`p20230404`。分区 `p20230403` 和 `p20230404` 将被保留，其他分区将被删除。如果加载了脏数据，例如来自未来日期 4 月 5 日和 4 月 6 日的数据，分区包括 `p20230401`、`p20230402`、`p20230403`、`p20230404` 和 `p20230405`，以及 `p20230406`。那么分区 `p20230403`、`p20230404`、`p20230405` 和 `p20230406` 将被保留，其他分区将被删除。

#### `partition_retention_condition`

从 v3.5.0 起，StarRocks 原生表支持通用分区表达式 TTL。

`partition_retention_condition`：声明要动态保留的分区的表达式。不满足表达式中条件的分区将定期被删除。示例：`'partition_retention_condition' = 'dt >= CURRENT_DATE() - INTERVAL 3 MONTH'`。

- 表达式只能包含分区列和常量。不支持非分区列。
- 通用分区表达式对列表分区和范围分区的应用方式不同：
  - 对于列表分区表，StarRocks 支持删除通过通用分区表达式过滤的分区。
  - 对于范围分区表，StarRocks 只能通过 FE 的分区裁剪能力来过滤和删除分区。与分区裁剪不支持的谓词对应的分区无法被过滤和删除。

### 使用说明

- 在数据加载过程中，StarRocks 会根据加载的数据自动创建一些分区，但如果加载作业因某种原因失败，StarRocks 自动创建的分区无法自动删除。
- StarRocks 将单次加载自动创建分区的默认最大数量设置为 4096，这可以通过 FE 参数 `auto_partition_max_creation_number_per_load` 进行配置。此参数可以防止您意外创建过多分区。
- 分区的命名规则与动态分区的命名规则一致。

### 示例

示例 1：假设您经常按天查询数据。您可以在建表时使用分区表达式 `date_trunc()`，并将分区列设置为 `event_day`，分区粒度设置为 `day`。数据在加载时会根据日期自动分区。同一天的数据存储在一个分区中，并且可以使用分区裁剪显著提高查询效率。

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

例如，当加载以下两行数据时，StarRocks 将自动创建两个分区 `p20230226` 和 `p20230227`，其范围分别为 [2023-02-26 00:00:00, 2023-02-27 00:00:00) 和 [2023-02-27 00:00:00, 2023-02-28 00:00:00)。如果后续加载的数据落入这些范围，它们将自动路由到相应的分区。

```SQL
-- 插入两行数据
INSERT INTO site_access1  
VALUES ("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- 查看分区
mysql > SHOW PARTITIONS FROM site_access1;
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range                                                                                                | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | StorageSize | IsInMemory | RowCount | DataVersion | VersionEpoch       | VersionTxnType | TabletBalanced |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| 17138       | p20230226     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-26 00:00:00]; ..types: [DATETIME]; keys: [2023-02-27 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409742105974407168 | TXN_NORMAL     | true           |
| 17113       | p20230227     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-27 00:00:00]; ..types: [DATETIME]; keys: [2023-02-28 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409742105974407169 | TXN_NORMAL     | true           |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
2 rows in set (0.00 sec)
```

示例 2：如果您想实现分区生命周期管理，即只保留一定数量的最新分区并删除历史分区，您可以使用 `partition_live_number` 属性来指定要保留的分区数量。

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

示例 3：假设您经常按周查询数据。您可以在建表时使用分区表达式 `time_slice()`，并将分区列设置为 `event_day`，分区粒度设置为一周。一周的数据存储在一个分区中，并且可以使用分区裁剪显著提高查询效率。

```SQL
CREATE TABLE site_access(
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY time_slice(event_day, INTERVAL 1 week)
DISTRIBUTED BY HASH(event_day, site_id);
```

## 基于列表达式的分区（自 v3.1 起）

如果您经常查询和管理特定类型的数据，您只需将表示该类型的列指定为分区列。StarRocks 将根据加载数据的分区列值自动创建分区。

但是，在某些特殊场景下，例如当表包含列 `city`，并且您经常根据国家和城市查询和管理数据时。您必须使用[列表分区](./list_partitioning.md)将同一国家/地区的多个城市的数据存储在一个分区中。

### 语法

```sql
PARTITION BY expression
...

expression ::=
    partition_columns 
    
partition_columns ::=
    <column>, [ <column> [,...] ]
```

### 参数

#### `partition_columns`

**必填**：是\
**说明**：分区列的名称。

- 分区列值可以是字符串（不支持 `BINARY`）、`date` 或 `datetime`、`integer` 和 `boolean` 值。分区列允许 `NULL` 值。
- 每个分区只能包含分区列中具有相同值的数据。要在分区中包含分区列中具有不同值的数据，请参阅[列表分区](./list_partitioning.md)。

:::note

从 v3.4 起，您可以省略用于包裹分区列的括号。例如，您可以将 `PARTITION BY (dt,city)` 替换为 `PARTITION BY dt,city`。

:::

### 使用说明

- 在数据加载过程中，StarRocks 会根据加载的数据自动创建一些分区，但如果加载作业因某种原因失败，StarRocks 自动创建的分区无法自动删除。
- StarRocks 将单次加载自动创建分区的默认最大数量设置为 4096，这可以通过 FE 参数 `auto_partition_max_creation_number_per_load` 进行配置。此参数可以防止您意外创建过多分区。
- 分区的命名规则：如果指定了多个分区列，不同分区列的值在分区名称中用下划线 `_` 连接，格式为 `p<value in partition column 1>_<value in partition column 2>_...`。例如，如果指定 `dt` 和 `province` 两个列作为分区列，两者都是字符串类型，并且加载了一行值为 `2022-04-01` 和 `beijing` 的数据，则自动创建的相应分区名为 `p20220401_beijing`。

### 示例

示例 1：假设您经常根据时间范围和特定城市查询数据中心计费的详细信息。在建表时，您可以使用分区表达式将第一个分区列指定为 `dt` 和 `city`。这样，属于相同日期和城市的数据将被路由到同一个分区中，并且可以使用分区裁剪显著提高查询效率。

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

向表中插入一行数据。

```SQL
INSERT INTO t_recharge_detail1 
VALUES (1, 1, 1, 'Houston', '2022-04-01');
```

查看分区。结果显示 StarRocks 根据加载的数据自动创建了一个分区 `p20220401_Houston`。在后续加载过程中，分区列 `dt` 和 `city` 中值为 `2022-04-01` 和 `Houston` 的数据将存储在此分区中。

:::tip
每个分区只能包含分区列具有指定单个值的数据。要在分区中为分区列指定多个值，请参阅[列表分区](./list_partitioning.md)。
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
                    List: [["2022-04-01","Houston"]]
         DistributionKey: id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 2.5KB
             StorageSize: 2.5KB
              IsInMemory: false
                RowCount: 1
             DataVersion: 2
            VersionEpoch: 409742188174376960
          VersionTxnType: TXN_NORMAL
          TabletBalanced: true
1 row in set (0.00 sec)
```

## 基于复杂时间函数表达式的分区（v3.4 起）

从 v3.4.0 起，表达式分区支持返回 `DATE` 或 `DATETIME` 类型值的任何表达式，以适应更复杂的场景。有关支持的时间函数，请参阅[附录 - 支持的时间函数](#supported-time-functions)。

例如，您可以定义一个 Unix 时间戳列，并在分区表达式中直接对该列使用 `from_unixtime()` 来定义分区键，而不是定义一个带有函数的生成 `DATE` 或 `DATETIME` 列。有关更多用法，请参阅[示例](#examples-2)。

从 v3.4.4 起，基于大多数 `DATETIME` 相关函数的分区支持分区裁剪。

### 示例

示例 1：假设您为每行数据分配一个 Unix 时间戳，并经常按天查询数据。您可以在创建表时，在表达式中使用 `from_unixtime()` 函数将时间戳定义为分区列，并将分区粒度设置为天。每天的数据存储在一个分区中，并且可以使用分区裁剪来提高查询效率。

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d');
```

示例 2：假设您为每行数据分配一个 `INT` 类型的时间戳，并按月存储数据。您可以在创建表时，在表达式中使用 `cast()` 和 `str_to_date()` 函数将时间戳转换为 `DATE` 类型，将其设置为分区列，并使用 `date_trunc()` 将分区粒度设置为月。每个月的数据存储在一个分区中，并且可以使用分区裁剪来提高查询效率。

```SQL
CREATE TABLE orders_new (
    ts INT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY date_trunc('month', str_to_date(CAST(ts as STRING),'%Y%m%d'));
```

### 使用说明

分区裁剪适用于基于复杂时间函数表达式的分区情况：

- 如果分区子句是 `PARTITION BY from_unixtime(ts)`，则格式为 `ts>1727224687` 的带过滤条件的查询可以裁剪到相应的分区。
- 如果分区子句是 `PARTITION BY str2date(CAST(ts AS string),'%Y%m')`，则格式为 `ts = "20240506"` 的带过滤条件的查询可以裁剪。
- 上述情况也适用于[基于混合表达式的分区](#partitioning-based-on-the-mixed-expression-since-v34)。

## 基于混合表达式的分区（v3.4 起）

从 v3.4.0 起，表达式分区支持多个分区列，其中一个可以是时间函数表达式。

### 示例

示例 1：假设您为每行数据分配一个 Unix 时间戳，并经常按天和特定城市查询数据。您可以在创建表时，使用时间戳列（带 `from_unixtime()` 函数）和城市列作为分区列。每个城市每天的数据存储在一个分区中，并且可以使用分区裁剪来提高查询效率。

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d'), city;
```

## 管理分区

### 将数据加载到分区

在数据加载期间，StarRocks 会根据加载的数据和分区表达式定义的分区规则自动创建分区。

使用表达式分区时，您可以使用[INSERT OVERWRITE](../../loading/InsertInto.md#overwrite-data-via-insert-overwrite-select)动态覆盖数据，而无需指定分区名称。StarRocks 将自动路由并覆盖相应分区中的数据。

如果您想覆盖*特定*分区中的数据，您可以在 `PARTITION()` 中明确提供分区范围。请注意，对于表达式分区，您需要提供该分区的起始日期或日期时间（在表创建时配置的分区粒度）或特定的列值，而不仅仅是分区名称。如果分区不存在，它将在数据加载期间自动创建。

```SQL
INSERT OVERWRITE site_access1 PARTITION(event_day='2022-06-08 20:12:04')
SELECT * FROM site_access2 PARTITION(p20220608);
```

如果您在创建表时使用列表达式并想覆盖特定分区中的数据，您需要提供该分区包含的分区列值。如果分区不存在，它可以在数据加载期间自动创建。

```SQL
INSERT OVERWRITE t_recharge_detail1 PARTITION(dt='2022-04-02',city='texas')
SELECT * FROM t_recharge_detail2 PARTITION(p20220402_texas);
```

### 查看分区

当您想查看自动创建的分区的具体信息时，您需要使用 `SHOW PARTITIONS FROM <table_name>` 语句。`SHOW CREATE TABLE <table_name>` 语句仅返回在表创建时配置的表达式分区的语法。

```SQL
MySQL > SHOW PARTITIONS FROM t_recharge_detail1;
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| PartitionId | PartitionName     | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | List                       | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | StorageSize | IsInMemory | RowCount | DataVersion | VersionEpoch       | VersionTxnType | TabletBalanced |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| 11099       | p20220401_Houston | 2              | 2026-03-11 13:59:51 | 0                  | NORMAL | dt, city     | [["2022-04-01","Houston"]] | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409743636180238336 | TXN_NORMAL     | true           |
| 11116       | p20220402_texas   | 2              | 2026-03-11 13:59:52 | 0                  | NORMAL | dt, city     | [["2022-04-02","texas"]]   | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409743639174971392 | TXN_NORMAL     | true           |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
2 rows in set (0.01 sec)
```

### 合并表达式分区

在数据管理中，基于不同时间粒度的分区对于优化查询和存储至关重要。为了提高存储效率和查询性能，StarRocks 支持将多个更细时间粒度的表达式分区合并为一个更粗时间粒度的分区，例如，将按天分区合并为按月分区。通过合并符合指定条件（时间范围）的分区，StarRocks 允许您以不同的时间粒度对数据进行分区。

#### 语法

```SQL
ALTER TABLE [<db_name>.]<table_name>
PARTITION BY <time_expr>
[WHERE <time_range_column>] BETWEEN <start_time> AND <end_time>
```

#### 参数

##### `PARTITION BY <time_expr>`

**必填**: 是\
**说明**: 指定分区策略的新时间粒度，例如 `PARTITION BY date_trunc('month', dt)`。

##### `WHERE <time_range_column> BETWEEN <start_time> AND <end_time>`

**必填**: 是\
**说明**: 指定要合并的分区的时间范围。此范围内的分区将根据 `PARTITION BY` 子句中定义的规则进行合并。

#### 示例

合并表 `site_access1` 中的分区，并将分区时间粒度从天更改为月。要合并的分区的时间范围是从 `2024-01-01` 到 `2024-03-31`。

```SQL
ALTER TABLE site_access1 PARTITION BY date_trunc('month', event_day)
BETWEEN '2024-01-01' AND '2024-03-31';
```

合并后：

- 日级别分区 `2024-01-01` 到 `2024-01-31` 合并为月级别分区 `2024-01`。
- 日级别分区 `2024-02-01` 到 `2024-02-29` 合并为月级别分区 `2024-02`。
- 日级别分区 `2024-03-01` 到 `2024-03-31` 合并为月级别分区 `2024-03`。

#### 使用须知

- 仅支持基于时间函数的表达式分区合并。
- 不支持合并具有多个分区列的分区。
- 不支持合并操作与 Schema Change/DML 操作并行执行。

## 限制

- 从 v3.1.0 开始，StarRocks 的共享数据模式支持[时间函数表达式](#partitioning-based-on-a-simple-time-function-expression)。从 v3.1.1 开始，StarRocks 的共享数据模式进一步支持[列表达式](#partitioning-based-on-the-column-expression-since-v31)。
- 目前，不支持使用 CTAS 创建配置了表达式分区的表。
- 目前，不支持使用 Spark Load 向使用表达式分区的表加载数据。
- 当使用 `ALTER TABLE <table_name> DROP PARTITION <partition_name>` 语句删除通过列表达式创建的分区时，分区中的数据将直接删除且无法恢复。
- 从 v3.4.0、v3.3.8、v3.2.13 和 v3.1.16 版本开始，StarRocks 支持[备份和恢复](../../administration/management/Backup_and_restore.md)使用表达式分区策略创建的表。

## 附录

### 支持的时间函数

表达式分区支持以下函数：

**时间函数**:

- `timediff`
- `datediff`
- `to_days`
- `years_add`/`sub`
- `quarters_add`/`sub`
- `months_add`/`sub`
- `weeks_add`/`sub`
- `date_add`/`sub`
- `days_add`/`sub`
- `hours_add`/`sub`
- `minutes_add`/`sub`
- `seconds_add`/`sub`
- `milliseconds_add`/`sub`
- `date_trunc`
- `date_format(YmdHiSf/YmdHisf)`
- `str2date(YmdHiSf/YmdHisf)`
- `str_to_date(YmdHiSf/YmdHisf)`
- `to_iso8601`
- `to_date`
- `unix_timestamp`
- `from_unixtime(YmdHiSf/YmdHisf)`
- `time_slice`

**其他函数**:

- 加
- 减
- 转换

:::note

- 支持组合使用多个时间函数。
- 上述所有时间函数均使用系统默认时区。
- 时间函数 `YmdHiSf` 的值格式必须以最粗的时间粒度 `%Y` 开头。不允许使用以更细的时间粒度（例如 `%m-%d`）开头的格式。

**示例**

`PARTITION BY from_unixtime(cast(str as INT) + 3600, '%Y-%m-%d')`

:::
