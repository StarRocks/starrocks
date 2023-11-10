---
displayed_sidebar: "Chinese"
---

# CBO 统计信息

本文介绍 StarRocks CBO（Cost-based Optimizer）优化器的基本概念，以及如何为 CBO 优化器采集统计信息。StarRocks 2.4 版本引入直方图作为统计信息，提供更准确的数据分布统计。

## 什么是 CBO 优化器

CBO 优化器是查询优化的关键。一条 SQL 查询到达 StarRocks 后，会解析为一条逻辑执行计划，CBO 优化器对逻辑计划进行改写和转换，生成多个物理执行计划。通过估算计划中每个算子的执行代价（CPU、内存、网络、I/O 等资源消耗），选择代价最低的一条查询路径作为最终的物理查询计划。

StarRocks CBO 优化器采用 Cascades 框架，基于多种统计信息进行代价估算，能够在数万级别的执行计划中，选择代价最低的执行计划，提升复杂查询的效率和性能。StarRocks 1.16.0 版本推出自研的 CBO 优化器。1.19 版本及以上，该特性默认开启。

统计信息是 CBO 优化器的重要组成部分，统计信息的准确与否决定了代价估算是否准确，进而决定了CBO 优化器的性能好坏。下文详细介绍统计信息的类型、采集策略、以及如何创建采集任务和查看统计信息。

## 统计信息数据类型

StarRocks 会采集多种统计信息，为查询优化提供代价估算的参考。

### 基础统计信息

StarRocks 默认定期采集表和列的如下基础信息：

- row_count : 表的总行数

- data_size: 列的数据大小

- ndv: 列基数，即 distinct value 的个数

- null_count: 列中 NULL 值的个数

- min: 列的最小值

- max: 列的最大值

基础统计信息存储在 `_statistics_.table_statistic_v1` 表中，您可以在 StarRocks 集群 `_statistics_` 数据库下查看。

### 直方图

从 2.4 版本开始，StarRocks 引入直方图 (Histogram)。直方图常用于估算数据分布，当数据存在倾斜时，直方图可以弥补基础统计信息估算存在的偏差，输出更准确的数据分布信息。

StarRocks 采用等深直方图 (Equi-height Histogram)，即选定若干个 bucket，每个 bucket 中的数据量几乎相等。对于出现频次较高、对选择率（selectivity）影响较大的数值，StarRocks 会分配单独的桶进行存储。桶数量越多时，直方图的估算精度就越高，但是也会增加统计信息的内存使用。您可以根据业务情况调整直方图的桶个数和单个采集任务的 MCV（most common value) 个数。

**直方图适用于有明显数据倾斜，并且有频繁查询请求的列。如果您的表数据分布比较均匀，可以不使用直方图。直方图支持的列类型为数值类型、DATE、DATETIME 或字符串类型。**

目前直方图仅支持**手动抽样**采集。直方图统计信息存储在 `_statistics_` 数据库的 `histogram_statistics` 表中。

## 采集类型

随着表的导入或者删除操作，表的大小和数据分布会频繁更新，因此需要定期更新统计信息，以确保统计信息能准确反映表中的实际数据。在创建采集任务之前，您需要根据业务场景决定使用哪种采集类型和采集方式。

StarRocks 支持全量采集 (full collection) 和抽样采集 (sampled collection)。全量采集和抽样采集均支持自动和手动采集方式。

| **采集类型** | **采集方式** | **采集方法**                                                                                                                                                | **优缺点**                                                                          |
|----------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| 全量采集     | 自动或手动    | 扫描全表，采集真实的统计信息值。按照分区（Partition）级别采集，基础统计信息的相应列会按照每个分区采集一条统计信息。如果对应分区无数据更新，则下次自动采集时，不再采集该分区的统计信息，减少资源占用。全量统计信息存储在 `_statistics_.column_statistics` 表中。   | 优点：统计信息准确，有利于优化器更准确地估算执行计划。缺点：消耗大量系统资源，速度较慢。从 2.4.5 版本开始支持用户配置自动采集的时间段，减少资源消耗。 |
| 抽样采集     | 自动或手动    | 从表的每个分区中均匀抽取 N 行数据，计算统计信息。抽样统计信息按照表级别采集，基础统计信息的每一列会存储一条统计信息。列的基数信息 (ndv) 是按照抽样样本估算的全局基数，和真实的基数信息会存在一定偏差。抽样统计信息存储在 `_statistics_.table_statistic_v1` 表中。 | 优点：消耗较少的系统资源，速度快。 缺点：统计信息存在一定误差，可能会影响优化器评估执行计划的准确性。                              |

## 采集统计信息

StarRocks 提供灵活的信息采集方式，您可以根据业务场景选择自动采集、手动采集，也可以自定义自动采集任务。

默认情况下，StarRocks 会周期性自动采集表的全量统计信息。默认检查更新时间为 5 分钟一次，如果发现数据的更新比例满足条件，会自动触发采集。**全量采集可能会消耗大量的系统资源**，如果您不希望使用自动全量采集，可以设置 FE 配置项 `enable_collect_full_statistic` 为 `false`，系统会停止自动全量采集，根据您创建的自定义任务进行定制化采集。

### 自动全量采集 (Auto Full Collection)

对于基础统计信息，StarRocks 默认自动进行全表全量统计信息采集，无需人工操作。对于从未采集过统计信息的表，会在一个调度周期内自动进行统计信息采集。对于已经采集过统计信息的表，StarRocks 会自动更新表的总行数以及修改的行数，将这些信息定期持久化下来，作为是否触发自动采集的判断条件。

从 2.4.5 版本开始，支持用户配置自动全量采集的时间段，防止因集中采集而导致的集群性能抖动。采集时间段可通过 `statistic_auto_analyze_start_time` 和 `statistic_auto_analyze_end_time` 这两个 FE 配置项来配置。

触发自动采集的判断条件：

- 上次统计信息采集之后，该表是否发生过数据变更。
- 分区数据是否发生过修改，未发生过修改的分区不做重新采集。
- 采集是否落在配置的自动采集时间段内（默认为全天采集，可进行修改）。
- 该表的统计信息健康度（`statistic_auto_collect_ratio`）是否低于配置阈值。

> 健康度计算公式：
>
> 1. 当更新分区数量小于 10个 时：1 - MIN(上次统计信息采集后的更新行数/总行数)
>
> 2. 当更新分区数量大于等于 10 个时： 1 - MIN(上次统计信息采集后的更新行数/总行数, 上次统计信息采集后的更新的分区数/总分区数)

同时，StarRocks 对于不同更新频率、不同大小的表，做了详细的配置策略。

对于数据量较小的表，**StarRocks 默认不做限制，即使表的更新频率很高，也会实时采集**。可以通过 `statistic_auto_collect_small_table_size` 配置小表的大小阈值，或者通过`statistic_auto_collect_small_table_interval` 配置小表的采集间隔。

对于数据量较大的表，StarRocks按照以下策略限制：

- 默认采集的间隔不低于 12 小时，通过  `statistic_auto_collect_large_table_interval` 配置。

- 满足采集间隔的条件下，当健康度低于抽样阈采集值时，触发抽样采集，通过 `statistic_auto_collect_sample_threshold` 配置。

- 满足采集间隔的条件下，健康度高于抽样采集阈值，低于采集阈值时，触发全量采集，通过 `statistic_auto_collect_ratio` 配置。

- 当收集的最大分区大小大于 100G 时，触发抽样采集，通过 `statistic_max_full_collect_data_size` 配置。

自动全量采集任务由系统自动执行，默认配置如下。您可以通过 [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) 命令修改。

配置项:

| **FE配置项**                                   | **类型**  | **默认值**      | **说明**                                              |
|---------------------------------------------|---------|--------------|-----------------------------------------------------|
| enable_statistic_collect                    | BOOLEAN | TRUE         | 是否采集统计信息。该参数默认打开。                                   |
| enable_collect_full_statistic               | BOOLEAN | TRUE         | 是否开启自动全量统计信息采集。该参数默认打开。                             |
| statistic_collect_interval_sec              | LONG    | 300          | 自动定期任务中，检测数据更新的间隔，默认 5 分钟。单位：秒。                     |
| statistic_auto_analyze_start_time           | STRING  | 00:00:00     | 用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。       |
| statistic_auto_analyze_end_time             | STRING  | 23:59:59     | 用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。       |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 自动全量采集任务的小表阈值，默认 5 GB，单位：Byte。                         |
| statistic_auto_collect_small_table_interval | LONG    | 3600         | 自动全量采集任务的小表采集间隔，单位：秒。                               |
| statistic_auto_collect_large_table_interval | LONG    | 86400        | 自动全量采集任务的大表采集间隔，单位：秒。                               |
| statistic_auto_collect_ratio                | DOUBLE  | 0.8          | 触发自动统计信息收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。           |
| statistic_auto_collect_sample_threshold     | DOUBLE  | 0.3          | 触发自动统计信息抽样收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动抽样采集。       |
| statistic_max_full_collect_data_size        | LONG    | 107374182400 | 自动统计信息采集的最大分区大小。单位：Byte。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。 |
| statistic_full_collect_buffer               | LONG    | 20971520     | 自动全量采集任务写入的缓存大小，单位：Byte。                              |
| statistic_collect_max_row_count_per_query   | LONG    | 5000000000   | 统计信息采集单次最多查询的数据行数。统计信息任务会按照该配置自动拆分为多次任务执行。 |
| statistic_collect_too_many_version_sleep    | LONG    | 600000       | 当统计信息表的写入版本过多时(Too many tablet 异常)，自动采集任务的休眠时间，单位：秒。 |

### 手动采集 (Manual Collection)

可以通过 ANALYZE TABLE 语句创建手动采集任务。**手动采集默认为同步操作。您也可以将手动任务设置为异步，执行命令后，系统会立即返回命令的状态，但是统计信息采集任务会异步在后台运行。异步采集适用于表数据量大的场景，同步采集适用于表数据量小的场景。手动任务创建后仅会执行一次，无需手动删除。运行状态可以使用 SHOW ANALYZE STATUS 查看**。

#### 手动采集基础统计信息

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property]);
```

参数说明：

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为全量采集。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步采集。

- `col_name`: 要采集统计信息的列，多列使用逗号分隔。如果不指定，表示采集整张表的信息。

- `PROPERTIES`: 采集任务的自定义参数。如果不配置，则采用`fe.conf`中的默认配置。

| **PROPERTIES**                | **类型** | **默认值** | **说明**                           |
|-------------------------------|--------|---------|----------------------------------|
| statistic_sample_collect_rows | INT    | 200000  | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |

**示例**

- 手动全量采集 (Manual full collection)

```SQL
-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE TABLE tbl_name;

-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE FULL TABLE tbl_name;

-- 手动全量采集指定表指定列的统计信息，使用默认配置。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

- 手动抽样采集 (Manual sampled collection)

```SQL
-- 手动抽样采集指定表的统计信息，使用默认配置。
ANALYZE SAMPLE TABLE tbl_name;

-- 手动抽样采集指定表指定列的统计信息，设置抽样行数。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

#### 手动采集直方图统计信息

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

参数说明：

- `col_name`: 要采集统计信息的列，多列使用逗号分隔。该参数必填。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步采集。

- `WITH N BUCKETS`: `N`为直方图的分桶数。如果不指定，则使用 `fe.conf` 中的默认值。

- PROPERTIES: 采集任务的自定义参数。如果不指定，则使用 `fe.conf` 中的默认配置。

| **PROPERTIES**                 | **类型** | **默认值**  | **说明**                           |
|--------------------------------|--------|----------|----------------------------------|
| statistic_sample_collect_rows  | INT    | 200000   | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |
| histogram_mcv_size             | INT    | 100      | 直方图 most common value (MCV) 的数量。 |
| histogram_sample_ratio         | FLOAT  | 0.1      | 直方图采样比例。                         |
| histogram_max_sample_row_count | LONG   | 10000000 | 直方图最大采样行数。                       |

直方图的采样行数由多个参数共同控制，采样行数取 `statistic_sample_collect_rows` 和表总行数 `histogram_sample_ratio` 两者中的最大值。最多不超过 `histogram_max_sample_row_count` 指定的行数。如果超过，则按照该参数定义的上限行数进行采集。

直方图任务实际执行中使用的 **PROPERTIES**，可以通过 SHOW ANALYZE STATUS 中的 **PROPERTIES** 列查看。

**示例**

```SQL
-- 手动采集v1列的直方图信息，使用默认配置。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 手动采集v1列的直方图信息，指定32个分桶，mcv指定为32个，采样比例为50%。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### 自定义自动采集 (Custom Collection)

#### 创建自动采集任务

可以通过 CREATE ANALYZE 语句创建自定义自动采集任务。

创建自定义自动采集任务之前，需要先关闭自动全量采集 `enable_collect_full_statistic=false`，否则自定义采集任务不生效。 在关闭 `enable_collect_full_statistic=false`后， **StarRocks会自动创建自定义采集任务，默认采集抽样采集所有表。**

```SQL
-- 定期采集所有数据库的统计信息。
CREATE ANALYZE [FULL|SAMPLE] ALL PROPERTIES (property [,property]);

-- 定期采集指定数据库下所有表的统计信息。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name PROPERTIES (property [,property]);

-- 定期采集指定表、列的统计信息。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name]) PROPERTIES (property [,property]);
```

参数说明：

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为全量采集。

- `col_name`: 要采集统计信息的列，多列使用逗号 (,)分隔。如果不指定，表示采集整张表的信息。

- `PROPERTIES`: 采集任务的自定义参数。如果不配置，则采用`fe.conf`中的默认配置。

| **PROPERTIES**                        | **类型** | **默认值** | **说明**                                                                                                                           |
|---------------------------------------|--------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| statistic_auto_collect_ratio          | FLOAT  | 0.8     | 自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。                                                                                            |
| statistics_max_full_collect_data_size | INT    | 100     | 自动统计信息采集的最大分区的大小。单位: GB。如果某个分区超过该值，则放弃全量统计信息采集，转为对该表进行抽样统计信息采集。                                                                  |
| statistic_sample_collect_rows         | INT    | 200000  | 抽样采集的采样行数。如果该参数取值超过了实际的表行数，则进行全量采集。                                                                                              |
| statistic_exclude_pattern             | STRING | NULL    | 自动采集时，需要排除的库表，支持正则表达式匹配，匹配的格式为 `database.table`。                                                                                  |
| statistic_auto_collect_interval       | LONG   | 0       | 自动采集的间隔时间，StarRocks 默认根据表大小选择使用`statistic_auto_collect_small_table_interval` 或者 `statistic_auto_collect_large_table_interval`，单位：秒。 |

**示例**

**自动全量采集**

```SQL
-- 定期全量采集所有数据库的统计信息。
CREATE ANALYZE ALL;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE DATABASE db_name;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE FULL DATABASE db_name;

-- 定期全量采集指定表、列的统计信息。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 
    
-- 定期全量采集所有数据库的统计信息，不收集`db_name`数据库。
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);    
```

**自动抽样采集**

```SQL
-- 定期抽样采集指定数据库下所有表的统计信息。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 定期抽样采集指定表、列的统计信息，设置采样行数和健康度阈值。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES(
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);
    
-- 自动采集所有数据库的统计信息，不收集`db_name.tbl_name`表。
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name\.tbl_name"
);    
```

#### 查看自动采集任务

```SQL
SHOW ANALYZE JOB [WHERE predicate]
```

您可以使用 WHERE 子句设定筛选条件，进行返回结果筛选。该语句会返回如下列。

| **列名**       | **说明**                                                         |
|--------------|----------------------------------------------------------------|
| Id           | 采集任务的ID。                                                       |
| Database     | 数据库名。                                                          |
| Table        | 表名。                                                            |
| Columns      | 列名列表。                                                          |
| Type         | 统计信息的类型。取值： FULL，SAMPLE。                                       |
| Schedule     | 调度的类型。自动采集任务固定为 `SCHEDULE`。                                    |
| Properties   | 自定义参数信息。                                                       |
| Status       | 任务状态，包括 PENDING（等待）、RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| LastWorkTime | 最近一次采集时间。                                                      |
| Reason       | 任务失败的原因。如果执行成功则为 `NULL`。                                       |

**示例**

```SQL
-- 查看集群全部自定义采集任务。
SHOW ANALYZE JOB

-- 查看数据库test下的自定义采集任务。
SHOW ANALYZE JOB where `database` = 'test';
```

#### 删除自动采集任务

```SQL
DROP ANALYZE <ID>;
```

**示例**

```SQL
DROP ANALYZE 266030;
```

## 查看采集任务状态

您可以通过 SHOW ANALYZE STATUS 语句查看当前所有采集任务的状态。该语句不支持查看自定义采集任务的状态，如要查看，请使用 SHOW ANALYZE JOB。

```SQL
SHOW ANALYZE STATUS [LIKE | WHERE predicate];
```

您可以使用 `Like` 或 `Where` 来筛选需要返回的信息。

目前 SHOW ANALYZE STATUS 会返回如下列。

| **列名**   | **说明**                                                     |
| ---------- | ------------------------------------------------------------ |
| Id         | 采集任务的ID。                                               |
| Database   | 数据库名。                                                   |
| Table      | 表名。                                                       |
| Columns    | 列名列表。                                                   |
| Type       | 统计信息的类型，包括 FULL，SAMPLE，HISTOGRAM。               |
| Schedule   | 调度的类型。`ONCE` 表示手动，`SCHEDULE` 表示自动。             |
| Status     | 任务状态，包括 RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| StartTime  | 任务开始执行的时间。                                         |
| EndTime    | 任务结束执行的时间。                                         |
| Properties | 自定义参数信息。                                             |
| Reason     | 任务失败的原因。如果执行成功则为 NULL。                      |

## 查看统计信息

### 基础统计信息元数据

```SQL
SHOW STATS META [WHERE predicate];
```

该语句返回如下列。

| **列名**   | **说明**                                            |
| ---------- | --------------------------------------------------- |
| Database   | 数据库名。                                          |
| Table      | 表名。                                              |
| Columns    | 列名。                                              |
| Type       | 统计信息的类型，`FULL` 表示全量，`SAMPLE` 表示抽样。 |
| UpdateTime | 当前表的最新统计信息更新时间。                      |
| Properties | 自定义参数信息。                                    |
| Healthy    | 统计信息健康度。                                    |

### 直方图统计信息元数据

```SQL
SHOW HISTOGRAM META [WHERE predicate];
```

该语句返回如下列。

| **列名**   | **说明**                                  |
| ---------- | ----------------------------------------- |
| Database   | 数据库名。                                |
| Table      | 表名。                                    |
| Column     | 列名。                                    |
| Type       | 统计信息的类型，直方图固定为 `HISTOGRAM`。 |
| UpdateTime | 当前表的最新统计信息更新时间。            |
| Properties | 自定义参数信息。                          |

## 删除统计信息

StarRocks 支持手动删除统计信息。手动删除统计信息时，会删除统计信息数据和统计信息元数据，并且会删除过期内存中的统计信息缓存。需要注意的是，如果当前存在自动采集任务，可能会重新采集之前已删除的统计信息。您可以使用 SHOW ANALYZE STATUS 查看统计信息采集历史记录。

### 删除基础统计信息

```SQL
DROP STATS tbl_name
```

### 删除直方图统计信息

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name];
```

## 取消采集任务

您可以通过 KILL ANALYZE 语句取消正在运行中（Running）的统计信息采集任务，包括手动采集任务和自定义自动采集任务。

手动采集任务的任务 ID 可以在 SHOW ANALYZE STATUS 中查看。自定义自动采集任务的任务 ID 可以在 SHOW ANALYZE JOB 中查看。

```SQL
KILL ANALYZE <ID>;
```

## FE配置项说明

| **FE配置项**                                   | **类型**  | **默认值**      | **说明**                                                                                                            |
|---------------------------------------------|---------|--------------|-------------------------------------------------------------------------------------------------------------------|
| enable_statistic_collect                    | BOOLEAN | TRUE         | 是否采集统计信息。该参数默认打开。                                                                                                 |
| enable_collect_full_statistic               | BOOLEAN | TRUE         | 是否开启自动全量统计信息采集。该参数默认打开。                                                                                           |
| statistic_collect_interval_sec              | LONG    | 300          | 自动定期任务中，检测数据更新的间隔时间，默认为 5 分钟。单位：秒。                                                                                |
| statistic_auto_analyze_start_time           | STRING  | 00:00:00     | 用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。                                                                     |
| statistic_auto_analyze_end_time             | STRING  | 23:59:59     | 用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。                                                                     |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 自动全量采集任务的小表阈值，默认 5GB，单位：Byte。                                                                                       |
| statistic_auto_collect_small_table_interval | LONG    | 3600         | 自动全量采集任务的小表采集间隔，单位：秒。                                                                                             |
| statistic_auto_collect_large_table_interval | LONG    | 86400        | 自动全量采集任务的大表采集间隔，单位：秒。                                                                                             |
| statistic_auto_collect_ratio                | DOUBLE  | 0.8          | 触发自动统计信息收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。                                                                         |
| statistic_auto_collect_sample_threshold     | DOUBLE  | 0.3          | 触发自动统计信息抽样收集的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动抽样采集。                                                                     |
| statistic_max_full_collect_data_size        | LONG    | 107374182400 | 自动统计信息采集的最大分区大小。单位：Byte。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。                                                               |
| statistic_collect_max_row_count_per_query   | LONG    | 5000000000   | 统计信息采集单次最多查询的数据行数。统计信息任务会按照该配置自动拆分为多次任务执行。                                                                        |
| statistic_full_collect_buffer               | LONG    | 20971520     | 自动全量采集任务写入的缓存大小，单位：Byte。                                                                                            |
| statistic_collect_too_many_version_sleep    | LONG    | 600000       | 当统计信息表的写入版本过多时(Too many tablet异常)，自动采集任务的休眠时间，单位：秒。                                                               |
| statistic_sample_collect_rows               | LONG    | 200000       | 最小采样行数。如果指定了采集类型为抽样采集（SAMPLE），需要设置该参数。<br />如果参数取值超过了实际的表行数，默认进行全量采集。                                               |
| statistic_collect_concurrency               | INT     | 3            | 手动采集任务的最大并发数，默认为 3，即最多可以有 3 个手动采集任务同时运行。<br />超出的任务处于 PENDING 状态，等待调度。                                              |
| histogram_buckets_size                      | LONG    | 64           | 直方图默认分桶数。                                                                                                         |
| histogram_mcv_size                          | LONG    | 100          | 直方图默认 most common value 的数量。                                                                                      |
| histogram_sample_ratio                      | FLOAT   | 0.1          | 直方图默认采样比例。                                                                                                        |
| histogram_max_sample_row_count              | LONG    | 10000000     | 直方图最大采样行数。                                                                                                        |
| statistic_manager_sleep_time_sec            | LONG    | 60           | 统计信息相关元数据调度间隔周期。单位：秒。系统根据这个间隔周期，来执行如下操作：<ul><li>创建统计信息表；</li><li>删除已经被删除的表的统计信息；</li><li>删除过期的统计信息历史记录。</li></ul> |
| statistic_analyze_status_keep_second        | LONG    | 259200       | 采集任务记录保留时间，默认为 3 天。单位：秒。                                                                                          |

## 更多信息

- 如需查询 FE 配置项的取值，执行 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md)。

- 如需修改 FE 配置项的取值，执行 [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md)。
