# CBO优化器

StarRocks 1.16.0 版本推出CBO优化器（Cost-based Optimizer ）。StarRocks 1.19及以上版本，该特性默认开启。CBO优化器能够基于成本选择最优的执行计划，大幅提升复杂查询的效率和性能。

## 什么是CBO优化器

CBO优化器采用Cascades框架，使用多种统计信息来完善成本估算，同时补充逻辑转换（Transformation Rule）和物理实现（Implementation Rule）规则，能够在数万级别执行计划的搜索空间中，选择成本最低的最优执行计划。

CBO优化器会使用StarRocks定期采集的多种统计信息，例如行数，列的平均大小、基数信息、NULL值数据量、MAX/MIN值等，这些统计信息存储在`_statistics_.table_statistic_v1`中。

统计信息支持全量或抽样的采集类型，并且支持手动或自动定期的采集方式，可以帮助CBO优化器完善成本估算，选择最优执行计划。

| 采集类型 | 采集方式         | 说明                                                         | 优缺点                                                       |
| -------- | ---------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 全量采集 | 手动和自动定期。 | 对整个表的所有数据，计算统计信息。                           | 优点：全量采集的统计信息准确，能够帮助CBO更准确地评估执行计划。 缺点：全量采集消耗大量系统资源，速度慢。 |
| 抽样采集 | 手动和自动定期。 | 从表的每一个分区（Partition）中均匀抽取N行数据，计算统计信息。 | 优点：抽样采集消耗较少的系统资源，速度快。 缺点：抽样采集的统计信息存在一定误差，可能会影响CBO评估执行计划的准确性。 |

> - 手动采集：手动执行一次采集任务，采集统计信息。
> - 自动定期采集：周期性执行采集任务，采集统计信息。采集间隔时间默认为一天。StarRocks默认每隔两个小时检查数据是否更新。如果检查到数据更新，且距离上一次采集时间已超过采集间隔时间（默认为一天），则StarRocks会重新采集统计信息；反之，则StarRocks不重新采集统计信息。

## 采集统计信息

CBO优化器开启后，需要使用统计信息，统计信息默认为抽样且自动定期采集。抽样行数为20万行，采集间隔时间为一天。每隔两小时StarRocks会检查数据是否更新。您也可以按照业务需要，进行如下设置，调整统计信息的采集类型（全量或抽样）和采集方式（手动或自动定期）。

### 全量采集

选择采集方式（手动或自动定期），并执行相关命令。相关参数说明，请参见[参数说明](./Cost_based_optimizer.md#参数说明)。

- 如果为手动采集，您可以执行如下命令。

```SQL
ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...);
```

- 如果为自动定期采集，您可以执行如下命令，设置采集间隔时间、检查间隔时间。

> 如果多个自动定期采集任务的采集对象完全一致，则CBO仅运行最新创建的自动定期采集任务(即ID最大的任务)。

```SQL
-- 定期全量采集所有数据库的统计信息。
CREATE ANALYZE FULL ALL 
PROPERTIES(
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE FULL DATABASE db_name 
PROPERTIES(
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);

-- 定期全量采集指定表、列的统计信息。
CREATE ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...) 
PROPERTIES(
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);
```

示例：

```SQL
-- 定期全量采集tpch数据库下所有表的统计信息，采集间隔时间为默认，检查间隔时间为默认。
CREATE ANALYZE FULL DATABASE tpch;
```

### 抽样采集

选择采集方式（手动或自动定期），并执行相关命令。相关参数说明，请参见[参数说明](./Cost_based_optimizer.md#参数说明)。

- 如果为手动采集，您可以执行如下命令，设置抽样行数。

```SQL
ANALYZE TABLE tbl_name(columnA, columnB, columnC...)
PROPERTIES(
    "sample_collect_rows" = "100000"
);
```

- 如果为自动定期采集，您可以执行如下命令，设置抽样行数、采集间隔时间、检查间隔时间。

> 如果多个自动定期采集任务的采集对象完全一致，则CBO仅运行最新创建的自动定期采集任务(即ID最大的任务)。

```SQL
-- 定期抽样采集所有数据库的统计信息。
CREATE ANALYZE ALL
PROPERTIES(
    "sample_collect_rows" = "100000",
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);

-- 定期抽样采集指定数据库下所有表的统计信息。
CREATE ANALYZE DATABASE db_name
PROPERTIES(
    "sample_collect_rows" = "100000",
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);

-- 定期抽样采集指定表、列的统计信息。
CREATE ANALYZE TABLE tbl_name(columnA, columnB, columnC...)
PROPERTIES(
    "sample_collect_rows" = "100000",
    "update_interval_sec" = "43200",
    "collect_interval_sec" = "3600"
);
```

示例：

```SQL
-- 每隔43200秒（12小时）定期抽样采集所有数据库的统计信息，检查间隔时间为默认。
CREATE ANALYZE ALL PROPERTIES("update_interval_sec" = "43200");

-- 定期抽样采集test表中v1列的统计信息，采集间隔时间为默认，检查间隔时间为默认。
CREATE ANALYZE TABLE test(v1);
```

### 查询或删除采集任务

执行如下命令，显示所有采集任务。

```SQL
SHOW ANALYZE;
```

执行如下命令，删除指定采集任务。

> `ID`为采集任务ID，可以通过`SHOW ANALYZE`获取。

```SQL
DROP ANALYZE <ID>;
```

### 参数说明

- `sample_collect_rows`：抽样采集时的抽样行数。
- `update_interval_sec`：自动定期任务的采集间隔时间，默认为86400（一天），单位为秒。
- `collect_interval_sec`：自动定期任务中，检测数据更新的间隔时间，默认为7200（两小时），单位为秒。自动定期任务执行时，StarRocks每隔一段时间会检查表中数据是否更新，如果检查到数据更新，且距离上一次采集时间已超过`update_interval_sec`，则StarRocks会重新采集统计信息；反之，则StarRocks不重新采集统计信息。

## FE配置项

您可以在FE配置文件fe.conf中，查询或修改统计信息采集的默认配置。

```Plain_Text
# 是否采集统计信息。
enable_statistic_collect = true

# 自动定期任务中，检测数据更新的间隔时间，默认为7200（两小时），单位为秒。
statistic_collect_interval_sec = 7200

# 自动定期任务的采集间隔时间，默认为86400（一天），单位为秒。
statistic_update_interval_sec = 86400

# 采样统计信息Job的默认采样行数，默认为200000行。
statistic_sample_collect_rows = 200000
```
