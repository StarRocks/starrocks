# CREATE ANALYZE

## 功能

创建自定义自动采集任务，进行 CBO 统计信息采集。

默认情况下，StarRocks 会周期性自动采集表的全量统计信息。默认检查更新时间为 5 分钟一次，如果发现有数据更新，会自动触发采集。如果您不希望使用自动全量采集，可以设置 FE 配置项 `enable_collect_full_statistic` 为 `false`，系统会停止自动全量采集，根据您创建的自定义任务进行定制化采集。

创建自定义自动采集任务之前，需要先关闭自动全量采集 `enable_collect_full_statistic=false`，否则自定义采集任务不生效。

## 语法

```SQL
-- 定期采集所有数据库的统计信息。
CREATE ANALYZE [FULL|SAMPLE] ALL PROPERTIES (property [,property])

-- 定期采集指定数据库下所有表的统计信息。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name PROPERTIES (property [,property])

-- 定期采集指定表、列的统计信息。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name]) PROPERTIES (property [,property])
```

## 参数说明

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为全量采集。

- `col_name`: 要采集统计信息的列，多列使用逗号 (,)分隔。如果不指定，表示采集整张表的信息。

- PROPERTIES: 采集任务的自定义参数。如果不配置，则采用 `fe.conf` 中的默认配置。任务实际执行中使用的 PROPERTIES，可以通过 [SHOW ANALYZE JOB](../data-definition/SHOW_ANALYZE_JOB.md) 中的 `Properties` 列查看。

| **PROPERTIES**                        | **类型** | **默认值** | **说明**                                                     |
| ------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8        | 自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。 |
| statistics_max_full_collect_data_size | INT      | 100        | 自动统计信息采集的最大分区的大小。单位: GB。如果某个分区超过该值，则放弃全量统计信息采集，转为对该表进行抽样统计信息采集。 |
| statistic_sample_collect_rows         | INT      | 200000     | 抽样采集的采样行数。如果该参数取值超过了实际的表行数，则进行全量采集。 |

## 示例

示例 1：自动全量采集

```SQL
-- 定期全量采集所有数据库的统计信息。
CREATE ANALYZE ALL;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE DATABASE db_name;

-- 定期全量采集指定数据库下所有表的统计信息。
CREATE ANALYZE FULL DATABASE db_name;

-- 定期全量采集指定表、列的统计信息。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 
```

示例 2：自动抽样采集

```SQL
-- 定期抽样采集指定数据库下所有表的统计信息，使用默认配置。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 定期抽样采集指定表、列的统计信息，设置采样行数和健康度阈值。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES(
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);
```

## 相关文档

[SHOW ANALYZE JOB](../data-definition/SHOW_ANALYZE_JOB.md)：查看创建的自定义自动采集任务。

[DROP ANALYZE](../data-definition/DROP_ANALYZE.md)：删除自动采集任务。

[KILL ANALYZE](../data-definition/KILL_ANALYZE.md)：取消正在运行中（Running）的统计信息收集任务。

想了解更多 CBO 统计信息采集的内容，参见 [CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
