# ANALYZE TABLE

## 功能

创建手动采集任务，进行 CBO 统计信息采集。**手动采集默认为同步操作。您也可以将手动任务设置为异步，执行命令后，会立即返回命令的状态，但是统计信息采集任务会在后台运行，运行的状态可以使用 SHOW ANALYZE STATUS 查看。手动任务创建后仅会执行一次，无需手动删除。**

### 手动采集基础统计信息

关于基础统计信息，请参见[CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md#统计信息数据类型)。

#### 语法

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property])
```

#### 参数说明

- 采集类型
  - FULL：全量采集。
  - SAMPLE：抽样采集。
  - 如果不指定采集类型，默认为全量采集。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步操作。

- `col_name`: 要采集统计信息的列，多列使用逗号分隔。如果不指定，表示采集整张表的信息。

- PROPERTIES: 采集任务的自定义参数。如果不配置，则采用`fe.conf`中的默认配置。任务实际执行中使用的 PROPERTIES，可以通过 SHOW ANALYZE STATUS 返回结果中的 `Properties` 列查看。

| **PROPERTIES**                | **类型** | **默认值** | **说明**                                                     |
| ----------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000     | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |

#### 示例

示例1：手动全量采集

```SQL
-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE TABLE tbl_name;

-- 手动全量采集指定表的统计信息，使用默认配置。
ANALYZE FULL TABLE tbl_name;

-- 手动全量采集指定表指定列的统计信息，使用默认配置。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

示例2：手动抽样采集

```SQL
-- 手动抽样采集指定表的统计信息，使用默认配置。
ANALYZE SAMPLE TABLE tbl_name;

-- 手动抽样采集指定表指定列的统计信息，设置抽样行数。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

### 手动采集直方图统计信息

关于直方图的说明，请参见[CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md#统计信息数据类型)。

#### 语法

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

#### 参数说明

- `col_name`: 要采集统计信息的列，多列使用逗号分隔。该参数必填。

- `WITH SYNC | ASYNC MODE`: 如果不指定，默认为同步采集。

- `WITH ``N`` BUCKETS`: `N`为直方图的分桶数。如果不指定，则使用`fe.conf`中的默认值。

- PROPERTIES: 采集任务的自定义参数。如果不指定，则使用`fe.conf`中的默认配置。

| **PROPERTIES**                 | **类型** | **默认值** | **说明**                                                     |
| ------------------------------ | -------- | ---------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000     | 最小采样行数。如果参数取值超过了实际的表行数，默认进行全量采集。 |
| histogram_mcv_size             | INT      | 100        | 直方图 most common value (MCV) 的数量。                      |
| histogram_sample_ratio         | FLOAT    | 0.1        | 直方图采样比例。                                             |
| histogram_max_sample_row_count | LONG     | 10000000   | 直方图最大采样行数。                                         |

直方图的采样行数由多个参数共同控制，采样行数取 `statistic_sample_collect_rows` 和表总行数 `histogram_sample_ratio` 两者中的最大值。最多不超过 `histogram_max_sample_row_count` 指定的行数。如果超过，则按照该参数定义的上限行数进行采集。

直方图任务实际执行中使用的**PROPERTIES**，可以通过 SHOW ANALYZE STATUS 返回结果中的**PROPERTIES**列查看。

#### 示例

```SQL
-- 手动采集v1列的直方图信息，使用默认配置。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 手动采集 v1 列的直方图信息，指定 32 个分桶，MCV 指定为 32 个，采样比例为 50%。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

## 相关文档

[SHOW ANALYZE STATUS](../data-definition/SHOW_ANALYZE_STATUS.md)：查看当前所有**手动采集任务**的状态。

[KILL ANALYZE](../data-definition/KILL_ANALYZE.md)：取消**正在运行中（Running）**的统计信息收集任务。

想了解更多 CBO 统计信息采集的内容，参见[CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
