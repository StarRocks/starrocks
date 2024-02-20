---
displayed_sidebar: "Chinese"
---

# CREATE MATERIALIZED VIEW

## 功能

创建物化视图。关于物化视图适用的场景请参考[同步物化视图](../../../using_starrocks/Materialized_view-single_table.md)和[异步物化视图](../../../using_starrocks/Materialized_view.md)。

创建物化视图是一个异步的操作。该命令执行成功即代表创建物化视图的任务提交成功。您可以通过 [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) 命令查看当前数据库中同步物化视图的构建状态，或通过查询 [Information Schema](../../../administration/information_schema.md) 中的元数据表 `tasks` 和 `task_runs` 来查看异步物化视图的构建状态。

> **注意**
>
> 只有拥有基表所在数据库的 CREATE MATERIALIZED VIEW 权限的用户才可以创建物化视图。

StarRocks 自 v2.4 起支持异步物化视图。异步物化视图与先前版本中的同步物化视图区别主要体现在以下方面：

|                              | **单表聚合** | **多表关联** | **查询改写** | **刷新策略** | **基表** |
| ---------------------------- | ----------- | ---------- | ----------- | ---------- | -------- |
| **异步物化视图** | 是 | 是 | 是 | <ul><li>异步刷新</li><li>手动刷新</li></ul> | 支持多表构建。基表可以来自：<ul><li>Default Catalog</li><li>External Catalog（v2.5）</li><li>已有异步物化视图（v2.5）</li><li>已有视图（v3.1）</li></ul> |
| **同步物化视图（Rollup）** | 仅部分聚合函数 | 否 | 是 | 导入同步刷新 | 仅支持基于 Default Catalog 的单表构建 |

## 同步物化视图

### 语法

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

### 参数

**mv_name**（必填）

物化视图的名称。命名要求如下：

- 必须由字母（a-z 或 A-Z）、数字（0-9）或下划线（_）组成，且只能以字母开头。
- 总长度不能超过 64 个字符。
- 视图名大小写敏感。

**COMMENT**（选填）

物化视图的注释。注意建立物化视图时 `COMMENT` 必须在 `mv_name` 之后，否则创建失败。

**query_statement**（必填）

创建物化视图的查询语句，其结果即为物化视图中的数据。语法如下：

```SQL
SELECT select_expr[, select_expr ...]
[WHERE where_expr]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr（必填）

  构建同步物化视图的查询语句。

  - 单列或聚合列：形如 `SELECT a, b, c FROM table_a`，其中 `a`、`b` 和 `c` 为基表的列名。如果您没有为物化视图指定列名，那么 StarRocks 自动为这些列命名。
  - 表达式：形如 `SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a`，其中 `a+1`、`b+2` 和 `c*c` 为包含基表列名的表达式，`x`、`y` 和 `z` 为物化视图的列名。

  > **说明**
  >
  > - 该参数至少需包含一个单列。
  > - 同步物化视图仅支持在单列上使用聚合函数。不支持形如 `sum(a+b)` 形式的查询语句。
  > - 使用聚合函数创建同步物化视图时，必须指定 GROUP BY 子句，并在 `select_expr` 中指定至少一个 GROUP BY 列。
  > - 同步物化视图不支持 JOIN、以及 GROUP BY 的 HAVING 子句。
  > - 从 v3.1 开始，每个同步物化视图支持为基表的每一列使用多个聚合函数，支持形如 `select b, sum(a), min(a) from table group by b` 形式的查询语句。
  > - 从 v3.1 开始，同步物化视图支持 SELECT 和聚合函数的复杂表达式，即形如 `select b, sum(a + 1) as sum_a1, min(cast (a as bigint)) as min_a from table group by b` 或 `select abs(b) as col1, a + 1 as col2, cast(a as bigint) as col3 from table` 的查询语句。同步物化视图的复杂表达式有以下限制：
  >   - 每个复杂表达式必须有一个列名，并且基表所有同步物化视图中的不同复杂表达式的别名必须不同。例如，查询语句 `select b, sum(a + 1) as sum_a from table group by b` 和`select b, sum(a) as sum_a from table group by b` 不能同时用于为相同的基表创建同步物化视图。
  >   - 每个复杂表达式只能引用一列。不支持形如 `a + b as col1` 形式的查询语句。
  >   - 您可以通过执行 `EXPLAIN <sql_statement>` 来查看您的查询是否被使用复杂表达式创建的同步物化视图改写。更多信息请参见[查询分析](../../../administration/Query_planning.md)。

- WHERE （选填）

  自 v3.1.8 起，同步物化视图支持通过 WHERE 子句筛选数据。

- GROUP BY（选填）

  构建物化视图查询语句的分组列。如不指定该参数，则默认不对数据进行分组。

- ORDER BY（选填）

  构建物化视图查询语句的排序列。

  - 排序列的声明顺序必须和 `select_expr` 中列声明顺序一致。
  - 如果查询语句中包含分组列，则排序列必须和分组列一致。
  - 如果不指定排序列，则系统根据以下规则自动补充排序列：
  
    - 如果物化视图是聚合类型，则所有的分组列自动补充为排序列。
    - 如果物化视图是非聚合类型，则系统根据前缀列自动选择排序列。

### 查询同步物化视图

因为同步物化视图本质上是基表的索引而不是物理表，所以您只能使用 Hint `[_SYNC_MV_]` 查询同步物化视图：

```SQL
-- 请勿省略 Hint 中的括号[]。
SELECT * FROM <mv_name> [_SYNC_MV_];
```

> **注意**
>
> 目前，StarRocks 会自动为同步物化视图中的列生成名称。您为同步物化视图中的列指定的 Alias 将无法生效。

### 同步物化视图查询自动改写

使用同步物化视图查询时，原始查询语句将会被自动改写并用于查询物化视图中保存的中间结果。

下表展示了原始查询聚合函数和构建同步物化视图用到的聚合函数的匹配关系。您可以根据业务场景选择对应的聚合函数构建同步物化视图。

| **原始查询聚合函数**                                   | **物化视图构建聚合函数** |
| ------------------------------------------------------ | ------------------------ |
| sum                                                    | sum                      |
| min                                                    | min                      |
| max                                                    | max                      |
| count                                                  | count                    |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union             |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                |

## 异步物化视图

### 语法

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
-- distribution_desc
[DISTRIBUTED BY HASH(<bucket_key>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]]
-- refresh_desc
[REFRESH 
-- refresh_moment
    [IMMEDIATE | DEFERRED]
-- refresh_scheme
    [ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>) | MANUAL]
]
-- partition_expression
[PARTITION BY 
    {<date_column> | date_trunc(fmt, <date_column>)}
]
-- order_by_expression
[ORDER BY (<sort_key>)]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

### 参数

**mv_name**（必填）

物化视图的名称。命名要求如下：

- 必须由字母（a-z 或 A-Z）、数字（0-9）或下划线（_）组成，且只能以字母开头。
- 总长度不能超过 64 个字符。
- 视图名大小写敏感。

> **注意**
>
> 同一张基表可以创建多个异步物化视图，但同一数据库内的异步物化视图名称不可重复。

**COMMENT**（选填）

物化视图的注释。注意建立物化视图时 `COMMENT` 必须在 `mv_name` 之后，否则创建失败。

**distribution_desc**（选填）

异步物化视图的分桶方式，包括哈希分桶和随机分桶（自 3.1 版本起）。如不指定该参数，StarRocks 使用随机分桶方式，并自动设置分桶数量。

> **说明**
>
> 创建异步物化视图时必须至少指定 `distribution_desc` 和 `refresh_scheme` 其中之一。

- **哈希分桶**：

  语法

  ```SQL
  DISTRIBUTED BY HASH (<bucket_key1>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
  ```

  更多信息，请参见 [分桶](../../../table_design/Data_distribution.md#分桶)。

  > **说明**
  >
  > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../../../table_design/Data_distribution.md#设置分桶数量)。

- **随机分桶**：

  如果您选择随机分桶方式，并且自动设置分桶数量，则无需指定 `distribution_desc`。如果您需要手动设置分桶数，请使用以下语法：

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <bucket_number>
  ```

  > **注意**
  >
  > 采用随机分桶方式的异步物化视图不支持设置 Colocation Group。

  更多信息，请参见 [随机分桶](../../../table_design/Data_distribution.md#随机分桶自-v31)。

**refresh_moment**（选填）

物化视图的刷新时刻。默认值：`IMMEDIATE`。有效值：

- `IMMEDIATE`：异步物化视图创建成功后立即刷新。
- `DEFERRED`：异步物化视图创建成功后不进行刷新。您可以通过手动调用或创建定时任务触发刷新。

**refresh_scheme**（选填）

> **说明**
>
> 创建异步物化视图时必须至少指定 `distribution_desc` 和 `refresh_scheme` 其中之一。

物化视图的刷新方式。该参数支持如下值：

- `ASYNC`: 自动刷新模式。每当基表数据发生变化时，物化视图会自动刷新。
- `ASYNC [START (<start_time>)] EVERY(INTERVAL <interval>)`: 定时刷新模式。物化视图将按照定义的间隔定时刷新。您可以使用 `DAY`（天）、`HOUR`（小时）、`MINUTE`（分钟）和 `SECOND`（秒）作为单位指定间隔，格式为 `EVERY (interval n day/hour/minute/second)`。默认值为 `10 MINUTE`（10 分钟）。您还可以进一步指定刷新起始时间，格式为 `START('yyyy-MM-dd hh:mm:ss')`。如未指定起始时间，默认使用当前时间。示例：`ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)`。
- `MANUAL`: 手动刷新模式。除非手动触发刷新任务，否则物化视图不会刷新。

如果不指定该参数，则默认使用 MANUAL 方式。

**partition_expression**（选填）

异步物化视图的分区表达式。目前仅支持在创建异步物化视图时使用一个分区表达式。

> **注意**
>
> 异步物化视图暂不支持使用 List 分区策略。

该参数支持如下值：

- `date_column`：用于分区的列的名称。形如 `PARTITION BY dt`，表示按照 `dt` 列进行分区。
- `date_trunc` 函数：形如 `PARTITION BY date_trunc("MONTH", dt)`，表示将 `dt` 列截断至以月为单位进行分区。date_trunc 函数支持截断的单位包括 `YEAR`、`MONTH`、`DAY`、`HOUR` 以及 `MINUTE`。
- `str2date` 函数：用于将基表的 STRING 类型分区键转化为物化视图的分区键所需的日期类型。`PARTITION BY str2date(dt, "%Y%m%d")` 表示 `dt` 列是一个 STRING 类型日期，其日期格式为 `"%Y%m%d"`。`str2date` 函数支持多种日期格式。更多信息，参考[str2date](../../sql-functions/date-time-functions/str2date.md)。自 v3.1.4 起支持。
- `time_slice` 或 `date_slice` 函数：从 v3.1 开始，您可以进一步使用 time_slice 或 date_slice 函数根据指定的时间粒度周期，将给定的时间转化到其所在的时间粒度周期的起始或结束时刻，例如 `PARTITION BY date_trunc("MONTH", time_slice(dt, INTERVAL 7 DAY))`，其中 time_slice 或 date_slice 的时间粒度必须比 `date_trunc` 的时间粒度更细。你可以使用它们来指定一个比分区键更细时间粒度的 GROUP BY 列，例如，`GROUP BY time_slice(dt, INTERVAL 1 MINUTE) PARTITION BY date_trunc('DAY', ts)`。

如不指定该参数，则默认物化视图为无分区。

**order_by_expression**（选填）

异步物化视图的排序键。如不指定该参数，StarRocks 从 SELECT 列中选择部分前缀作为排序键，例如：`select a, b, c, d` 中, 排序列可能为 `a` 和 `b`。此参数自 StarRocks 3.0 起支持。

**PROPERTIES**（选填）

异步物化视图的属性。您可以使用 [ALTER MATERIALIZED VIEW](./ALTER_MATERIALIZED_VIEW.md) 修改已有异步物化视图的属性。

- `session.`: 如果您想要更改与物化视图相关的 Session 变量属性，必须在属性前添加 `session.` 前缀，例如，`session.query_timeout`。对于非 Session 属性，例如，`mv_rewrite_staleness_second`，则无需指定前缀。
- `replication_num`：创建物化视图副本数量。
- `storage_medium`：存储介质类型。有效值：`HDD` 和 `SSD`。
- `storage_cooldown_time`: 当设置存储介质为 SSD 时，指定该分区在该时间点之后从 SSD 降冷到 HDD，设置的时间必须大于当前时间。如不指定该属性，默认不进行自动降冷。取值格式为："yyyy-MM-dd HH:mm:ss"。
- `partition_ttl`: 物化视图分区的生存时间 (TTL)。数据在指定的时间范围内的分区将被保留，过期的分区将被自动删除。单位：`YEAR`、`MONTH`、`DAY`、`HOUR` 和 `MINUTE`。例如，您可以将此属性设置为 `2 MONTH`（2个月）。建议您使用此属性，不推荐使用 `partition_ttl_number`。该属性自 v3.1.5 起支持。
- `partition_ttl_number`：需要保留的最近的物化视图分区数量。对于分区开始时间小于当前时间的分区，当数量超过该值之后，多余的分区将会被删除。StarRocks 将根据 FE 配置项 `dynamic_partition_check_interval_seconds` 中的时间间隔定期检查物化视图分区，并自动删除过期分区。在[动态分区](../../../table_design/dynamic_partitioning.md)场景下，提前创建的未来分区将不会被纳入 TTL 考虑。默认值：`-1`。当值为 `-1` 时，将保留物化视图所有分区。
- `partition_refresh_number`：单次刷新中，最多刷新的分区数量。如果需要刷新的分区数量超过该值，StarRocks 将拆分这次刷新任务，并分批完成。仅当前一批分区刷新成功时，StarRocks 会继续刷新下一批分区，直至所有分区刷新完成。如果其中有分区刷新失败，将不会产生后续的刷新任务。默认值：`-1`。当值为 `-1` 时，将不会拆分刷新任务。
- `excluded_trigger_tables`：在此项属性中列出的基表，其数据产生变化时不会触发对应物化视图自动刷新。该参数仅针对导入触发式刷新，通常需要与属性 `auto_refresh_partitions_limit` 搭配使用。形式：`[db_name.]table_name`。默认值为空字符串。当值为空字符串时，任意的基表数据变化都将触发对应物化视图刷新。
- `auto_refresh_partitions_limit`：当触发物化视图刷新时，需要刷新的最近的物化视图分区数量。您可以通过该属性限制刷新的范围，降低刷新代价，但因为仅有部分分区刷新，有可能导致物化视图数据与基表无法保持一致。默认值：`-1`。当参数值为 `-1` 时，StarRocks 将刷新所有分区。当参数值为正整数 N 时，StarRocks 会将已存在的分区按时间先后排序，并刷新当前分区和 N-1 个历史分区。如果分区数不足 N，则刷新所有已存在的分区。如果物化视图存在提前创建的未来分区，将会刷新所有提前创建的分区。
- `mv_rewrite_staleness_second`：如果当前物化视图的上一次刷新在此属性指定的时间间隔内，则此物化视图可直接用于查询重写，无论基表数据是否更新。如果上一次刷新时间早于此属性指定的时间间隔，StarRocks 通过检查基表数据是否变更决定该物化视图能否用于查询重写。单位：秒。该属性自 v3.0 起支持。
- `colocate_with`：异步物化视图的 Colocation Group。更多信息请参阅 [Colocate Join](../../../using_starrocks/Colocate_join.md)。该属性自 v3.0 起支持。
- `unique_constraints` 和 `foreign_key_constraints`：创建 View Delta Join 查询改写的异步物化视图时的 Unique Key 约束和外键约束。更多信息请参阅 [异步物化视图 - 基于 View Delta Join 场景改写查询](../../../using_starrocks/query_rewrite_with_materialized_views.md#view-delta-join-改写)。该属性自 v3.0 起支持。
- `resource_group`: 为物化视图刷新任务设置资源组。更多关于资源组信息，请参考[资源隔离](../../../administration/resource_group.md)。
- `storage_volume`：如果您使用存算分离集群，则需要指定创建物化视图的 [Storage Volume](../../../deployment/shared_data/s3.md#使用-starrocks-存算分离集群) 名称。该属性自 v3.1 版本起支持。如果未指定该属性，则使用默认 Storage Volume。示例：`"storage_volume" = "def_volume"`。

  > **注意**
  >
  > Unique Key 约束和外键约束仅用于查询重写。导入数据时，不保证进行外键约束校验。您必须确保导入的数据满足约束条件。

**query_statement**（必填）

创建异步物化视图的查询语句，其结果即为异步物化视图中的数据。从 v3.1.6 版本开始，StarRocks 支持使用 Common Table Expression (CTE) 创建异步物化视图。

> **注意**
>
> 异步物化视图暂不支持基于使用 List 分区的基表创建。

### 查询异步物化视图

异步物化视图本质是一张实体表。您可以将其作为普通表进行任何**除直接导入数据**以外的操作。

### 支持数据类型

- 基于 StarRocks 内部数据目录（Default Catalog）创建的异步物化视图支持以下数据类型：

  - **日期类型**：DATE、DATETIME
  - **字符串类型**：CHAR、VARCHAR
  - **数值类型**：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL、PERCENTILE
  - **半结构化类型**：ARRAY、JSON、MAP（自 v3.1 起）、STRUCT（自 v3.1 起）
  - **其他类型**：BITMAP、HLL

> **说明**
>
> 自 v2.4.5 起支持 BITMAP、HLL 以及 PERCENTILE。

- 基于 StarRocks 外部数据目录（External Catalog）创建的异步物化视图支持以下数据类型：

  - Hive Catalog

    - **数值类型**：INT/INTEGER、BIGINT、DOUBLE、FLOAT、DECIMAL
    - **日期类型**：TIMESTAMP
    - **字符串类型**：STRING、VARCHAR、CHAR
    - **半结构化类型**：ARRAY

  - Hudi Catalog

    - **数值类型**：BOOLEAN、INT、LONG、FLOAT、DOUBLE、DECIMAL
    - **日期类型**：DATE、TimeMillis/TimeMicros、TimestampMillis/TimestampMicros
    - **字符串类型**：STRING
    - **半结构化类型**：ARRAY

  - Iceberg Catalog

    - **数值类型**：BOOLEAN、INT、LONG、FLOAT、DOUBLE、DECIMAL(P, S)
    - **日期类型**：DATE、TIME、TIMESTAMP
    - **字符串类型**：STRING、UUID、FIXED(L)、BINARY
    - **半结构化类型**：LIST

## 注意事项

- 当前版本暂时不支持同时创建多个物化视图。仅当当前创建任务完成时，方可执行下一个创建任务。

- 关于同步物化视图：

  - 同步物化视图仅支持单列聚合函数，不支持形如 `sum(a+b)` 的查询语句。
  - 同步物化视图仅支持对同一列数据使用一种聚合函数，不支持形如 `select sum(a), min(a) from table` 的查询语句。
  - 同步物化视图中使用聚合函数需要与 GROUP BY 语句一起使用，且 SELECT 的列中至少包含一个分组列。
  - 同步物化视图创建语句不支持 JOIN、WHERE 以及 GROUP BY 的 HAVING 子句。
  - 使用 ALTER TABLE DROP COLUMN 删除基表中特定列时，需要保证该基表所有同步物化视图中不包含被删除列，否则无法进行删除操作。如果必须删除该列，则需要将所有包含该列的同步物化视图删除，然后进行删除列操作。
  - 为一张表创建过多的同步物化视图会影响导入的效率。导入数据时，同步物化视图和基表数据将同步更新，如果一张基表包含 n 个物化视图，向基表导入数据时，其导入效率大约等同于导入 n 张表，数据导入的速度会变慢。

- 关于嵌套异步物化视图：

  - 每个异步物化视图的刷新方式仅影响当前物化视图。
  - 当前 StarRocks 不对嵌套层数进行限制。生产环境中建议嵌套层数不超过三层。

- 关于外部数据目录异步物化视图：

  - 外部数据目录物化视图仅支持异步定时刷新和手动刷新。
  - 物化视图中的数据不保证与外部数据目录的数据强一致。
  - 目前暂不支持基于资源（Resource）构建物化视图。
  - StarRocks 目前无法感知外部数据目录基表数据是否发生变动，所以每次刷新会默认刷新所有分区。您可以通过手动刷新方式指定刷新部分分区。

## 示例

### 同步物化视图示例

基表结构为：

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

1. 创建一个仅包含原始表 （k1，k2）列的物化视图。

    ```sql
    create materialized view k1_k2 as
    select k1, k2 from duplicate_table;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k1、k2 且不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+------+---------+-------+
    | IndexName       | Field | Type   | Null | Key  | Default | Extra |
    +-----------------+-------+--------+------+------+---------+-------+
    | k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
    |                 | k2    | INT    | Yes  | true | N/A     |       |
    +-----------------+-------+--------+------+------+---------+-------+
    ```

2. 创建一个以 k2 为排序列的物化视图。

    ```sql
    create materialized view k2_order as
    select k2, k1 from duplicate_table order by k2;
    ```

    物化视图的 schema 如下图，物化视图仅包含两列 k2、k1，其中 k2 列为排序列，不带任何聚合。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k1    | INT    | Yes  | false | N/A     | NONE  |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

3. 创建一个以 k1，k2 分组，k3 列为 SUM 聚合的物化视图。

    ```sql
    create materialized view k1_k2_sumk3 as
    select k1, k2, sum(k3) from duplicate_table group by k1, k2;
    ```

    物化视图的 schema 如下图，物化视图包含两列 k1、k2，sum(k3) 其中 k1、k2 为分组列，sum(k3) 为根据 k1、k2 分组后的 k3 列的求和值。

    由于物化视图没有声明排序列，且物化视图带聚合数据，系统默认补充分组列 k1、k2 为排序列。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

4. 创建一个去除重复行的物化视图。

    ```sql
    create materialized view deduplicate as
    select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
    ```

    物化视图 schema 如下图，物化视图包含 k1、k2、k3、k4 列，且不存在重复行。

    ```sql
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | true  | N/A     |       |
    |                 | k4    | BIGINT | Yes  | true  | N/A     |       |
    +-----------------+-------+--------+------+-------+---------+-------+

    ```

5. 创建一个不声明排序列的非聚合型物化视图。

    all_type_table 的 schema 如下：

    ```sql
    +-------+--------------+------+-------+---------+-------+
    | Field | Type         | Null | Key   | Default | Extra |
    +-------+--------------+------+-------+---------+-------+
    | k1    | TINYINT      | Yes  | true  | N/A     |       |
    | k2    | SMALLINT     | Yes  | true  | N/A     |       |
    | k3    | INT          | Yes  | true  | N/A     |       |
    | k4    | BIGINT       | Yes  | true  | N/A     |       |
    | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +-------+--------------+------+-------+---------+-------+
    ```

    物化视图包含 k3、k4、k5、k6、k7 列，且不声明排序列，则创建语句如下：

    ```sql
    create materialized view mv_1 as
    select k3, k4, k5, k6, k7 from all_type_table;
    ```

    系统默认补充的排序列为 k3、k4、k5 三列。这三列类型的字节数之和为 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36。所以补充的是这三列作为排序列。
    物化视图的 schema 如下，可以看到其中 k3、k4、k5 列的 key 字段为 true，也就是排序列。k6、k7 列的 key 字段为 false，也就是非排序列。

    ```sql
    +----------------+-------+--------------+------+-------+---------+-------+
    | IndexName      | Field | Type         | Null | Key   | Default | Extra |
    +----------------+-------+--------------+------+-------+---------+-------+
    | mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
    |                | k4    | BIGINT       | Yes  | true  | N/A     |       |
    |                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    |                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    |                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +----------------+-------+--------------+------+-------+---------+-------+
    ```

6. 使用 WHERE 子句和复杂表达式创建同步物化视图。

  ```sql
  -- 创建基表 user_event
  CREATE TABLE user_event (
      ds date   NOT NULL,
      id  varchar(256)    NOT NULL,
      user_id int DEFAULT NULL,
      user_id1    varchar(256)    DEFAULT NULL,
      user_id2    varchar(256)    DEFAULT NULL,
      column_01   int DEFAULT NULL,
      column_02   int DEFAULT NULL,
      column_03   int DEFAULT NULL,
      column_04   int DEFAULT NULL,
      column_05   int DEFAULT NULL,
      column_06   DECIMAL(12,2)   DEFAULT NULL,
      column_07   DECIMAL(12,3)   DEFAULT NULL,
      column_08   JSON   DEFAULT NULL,
      column_09   DATETIME    DEFAULT NULL,
      column_10   DATETIME    DEFAULT NULL,
      column_11   DATE    DEFAULT NULL,
      column_12   varchar(256)    DEFAULT NULL,
      column_13   varchar(256)    DEFAULT NULL,
      column_14   varchar(256)    DEFAULT NULL,
      column_15   varchar(256)    DEFAULT NULL,
      column_16   varchar(256)    DEFAULT NULL,
      column_17   varchar(256)    DEFAULT NULL,
      column_18   varchar(256)    DEFAULT NULL,
      column_19   varchar(256)    DEFAULT NULL,
      column_20   varchar(256)    DEFAULT NULL,
      column_21   varchar(256)    DEFAULT NULL,
      column_22   varchar(256)    DEFAULT NULL,
      column_23   varchar(256)    DEFAULT NULL,
      column_24   varchar(256)    DEFAULT NULL,
      column_25   varchar(256)    DEFAULT NULL,
      column_26   varchar(256)    DEFAULT NULL,
      column_27   varchar(256)    DEFAULT NULL,
      column_28   varchar(256)    DEFAULT NULL,
      column_29   varchar(256)    DEFAULT NULL,
      column_30   varchar(256)    DEFAULT NULL,
      column_31   varchar(256)    DEFAULT NULL,
      column_32   varchar(256)    DEFAULT NULL,
      column_33   varchar(256)    DEFAULT NULL,
      column_34   varchar(256)    DEFAULT NULL,
      column_35   varchar(256)    DEFAULT NULL,
      column_36   varchar(256)    DEFAULT NULL,
      column_37   varchar(256)    DEFAULT NULL
  )
  PARTITION BY date_trunc("day", ds)
  DISTRIBUTED BY hash(id);
  
  -- 使用 WHERE 子句和复杂表达式创建同步物化视图
  CREATE MATERIALIZED VIEW test_mv1
  AS 
  SELECT
  ds,
  column_19,
  column_36,
  sum(column_01) as column_01_sum,
  bitmap_union(to_bitmap( user_id)) as user_id_dist_cnt,
  bitmap_union(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,
  bitmap_union(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,
  bitmap_union(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,
  bitmap_union(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,
  bitmap_union(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
  FROM user_event
  WHERE ds >= '2023-11-02'
  GROUP BY
  ds,
  column_19,
  column_36;
  ```

### 异步物化视图示例

以下示例基于下列基表。

```SQL
CREATE TABLE `lineorder` (
  `lo_orderkey` int(11) NOT NULL COMMENT "",
  `lo_linenumber` int(11) NOT NULL COMMENT "",
  `lo_custkey` int(11) NOT NULL COMMENT "",
  `lo_partkey` int(11) NOT NULL COMMENT "",
  `lo_suppkey` int(11) NOT NULL COMMENT "",
  `lo_orderdate` int(11) NOT NULL COMMENT "",
  `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
  `lo_shippriority` int(11) NOT NULL COMMENT "",
  `lo_quantity` int(11) NOT NULL COMMENT "",
  `lo_extendedprice` int(11) NOT NULL COMMENT "",
  `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
  `lo_discount` int(11) NOT NULL COMMENT "",
  `lo_revenue` int(11) NOT NULL COMMENT "",
  `lo_supplycost` int(11) NOT NULL COMMENT "",
  `lo_tax` int(11) NOT NULL COMMENT "",
  `lo_commitdate` int(11) NOT NULL COMMENT "",
  `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("-2147483648"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`);

CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL COMMENT "",
  `c_name` varchar(26) NOT NULL COMMENT "",
  `c_address` varchar(41) NOT NULL COMMENT "",
  `c_city` varchar(11) NOT NULL COMMENT "",
  `c_nation` varchar(16) NOT NULL COMMENT "",
  `c_region` varchar(13) NOT NULL COMMENT "",
  `c_phone` varchar(16) NOT NULL COMMENT "",
  `c_mktsegment` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`);

CREATE TABLE IF NOT EXISTS `dates` (
  `d_datekey` int(11) NOT NULL COMMENT "",
  `d_date` varchar(20) NOT NULL COMMENT "",
  `d_dayofweek` varchar(10) NOT NULL COMMENT "",
  `d_month` varchar(11) NOT NULL COMMENT "",
  `d_year` int(11) NOT NULL COMMENT "",
  `d_yearmonthnum` int(11) NOT NULL COMMENT "",
  `d_yearmonth` varchar(9) NOT NULL COMMENT "",
  `d_daynuminweek` int(11) NOT NULL COMMENT "",
  `d_daynuminmonth` int(11) NOT NULL COMMENT "",
  `d_daynuminyear` int(11) NOT NULL COMMENT "",
  `d_monthnuminyear` int(11) NOT NULL COMMENT "",
  `d_weeknuminyear` int(11) NOT NULL COMMENT "",
  `d_sellingseason` varchar(14) NOT NULL COMMENT "",
  `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
  `d_holidayfl` int(11) NOT NULL COMMENT "",
  `d_weekdayfl` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_datekey`);

CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT "",
  `s_name` varchar(26) NOT NULL COMMENT "",
  `s_address` varchar(26) NOT NULL COMMENT "",
  `s_city` varchar(11) NOT NULL COMMENT "",
  `s_nation` varchar(16) NOT NULL COMMENT "",
  `s_region` varchar(13) NOT NULL COMMENT "",
  `s_phone` varchar(16) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`);

CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL COMMENT "",
  `p_name` varchar(23) NOT NULL COMMENT "",
  `p_mfgr` varchar(7) NOT NULL COMMENT "",
  `p_category` varchar(8) NOT NULL COMMENT "",
  `p_brand` varchar(10) NOT NULL COMMENT "",
  `p_color` varchar(12) NOT NULL COMMENT "",
  `p_type` varchar(26) NOT NULL COMMENT "",
  `p_size` int(11) NOT NULL COMMENT "",
  `p_container` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`);

create table orders ( 
    dt date NOT NULL, 
    order_id bigint NOT NULL, 
    user_id int NOT NULL, 
    merchant_id int NOT NULL, 
    good_id int NOT NULL, 
    good_name string NOT NULL, 
    price int NOT NULL, 
    cnt int NOT NULL, 
    revenue int NOT NULL, 
    state tinyint NOT NULL 
) 
PRIMARY KEY (dt, order_id) 
PARTITION BY RANGE(`dt`) 
( PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')), 
PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')) ) 
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "3", 
    "enable_persistent_index" = "true"
);
```

示例一：从源表创建非分区物化视图

```SQL
CREATE MATERIALIZED VIEW lo_mv1
DISTRIBUTED BY HASH(`lo_orderkey`)
REFRESH ASYNC
AS
select
    lo_orderkey, 
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_custkey 
order by lo_orderkey;
```

示例二：从源表创建分区物化视图

```SQL
CREATE MATERIALIZED VIEW lo_mv2
PARTITION BY `lo_orderdate`
DISTRIBUTED BY HASH(`lo_orderkey`)
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    lo_orderkey,
    lo_orderdate,
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_orderdate, lo_custkey
order by lo_orderkey;

# 使用 date_trunc 函数将 `dt` 列截断至以月为单位进行分区。
CREATE MATERIALIZED VIEW order_mv1
PARTITION BY date_trunc('month', `dt`)
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    dt,
    order_id,
    user_id,
    sum(cnt) as total_cnt,
    sum(revenue) as total_revenue, 
    count(state) as state_count
from orders
group by dt, order_id, user_id;
```

示例三：创建异步物化视图

```SQL
CREATE MATERIALIZED VIEW flat_lineorder
DISTRIBUTED BY HASH(`lo_orderkey`)
REFRESH MANUAL
AS
SELECT
    l.LO_ORDERKEY AS LO_ORDERKEY,
    l.LO_LINENUMBER AS LO_LINENUMBER,
    l.LO_CUSTKEY AS LO_CUSTKEY,
    l.LO_PARTKEY AS LO_PARTKEY,
    l.LO_SUPPKEY AS LO_SUPPKEY,
    l.LO_ORDERDATE AS LO_ORDERDATE,
    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
    l.LO_QUANTITY AS LO_QUANTITY,
    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
    l.LO_DISCOUNT AS LO_DISCOUNT,
    l.LO_REVENUE AS LO_REVENUE,
    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
    l.LO_TAX AS LO_TAX,
    l.LO_COMMITDATE AS LO_COMMITDATE,
    l.LO_SHIPMODE AS LO_SHIPMODE,
    c.C_NAME AS C_NAME,
    c.C_ADDRESS AS C_ADDRESS,
    c.C_CITY AS C_CITY,
    c.C_NATION AS C_NATION,
    c.C_REGION AS C_REGION,
    c.C_PHONE AS C_PHONE,
    c.C_MKTSEGMENT AS C_MKTSEGMENT,
    s.S_NAME AS S_NAME,
    s.S_ADDRESS AS S_ADDRESS,
    s.S_CITY AS S_CITY,
    s.S_NATION AS S_NATION,
    s.S_REGION AS S_REGION,
    s.S_PHONE AS S_PHONE,
    p.P_NAME AS P_NAME,
    p.P_MFGR AS P_MFGR,
    p.P_CATEGORY AS P_CATEGORY,
    p.P_BRAND AS P_BRAND,
    p.P_COLOR AS P_COLOR,
    p.P_TYPE AS P_TYPE,
    p.P_SIZE AS P_SIZE,
    p.P_CONTAINER AS P_CONTAINER FROM lineorder AS l 
INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```

示例四：创建分区物化视图，并将基表 STRING 类型分区键转化为日期类型作为异步物化视图分区键。

```sql
-- 创建分区键为 STRING 类型的基表。
CREATE TABLE `part_dates` (
  `d_date` varchar(20) DEFAULT NULL,
  `d_dayofweek` varchar(10) DEFAULT NULL,
  `d_month` varchar(11) DEFAULT NULL,
  `d_year` int(11) DEFAULT NULL,
  `d_yearmonthnum` int(11) DEFAULT NULL,
  `d_yearmonth` varchar(9) DEFAULT NULL,
  `d_daynuminweek` int(11) DEFAULT NULL,
  `d_daynuminmonth` int(11) DEFAULT NULL,
  `d_daynuminyear` int(11) DEFAULT NULL,
  `d_monthnuminyear` int(11) DEFAULT NULL,
  `d_weeknuminyear` int(11) DEFAULT NULL,
  `d_sellingseason` varchar(14) DEFAULT NULL,
  `d_lastdayinweekfl` int(11) DEFAULT NULL,
  `d_lastdayinmonthfl` int(11) DEFAULT NULL,
  `d_holidayfl` int(11) DEFAULT NULL,
  `d_weekdayfl` int(11) DEFAULT NULL,
  `d_datekey` varchar(11) DEFAULT NULL
) partition by (d_datekey);


-- 使用 `str2date` 函数创建分区物化视图。
CREATE MATERIALIZED VIEW IF NOT EXISTS `test_mv` 
PARTITION BY str2date(`d_datekey`,'%Y%m%d')
DISTRIBUTED BY HASH(`d_date`, `d_month`, `d_month`) 
REFRESH MANUAL 
AS
SELECT
  `d_date` ,
  `d_dayofweek`,
  `d_month` ,
  `d_yearmonthnum` ,
  `d_yearmonth` ,
  `d_daynuminweek`,
  `d_daynuminmonth`,
  `d_daynuminyear` ,
  `d_monthnuminyear` ,
  `d_weeknuminyear` ,
  `d_sellingseason`,
  `d_lastdayinweekfl`,
  `d_lastdayinmonthfl`,
  `d_holidayfl` ,
  `d_weekdayfl`,
  `d_datekey`
FROM
`hive_catalog`.`ssb_1g_orc`.`part_dates` ;
```

## 更多操作

如要创建逻辑视图，请参见 [CREATE VIEW](./CREATE_VIEW.md)。
