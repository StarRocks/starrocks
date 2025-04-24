---
displayed_sidebar: docs
sidebar_position: 40
---

# 聚合表

在创建表时，您可以定义一个聚合键，并为值列指定一个聚合函数。当多行数据具有相同的聚合键时，值列中的值将被聚合。此外，您可以单独定义排序键。如果查询中的过滤条件包含排序键，StarRocks 可以快速过滤数据，提高查询效率。

在数据分析和聚合场景中，聚合表可以减少需要处理的数据量，从而提升查询效率。

## 场景

聚合表非常适合用于数据统计和分析场景。以下是一些示例：

- 帮助网站或应用程序提供商分析用户在特定网站或应用程序上的流量和停留时间，以及网站或应用程序的总访问次数。

- 帮助广告公司分析他们为客户提供的广告的总点击量、总浏览量和消费统计数据。

- 帮助电子商务公司分析其年度交易数据，以识别各个季度或月份内的地理畅销商品。

上述场景中的数据查询和摄取具有以下特点：

- 大多数查询是聚合查询，如 SUM、MAX 和 MIN。
- 不需要检索原始明细数据。
- 历史数据不经常更新。仅追加新数据。

## 原理

从数据摄取到数据查询，聚合表中的数据会多次被聚合，具体如下：

1. 在数据摄取阶段，当数据以批次导入聚合表时，每批数据形成一个版本。在一个版本中，具有相同聚合键的数据将被聚合。

2. 在后台压缩阶段，当在数据摄取时生成的多个数据版本的文件定期压缩成一个大文件时，StarRocks 会在大文件中聚合具有相同聚合键的数据。

3. 在数据查询阶段，StarRocks 会在返回查询结果之前聚合所有数据版本中具有相同聚合键的数据。

聚合操作有助于减少需要处理的数据量，从而加速查询。

假设您有一个使用聚合表的表，并希望将以下四条原始记录导入表中。

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 1    |
| 2020.05.01 | CHN     | 2    |
| 2020.05.01 | USA     | 3    |
| 2020.05.01 | USA     | 4    |

StarRocks 在数据摄取时将这四条原始记录聚合为以下两条记录。

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 3    |
| 2020.05.01 | USA     | 7    |

## 创建表

假设您希望分析来自不同城市的用户访问不同网页的次数。在此示例中，创建一个名为 `example_db.aggregate_tbl` 的表，定义 `site_id`、`date` 和 `city_code` 作为聚合键，定义 `pv` 作为值列，并为 `pv` 列指定 SUM 函数。

创建表的语句如下：

```SQL
CREATE TABLE aggregate_tbl (
    site_id LARGEINT NOT NULL COMMENT "id of site",
    date DATE NOT NULL COMMENT "time of event",
    city_code VARCHAR(20) COMMENT "city_code of user",
    pv BIGINT SUM DEFAULT "0" COMMENT "total page views"
)
AGGREGATE KEY(site_id, date, city_code)
DISTRIBUTED BY HASH(site_id);
```

> **注意**
>
> - 创建表时，必须使用 `DISTRIBUTED BY HASH` 子句指定分桶列。有关详细信息，请参见 [bucketing](../data_distribution/Data_distribution.md#bucketing)。
> - 自 v2.5.7 起，StarRocks 可以在创建表或添加分区时自动设置桶的数量（BUCKETS）。您不再需要手动设置桶的数量。有关详细信息，请参见 [set the number of buckets](../data_distribution/Data_distribution.md#set-the-number-of-buckets)。

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

## 通用聚合函数状态

<Beta />

StarRocks 从 v3.4.0 开始支持通用聚合函数状态。

在数据分析和汇总中，聚合表减少了查询过程中处理的数据量，提高了查询性能。对于大型数据集，聚合表非常有效，因为它们在查询之前按维度汇总数据。它们还作为 StarRocks 中增量聚合函数计算的重要方法。然而，在早期版本中，支持仅限于内置函数，如 `SUM`、`MAX`、`MIN`、`REPLACE`、`HLL_UNION`、`PERCENTILE_UNION` 和 `BITMAP_UNION`，而理论上，所有内置聚合函数都可以在聚合表中使用。为了解决这一限制，引入了通用聚合状态以支持存储所有内置函数状态。

### 存储通用聚合状态

您可以通过指定函数名称和输入参数类型在聚合表中定义通用聚合状态，以唯一标识一个聚合函数。列类型将自动推断为聚合函数的中间状态类型。

定义：

```SQL
col_name agg_func_name(parameter1_type, [parameter2_type], ...)
```

- **col_name**: 列的名称。
- **agg_func_name**: 需要存储其中间状态的聚合函数的名称。
- **parameter_type**: 聚合函数的输入参数类型。可以通过参数类型唯一标识该函数。

:::note

- 仅支持至少有一个参数的 StarRocks 内置函数。不支持 Java 和 Python UDAF。
- 为了稳定性和可扩展性，聚合状态列类型始终为 Nullable（除计数函数外），且不可修改。
- 定义多参数函数时不需要参数值，因为类型可以推断，参数值不参与计算。
- 不支持复杂参数，如 ORDER BY 和 DISTINCT。
- 对于特定内置函数的支持，如 `GROUP_CONCAT`、`WINDOW_FUNNEL` 和 `APPROX_TOP_K`，仍在开发中。它们将在未来版本中得到支持。有关详细信息，请参见 [FunctionSet.java#UNSUPPORTED_AGG_STATE_FUNCTIONS](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java#L776)。

:::

示例：

```SQL
CREATE TABLE test_create_agg_table (
  dt VARCHAR(10),
  -- 定义通用聚合状态存储。
  hll_sketch_agg ds_hll_count_distinct(varchar),
  avg_agg avg(bigint),
  array_agg_agg array_agg(int),
  min_by_agg min_by(varchar, bigint)
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;
```

### 组合函数

通用聚合状态使用组合函数封装中间状态计算和流程。

#### `_state` 组合函数

`_state` 函数将输入参数转换为中间状态类型。

定义：

```SQL
agg_intermediate_type {agg_func_name}_state(input_col1, [input_col2], ...)
```

- **agg_func_name**: 需要将输入参数转换为中间状态类型的聚合函数名称。
- **input_col1/col2**: 聚合函数的输入列。
- **agg_intermediate_type**: `_state` 函数的返回类型，即聚合函数的中间状态类型。

:::note

`_state` 是一个标量函数。您不需要为输入参数状态的计算定义聚合列。

:::

示例：

```SQL
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
PARTITION BY (dt)
DISTRIBUTED BY HASH(id) BUCKETS 4;

INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 10, "2024-07-24" FROM table(generate_series(1, 100));

-- 使用 _state 组合函数转换 t1 中的数据，并将其插入到聚合表中。
INSERT INTO test_create_agg_table
SELECT
    dt,
    ds_hll_count_distinct_state(id),
    avg_state(id),
    array_agg_state(id),
    min_by_state(province, id)
FROM t1;
```

#### `_union` 组合函数

`_union` 函数将多个中间状态列合并为一个状态。

定义：

```SQL
-- 合并多个聚合中间状态。
agg_intermediate_type {agg_func_name}_union(input_col)
```

- **agg_func_name**: 聚合函数的名称。
- **input_col**: 聚合函数的输入列。输入列类型是聚合函数的中间状态类型。您可以使用 `_state` 函数获取它。
- **agg_intermediate_type**: `_union` 函数的返回类型，即聚合函数的中间状态类型。

:::note

`_union` 是一个聚合函数。它返回中间状态类型，而不是函数最终结果的类型。

:::

示例：

```SQL
-- 案例 1：合并聚合表的中间状态。
SELECT 
    dt,
    ds_hll_count_distinct_union(hll_sketch_agg),
    avg_union(avg_agg),
    array_agg_union(array_agg_agg),
    min_by_union(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- 案例 2：合并 _state 组合函数输入的中间状态。
SELECT 
    dt,
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)),
    avg_union(avg_state(id)),
    array_agg_union(array_agg_state(id)),
    min_by_union(min_by_state(province, id))
FROM t1
GROUP BY dt
LIMIT 1;
```

#### `_merge` 组合函数

`_merge` 组合函数将聚合函数封装为通用聚合函数，以计算多个中间状态的最终聚合结果。

定义：

```SQL
-- 合并多个聚合中间状态。
agg_result_type {agg_func_name}_merge(input_col)
```

- **agg_func_name**: 聚合函数的名称。
- **input_col**: 聚合函数的输入列。输入列类型是聚合函数的中间状态类型。您可以使用 `_state` 函数获取它。
- **agg_intermediate_type**: `_merge` 函数的返回类型，即聚合函数的最终聚合结果。

示例：

```SQL
-- 案例 1：合并聚合表的中间状态以获得最终聚合结果。
SELECT 
    dt,
    ds_hll_count_distinct_merge(hll_sketch_agg),
    avg_merge(avg_agg),
    array_agg_merge(array_agg_agg),
    min_by_merge(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- 案例 2：合并 _state 组合函数输入的中间状态以获得最终聚合结果。
SELECT 
    dt,
    ds_hll_count_distinct_merge(ds_hll_count_distinct_state(id)),
    avg_merge(avg_state(id)),
    array_agg_merge(array_agg_state(id)),
    min_by_merge(min_by_state(province, id))
FROM t1
GROUP BY dt
LIMIT 1;
```

### 在物化视图中使用通用聚合状态

通用聚合状态可以在同步和异步物化视图中使用，以通过聚合状态的汇总加速查询性能。

#### 同步物化视图中的通用聚合状态

示例：

```SQL
-- 创建一个同步物化视图 test_mv1 以存储聚合状态。
CREATE MATERIALIZED VIEW test_mv1 
AS
SELECT 
    dt,
    -- 原始聚合函数。
    min(id) AS min_id,
    max(id) AS max_id,
    sum(id) AS sum_id,
    bitmap_union(to_bitmap(id)) AS bitmap_union_id,
    hll_union(hll_hash(id)) AS hll_union_id,
    percentile_union(percentile_hash(id)) AS percentile_union_id,
    -- 通用聚合状态函数。
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- 等待汇总创建完成。
show alter table rollup;

-- 直接针对聚合函数的查询将被 test_mv1 透明加速。
SELECT 
    dt,
    min(id),
    max(id),
    sum(id),
    bitmap_union_count(to_bitmap(id)), -- count(distinct id)
    hll_union_agg(hll_hash(id)), -- approx_count_distinct(id)
    percentile_approx(id, 0.5),
    ds_hll_count_distinct(id),
    avg(id),
    array_agg(id),
    min_by(province, id)
FROM t1
WHERE dt >= '2024-01-01'
GROUP BY dt;

-- 直接针对聚合函数和汇总的查询也将被 test_mv1 透明加速。
SELECT 
    min(id),
    max(id),
    sum(id),
    bitmap_union_count(to_bitmap(id)), -- count(distinct id)
    hll_union_agg(hll_hash(id)), -- approx_count_distinct(id)
    percentile_approx(id, 0.5),
    ds_hll_count_distinct(id),
    avg(id),
    array_agg(id),
    min_by(province, id)
FROM t1
WHERE dt >= '2024-01-01';

DROP MATERIALIZED VIEW test_mv1;
```

#### 异步物化视图中的通用聚合状态

示例：

```SQL
-- 创建一个异步物化视图 test_mv2 以存储聚合状态。
CREATE MATERIALIZED VIEW test_mv2
PARTITION BY (dt)
DISTRIBUTED BY RANDOM
AS
SELECT 
    dt,
    -- 原始聚合函数。
    min(id) AS min_id,
    max(id) AS max_id,
    sum(id) AS sum_id,
    bitmap_union(to_bitmap(id)) AS bitmap_union_id,
    hll_union(hll_hash(id)) AS hll_union_id,
    percentile_union(percentile_hash(id)) AS percentile_union_id,
    -- 通用聚合状态函数。
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- 刷新物化视图。
REFRESH MATERIALIZED VIEW test_mv2 WITH SYNC MODE;

-- 直接针对聚合函数的查询将被 test_mv2 透明加速。
SELECT 
    dt,
    min(id),
    max(id),
    sum(id),
    bitmap_union_count(to_bitmap(id)), -- count(distinct id)
    hll_union_agg(hll_hash(id)), -- approx_count_distinct(id)
    percentile_approx(id, 0.5),
    ds_hll_count_distinct(id),
    avg(id),
    array_agg(id),
    min_by(province, id)
FROM t1
WHERE dt >= '2024-01-01'
GROUP BY dt;

SELECT 
    min(id),
    max(id),
    sum(id),
    bitmap_union_count(to_bitmap(id)), -- count(distinct id)
    hll_union_agg(hll_hash(id)), -- approx_count_distinct(id)
    percentile_approx(id, 0.5),
    ds_hll_count_distinct(id),
    avg(id),
    array_agg(id),
    min_by(province, id)
FROM t1
WHERE dt >= '2024-01-01';
```

## 使用注意事项

- **聚合键**：
  - 在 CREATE TABLE 语句中，聚合键必须在其他列之前定义。
  - 聚合键可以使用 `AGGREGATE KEY` 显式定义。`AGGREGATE KEY` 必须包含除值列之外的所有列，否则表创建失败。

    如果未使用 `AGGREGATE KEY` 显式定义聚合键，则默认情况下，除值列之外的所有列都被视为聚合键。
  - 聚合键具有唯一性约束。

- **值列**：通过在列名后指定聚合函数来定义一个列为值列。此列通常保存需要聚合的数据。

- **聚合函数**：用于值列的聚合函数。有关聚合表支持的聚合函数，请参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

- **排序键**

  - 自 v3.3.0 起，聚合表中的排序键与聚合键解耦。聚合表支持使用 `ORDER BY` 指定排序键，并使用 `AGGREGATE KEY` 指定聚合键。排序键和聚合键中的列需要相同，但列的顺序不需要相同。

  - 在运行查询时，排序键列在多个数据版本聚合之前被过滤，而值列在多个数据版本聚合之后被过滤。因此，我们建议您识别那些经常用作过滤条件的列，并将这些列定义为排序键。这样，数据过滤可以在多个数据版本聚合之前开始，以提高查询性能。

- 创建表时，只能在表的键列上创建 Bitmap 索引或 Bloom Filter 索引。

## 下一步

创建表后，您可以使用各种数据摄取方法将数据导入 StarRocks。有关 StarRocks 支持的数据摄取方法的信息，请参见 [Loading options](../../loading/Loading_intro.md)。

> 注意：当您将数据导入使用聚合表的表时，您只能更新表的所有列。例如，当您更新前面的 `example_db.aggregate_tbl` 表时，必须更新其所有列，即 `site_id`、`date`、`city_code` 和 `pv`。