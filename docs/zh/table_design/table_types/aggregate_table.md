---
displayed_sidebar: docs
sidebar_position: 40
keywords: ['juhe']
---

# 聚合表

建表时可以定义聚合键并且为 value 列指定聚合函数。当多条数据具有相同的聚合键时，value 列会进行聚合。并且支持单独定义排序键，如果查询的过滤条件包含排序键，则 StarRocks 能够快速地过滤数据，提高查询效率。

在分析统计和汇总数据时，聚合表能够减少查询时所需要处理的数据，提升查询效率。

## 适用场景

适用于分析统计和汇总数据。比如:

- 通过分析网站或 APP 的访问流量，统计用户的访问总时长、访问总次数。

- 广告厂商为广告主提供的广告点击总量、展示总量、消费统计等。

- 通过分析电商的全年交易数据，获得指定季度或者月份中，各类消费人群的爆款商品。

在这些场景中，数据查询和导入，具有以下特点：

- 多为汇总类查询，比如 SUM、MAX、MIN等类型的查询。

- 不需要查询原始的明细数据。

- 旧数据更新不频繁，只会追加新的数据。

## 原理

从数据导入至数据查询阶段，聚合表内部同一聚合键的数据会多次聚合，聚合的具体时机和机制如下：

1. 数据导入阶段：数据按批次导入至聚合表时，每一个批次的数据形成一个版本。在一个版本中，同一聚合键的数据会进行一次聚合。

2. 后台文件合并阶段 (Compaction) ：数据分批次多次导入至聚合表中，会生成多个版本的文件，多个版本的文件定期合并成一个大版本文件时，同一聚合键的数据会进行一次聚合。

3. 查询阶段：所有版本中同一聚合键的数据进行聚合，然后返回查询结果。

因此，聚合表中数据多次聚合，能够减少查询时所需要的处理的数据量，进而提升查询的效率。

例如，导入如下数据至聚合表中，聚合键为 Date、Country：

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 1    |
| 2020.05.01 | CHN     | 2    |
| 2020.05.01 | USA     | 3    |
| 2020.05.01 | USA     | 4    |

在聚合表中，以上四条数据会聚合为两条数据。这样在后续查询处理的时候，处理的数据量就会显著降低。

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 3    |
| 2020.05.01 | USA     | 7    |

## 创建表

例如需要分析某一段时间内，来自不同城市的用户，访问不同网页的总次数。则可以将网页地址 `site_id`、日期 `date` 和城市代码 `city_code` 作为聚合键，将访问次数 `pv` 作为 value 列，并为 value 列 `pv` 指定聚合函数为 SUM。

在该业务场景下，建表语句如下：

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
> - 建表时必须使用 `DISTRIBUTED BY HASH` 子句指定分桶键。分桶键的更多说明，请参见[分桶](../data_distribution/Data_distribution.md#分桶)。
> - 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [设置分桶数量](../data_distribution/Data_distribution.md#设置分桶数量)。

## 通用聚合函数状态

StarRocks 自 v3.4.0 起支持通用聚合函数状态。

在进行数据分析、统计和汇总时，聚合表能够有效减少查询时需要处理的数据量，从而提升查询效率。对于海量明细数据，通过聚合模型先进行维度汇总再查询，能极大提高性能。同时，聚合模型是当前 StarRocks 实现聚合函数增量计算的关键手段。然而，目前系统仅内置支持 `SUM`、`MAX`、`MIN`、`REPLACE`、`HLL_UNION`、`PERCENTILE_UNION`、`BITMAP_UNION ` 等有限的聚合函数类型。实际上，理论上所有内置聚合函数都可以作为聚合模型列使用。因此，StarRocks 通过引入通用聚合状态，实现对所有内置函数状态存储的支持。

### 存储通用聚合状态

您可在聚合表中使用通用聚合函数定义需要存储的通用聚合状态。定义时，需指定聚合函数名称及输入参数类型，即可唯一地确定一个聚合函数，用于后台的合并和再聚合操作。定义的列类型为聚合函数的中间状态类型，系统会自动推导列类型。

定义：

```SQL
col_name agg_func_name(parameter1_type, [parameter2_type], ...)
```

- **col_name**：列的名称。
- **agg_func_name**：需要存储中间状态的聚合函数名称。
- **parameter_type**：聚合函数的输入参数类型，用以唯一标识该函数。

:::note

- 仅支持 StarRocks 内置聚合函数，且聚合函数需至少有一个参数，不支持 Java 和 Python UDAF。
- 为保证稳定性和扩展性，通用聚合函数状态列默认均为 Nullable 类型（count 聚合函数除外），无法手动修改。
- 定义多参数聚合函数时，参数值无需明确，系统仅根据类型推导，而非具体值。
- 当前不支持 ORDER BY、DISTINCT 等复杂参数。
- 部分内置聚合函数，例如 `GROUP_CONCAT`、`WINDOW_FUNNEL`、`APPROX_TOP_K` 等，其通用聚合状态支持的开发工作仍在进行中，并且将在后续版本中支持。详情参考 [FunctionSet.java#UNSUPPORTED_AGG_STATE_FUNCTIONS](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java#L776)。

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

### Combinator 函数

通用聚合状态支持通过 Combinator 函数封装中间状态的计算与流转。

#### `_state` Combinator 函数

`_state` Combinator 函数将输入参数转换为聚合函数的中间状态类型。

定义：

```SQL
agg_intermediate_type {agg_func_name}_state(input_col1, [input_col2], ...)
```

- **agg_func_name**：聚合函数名称。
- **input_col1/col2**：聚合函数的输入列。
- **agg_intermediate_type**：`_state` Combinator 函数的返回值，即聚合函数的中间状态类型。

:::note

`_state` Combinator 函数为标量函数，不需要指定聚合列即可计算输入值的状态结果。

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

-- 将 t1 表中的数据通过 _state Combinator 函数转换后写入到聚合表。
INSERT INTO test_create_agg_table
SELECT
    dt,
    ds_hll_count_distinct_state(id),
    avg_state(id),
    array_agg_state(id),
    min_by_state(province, id)
FROM t1;
```

#### `_union` Combinator 函数

`_union` Combinator 函数用于将多个中间状态列再聚合为一个中间状态。

定义：

```SQL
-- 聚合多个聚合中间状态。
agg_intermediate_type {agg_func_name}_union(input_col)
```

- **agg_func_name**：聚合函数名称。
- **input_col**：输入列，中间状态类型。您可以通过 `_state` Combinator 函数获取。
- **agg_intermediate_type**：`_union` Combinator 函数的返回值，即聚合函数的中间状态类型。

:::note

`_union` Combinator 函数为聚合函数，其返回的依然是中间聚合状态而不是该函数的最终计算结果类型。

:::

示例：

```SQL
-- Case 1: 将聚合表的中间状态再聚合。
SELECT 
    dt,
    ds_hll_count_distinct_union(hll_sketch_agg),
    avg_union(avg_agg),
    array_agg_union(array_agg_agg),
    min_by_union(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- Case 2: 将 _state Combinator 函数输出的中间状态再聚合。
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

#### `_merge` Combinator 函数

`_merge` Combinator 函数将聚合函数封装为一个通用的聚合函数结果，将输入的多个中间状态列再聚合并计算出对应的最终结果。

Definition:

```SQL
-- 将多个中间状态封装并计算最终结果。
agg_result_type {agg_func_name}_merge(input_col)
```

- **agg_func_name**：聚合函数名称。
- **input_col**：输入列，中间状态类型。您可以通过 `_state` Combinator 函数获取。
- **agg_intermediate_type**：`_merge` Combinator 函数的返回值，即聚合函数的最终计算类型。

Example:

```SQL
-- Case 1: 将聚合表的中间状态再聚合并求出最终聚合结果。
SELECT 
    dt,
    ds_hll_count_distinct_merge(hll_sketch_agg),
    avg_merge(avg_agg),
    array_agg_merge(array_agg_agg),
    min_by_merge(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- Case 2: 将 _state Combinator 函数输出的中间状态再聚合并求出最终聚合结果。
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

### 在物化视图中使用通用聚合函数状态

您可以在同步和异步物化视图中使用通用聚合函数状态，以便于利用聚合状态进行上卷聚合，加速查询性能。

#### 在同步物化视图中使用通用聚合函数状态

示例：

```SQL
-- 创建同步物化视图 test_mv1，保存聚合函数的中间状态。
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
    -- 通用聚合模型函数。
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- 等待 Rollup 创建完成。
SHOW ALTER TABLE ROLLUP;

-- 直接查询聚合函数，系统会自动地基于 test_mv1 进行透明加速。
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

-- 直接查询聚合函数以及上卷操作，也可以自动地基于 test_mv1 进行透明加速。
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

#### 在异步物化视图中使用通用聚合函数状态

示例：

```SQL
-- 创建异步物化视图 test_mv2，保存聚合函数的中间状态。
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
    -- 通用聚合模型函数。
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- 刷新物化视图。
REFRESH MATERIALIZED VIEW test_mv2 WITH SYNC MODE;

-- 直接查询聚合函数会自动地基于 test_mv2 进行透明加速
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

## 使用说明

- **聚合键**：
  - 在建表语句中，聚合键必须定义在其他列之前。
  - 聚合键可以通过 `AGGREGATE KEY` 显式定义。并且 `AGGREGATE KEY` 必须包含除 value 列之外的所有列，则建表会失败。

    如果不通过 `AGGREGATE KEY` 显示定义聚合键，则默认除 value 列之外的列均为聚合键。
  - 聚合键具有唯一性约束。

- **value 列**：通过在列名后指定聚合函数，定义该列为 value 列。一般为需要汇总统计的数据。

- **聚合函数**：value 列使用的聚合函数。聚合表支持的聚合函数，请参见 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

- **排序键**：
  - 自 v3.3.0 起，聚合表解耦了排序键和聚合键。聚合表支持使用 `ORDER BY` 指定排序键和使用 `AGGREGATE KEY` 指定聚合键。排序键和聚合键中的列需要保持一致，但是列的顺序不需要保持一致。

  - 查询时，排序键在多版聚合之前就能进行过滤，而 value 列的过滤在多版本聚合之后。因此建议将频繁使用的过滤字段作为排序键，在聚合前就能过滤数据，从而提升查询性能。

- 建表时，仅支持为 key 列创建 Bitmap 索引、Bloom filter 索引。

## 下一步

建表完成后，您可以创建多种导入作业，导入数据至表中。具体导入方式，请参见[导入方案](../../loading/Loading_intro.md)。

> 导入时，仅支持全字段导入，即导入任务需要涵盖表的所有列，例如示例中的 `site_id`、`date`、`city_code` 和 `pv` 四个列。
