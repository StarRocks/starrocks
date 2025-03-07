---
displayed_sidebar: docs
sidebar_position: 40
---

# Aggregate table

At table creation, you can define an aggregate key and specify an aggregate function for the value column. When multiple rows of data have the same aggregate key, the values in the value columns are aggregated. Additionally, you can define the sort key separately. If the filter conditions in queries include the sort key, StarRocks can quickly filter the data, improving query efficiency.

In data analysis and aggregation scenarios, Aggregate tables can reduce the amount of data that needs to be processed, thereby enhancing query efficiency.

## Scenarios

The Aggregate table is well suited to data statistics and analytics scenarios. A few examples are as follows:

- Help website or app providers analyze the amount of traffic and time that their users spend on a specific website or app and the total number of visits to the website or app.

- Help advertising agencies analyze the total clicks, total views, and consumption statistics of an advertisement that they provide for their customers.

- Help e-commerce companies analyze their annual trading data to identify the geographic bestsellers within individual quarters or months.

The data query and ingestion in the preceding scenarios have the following characteristics:

- Most queries are aggregate queries, such as SUM, MAX, and MIN.
- Raw detailed data does not need to be retrieved.
- Historical data is not frequently updated. Only new data is appended.

## Principle

From the phrase of data ingestion to data query, data in the Aggregate tables is aggregated multiple times as follows:

1. In the data ingestion phase, each batch of data forms a version when data is loaded into the Aggregate table in batches. In one version, data with the same aggregate key will be aggregated.

2. In the background compaction phase, when the files of multiple data versions that are generated at data ingestion are periodically compacted into a large file, StarRocks aggregates the data that has the same aggregate key in the large file.
3. In the data query phase, StarRocks aggregates the data that has the same aggregate key among all data versions before it returns the query result.

The aggregate operations help reduce the amount of data that needs to be processed, thereby accelerating queries.

Suppose that you have a table that uses the Aggregate table and want to load the following four raw records into the table.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 1    |
| 2020.05.01 | CHN     | 2    |
| 2020.05.01 | USA     | 3    |
| 2020.05.01 | USA     | 4    |

StarRocks aggregates the four raw records into the following two records at data ingestion.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 3    |
| 2020.05.01 | USA     | 7    |

## Create a table

Suppose that you want to analyze the numbers of visits by users from different cities to different web pages. In this example, create a table named `example_db.aggregate_tbl`, define `site_id`, `date`, and `city_code` as the aggregate key, define `pv` as a value column, and specify the SUM function for the `pv` column.

The statement for creating the table is as follows:

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

> **NOTICE**
>
> - When you create a table, you must specify the bucketing column by using the `DISTRIBUTED BY HASH` clause. For detailed information, see [bucketing](../data_distribution/Data_distribution.md#bucketing).
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../data_distribution/Data_distribution.md#set-the-number-of-buckets).

## Generic aggregate function states

StarRocks supports generic aggregate function states from v3.4.0.

In data analysis and summaries, Aggregate tables reduce the data processed during queries, improving query performance. For large datasets, Aggregate tables are highly effective as they summarize data by dimensions before querying. They also serve as a vital method for incremental aggregate function computation in StarRocks. However, in earlier versions, support is limited to built-in functions such as `SUM`, `MAX`, `MIN`, `REPLACE`, `HLL_UNION`, `PERCENTILE_UNION`, and `BITMAP_UNION`, while in theory, all built-in aggregate functions can be used in aggregate tables. To address this limitation, generic aggregate states is introduced to support the storage of all built-in function states.

### Store generic aggregate states

You can define generic aggregate states in Aggregate tables by specifying the function name and input parameter types to uniquely identify an aggregate function. The column type will automatically be inferred as the intermediate state type of the aggregate function.

Definition:

```SQL
col_name agg_func_name(parameter1_type, [parameter2_type], ...)
```

- **col_name**: Name of the column.
- **agg_func_name**: Name of the aggregate function whose intermediate states needs to be stored.
- **parameter_type**: Input parameter type of the aggregate function. The function can be identified uniquely with the parameter type.

:::note

- Only StarRocks built-in functions with at least one parameter are supported. Java and Python UDAFs are not supported.
- For stability and extensibility, the aggregate state column type is always Nullable (except for the count function) and cannot be modified.
- Parameter values are unnecessary for defining multi-parameter functions because types can be inferred and parameter values are not involved in computation.
- Complex parameters like ORDER BY and DISTINCT are not supported.
- The support for specific built-in functions, such as `GROUP_CONCAT`, `WINDOW_FUNNEL`, and `APPROX_TOP_K`, is still under development. They will be supported in future releases. For detailed information, see [FunctionSet.java#UNSUPPORTED_AGG_STATE_FUNCTIONS](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java#L776).

:::

Example:

```SQL
CREATE TABLE test_create_agg_table (
  dt VARCHAR(10),
  -- Define generic aggregate state storage.
  hll_sketch_agg ds_hll_count_distinct(varchar),
  avg_agg avg(bigint),
  array_agg_agg array_agg(int),
  min_by_agg min_by(varchar, bigint)
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;
```

### Combinator functions

Generic aggregate states use combinator functions to encapsulate intermediate state computation and flow.

#### `_state` combinator function

The `_state` function converts input parameters into intermediate state types.

Definition:

```SQL
agg_intermediate_type {agg_func_name}_state(input_col1, [input_col2], ...)
```

- **agg_func_name**: Name of the aggregate function that needs to transfer the input parameters into intermediate state types.
- **input_col1/col2**: Input columns of the aggregate function.
- **agg_intermediate_type**: Return type of the `_state` function, that is, the intermediate state type of the aggregate function.

:::note

`_state` is a scalar function. You do not need to define the aggregate column for the computation of input parameter state.

:::

Example:

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

-- Transfer the data in t1 with _state combinator function, and insert it into the Aggregate table.
INSERT INTO test_create_agg_table
SELECT
    dt,
    ds_hll_count_distinct_state(id),
    avg_state(id),
    array_agg_state(id),
    min_by_state(province, id)
FROM t1;
```

#### `_union` combinator function

The `_union` function merges multiple intermediate state columns into a single state.

Definition:

```SQL
-- Union multiple aggregate intermediate states.
agg_intermediate_type {agg_func_name}_union(input_col)
```

- **agg_func_name**: Name of the aggregate function.
- **input_col**: Input columns of the aggregate function. The input column type is the intermediate state type of the aggregate function. You can obtain it using `_state` functions.
- **agg_intermediate_type**: Return type of the `_union` function, that is, the intermediate state type of the aggregate function.

:::note

`_union` is an aggregate function. It returns the intermediate state type instead of the type of function's final result.

:::

Example:

```SQL
-- Case 1: Union the intermediate states of the Aggregate table.
SELECT 
    dt,
    ds_hll_count_distinct_union(hll_sketch_agg),
    avg_union(avg_agg),
    array_agg_union(array_agg_agg),
    min_by_union(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- Case 2: Union the intermediate states input by the _state combinator function.
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

#### `_merge` combinator function

The `_merge` combinator function encapsulates aggregate functions as a generic aggregate function to calculate the final aggregation result of multiple intermediate states.

Definition:

```SQL
-- Merge multiple aggregate intermediate states.
agg_result_type {agg_func_name}_merge(input_col)
```

- **agg_func_name**: Name of the aggregate function.
- **input_col**: Input columns of the aggregate function. The input column type is the intermediate state type of the aggregate function. You can obtain it using `_state` functions.
- **agg_intermediate_type**: Return type of the `_merge` function, that is, the final aggregation result of the aggregate function.

Example:

```SQL
-- Case 1: Merge the intermediate states of the Aggregate table to obtain the final aggregation result.
SELECT 
    dt,
    ds_hll_count_distinct_merge(hll_sketch_agg),
    avg_merge(avg_agg),
    array_agg_merge(array_agg_agg),
    min_by_merge(min_by_agg)
FROM test_create_agg_table
GROUP BY dt
LIMIT 1;

-- Case 2:  Merge the intermediate states input by the _state combinator function to obtain the final aggregation result.
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

### Use generic aggregate states in materialized views

Generic aggregate states can be used in synchronous and asynchronous materialized views to accelerate query performance with rollup of aggregate states.

#### Generic aggregate states in synchronous materialized views

Example:

```SQL
-- Create a synchronous materialized view test_mv1 to store aggregate states.
CREATE MATERIALIZED VIEW test_mv1 
AS
SELECT 
    dt,
    -- Original aggregate functions.
    min(id) AS min_id,
    max(id) AS max_id,
    sum(id) AS sum_id,
    bitmap_union(to_bitmap(id)) AS bitmap_union_id,
    hll_union(hll_hash(id)) AS hll_union_id,
    percentile_union(percentile_hash(id)) AS percentile_union_id,
    -- Generic aggregate state functions.
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- Wait until rollup creation finishes.
show alter table rollup;

-- Direct queries against the aggregate function will be transparently accelerated by test_mv1.
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

-- Direct queries against the aggregate function and the rollup will also be transparently accelerated by test_mv1.
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

#### Generic aggregate states in asynchronous materialized views

Example:

```SQL
-- Create an asynchronous materialized view test_mv2 to store aggregate states.
CREATE MATERIALIZED VIEW test_mv2
PARTITION BY (dt)
DISTRIBUTED BY RANDOM
AS
SELECT 
    dt,
    -- Original aggregate functions.
    min(id) AS min_id,
    max(id) AS max_id,
    sum(id) AS sum_id,
    bitmap_union(to_bitmap(id)) AS bitmap_union_id,
    hll_union(hll_hash(id)) AS hll_union_id,
    percentile_union(percentile_hash(id)) AS percentile_union_id,
    -- Generic aggregate state functions.
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;

-- Refresh the materialized view.
REFRESH MATERIALIZED VIEW test_mv2 WITH SYNC MODE;

-- Direct queries against the aggregate function will be transparently accelerated by test_mv2.
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

## Usage notes

- **Aggregate key**:
  - In the CREATE TABLE statement, the aggregate key must be defined before other columns.
  - The aggregate key can be explicitly defined using `AGGREGATE KEY`. The `AGGREGATE KEY` must include all columns except the value columns, otherwise the table fails to be created.

    If the aggregate key is not explicitly defined using `AGGREGATE KEY`, all columns except the value columns are considered as the aggregate key by default.
  - The aggregate key has uniqueness constraint.

- **Value column**: Define a column as the value column by specifying an aggregate function after the column name. This column generally holds data that needs to be aggregated.

- **Aggregate function**: The aggregate function used for the value column. For supported aggregate functions for the Aggregate tables, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

- **Sort key**

  - Since v3.3.0, the sort key is decoupled from the aggregate key in the Aggregate table. The Aggregate table supports specifying the sort key using `ORDER BY` and specifying the aggregate key using `AGGREGATE KEY`. The columns in the sort key and the aggregate key need to be the same, but the order of the columns does not need to be the same.

  - When queries are run, sort key columns are filtered before the aggregation of multiple data versions, whereas value columns are filtered after the aggregation of multiple data versions. Therefore, we recommend that you identify the columns that are frequently used as filter conditions and define these columns as the sort key. This way, data filtering can start before the aggregation of multiple data versions to improve query performance.

- When you create a table, you can only create Bitmap indexes or Bloom Filter indexes on the key columns of the table.

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Loading options](../../loading/Loading_intro.md).

> Note: When you load data into a table that uses the Aggregate table, you can only update all columns of the table. For example, when you update the preceding `example_db.aggregate_tbl` table, you must update all its columns, which are `site_id`, `date`, `city_code`, and `pv`.
