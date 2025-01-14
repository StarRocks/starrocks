# Automatic Materialized View Recommendation

This topic describes how to leverage the Automatic Materialized View Recommendation feature to generate schemas for materialized views that can be used to accelerate the queries in your business scenarios.

## Overview

StarRocks' asynchronous materialized views use a widely adopted transparent query rewrite algorithm based on the SPJG (select-project-join-group-by) form. They can help you significantly reduce computational costs, and substantially accelerate query execution. However, materialized views are difficult to design, and users need to spend a lot of effort to design materialized views suitable for query rewrite and acceleration. In addition, they will have to re-design the materialized views whenever their business scenarios change, leading to a huge cost of time and labor again.

To address these issues, the StarRocks Enterprise Edition introduced the Automatic Materialized View Recommendation feature in v3.3.2. Simply by adding the query statements of your business scenarios to a Tunespace, you can effortlessly get the schemas for some cost-efficient materialized views that can rewrite and accelerate these queries. StarRocks automatically recommends materialized views by parsing the queries, abstracting their structures and SPJG patterns available for query rewrite, and calculating suitable materialized view schemas based on the information. These structures, patterns, and other metadata of the queries form a "Tunespace". You can also add legacy materialized views to a Tunespace. StarRocks will parse the materialized views, and recommend them when they can accelerate queries in the Tunespace, or recommend the schema when some of them can be merged into one materialized view. Merging materialized views can reduce the number of materialized views and thereby the cost of refreshing and storing them. Whenever you add or delete records in the Tunespace, StarRocks will automatically update the recommendations, disentangling you from the trouble of re-designing the legacy materialized views.

From v3.4.0 onwards, StarRocks optimized the partitioning strategy of the recommended materialized views from the following perspectives:

- Partitioned materialized views can be recommended when the partitioning column is the string or integer type.
- When the base table is a list-partitioned **internal** table, StarRocks can recommend list-partitioned materialized views; when the base table is a list-partitioned **external** table, StarRocks can recommend list-partitioned or range-partitioned materialized views.
- When the base table is an external table, and its partitioning column is a string type whose values are dates (for example, `2024-01-01`), StarRocks can transform the partitioning column into the DATE type, and recommend range-partitioned materialized views by default. You can configure StarRocks to recommend list-partitioned materialized views in this scenario.

## Usage

### Step 1. Create a Tunespace

Syntax:

```SQL
CREATE TUNESPACE [IF NOT EXISTS] <tunespace_name>
```

`tunespace_name`: Name of the Tunespace to be created.

Example:

```SQL
CREATE TUNESPACE test_ts;
```

> **NOTE**
>
> A Tunespace is essentially a regular table that allows SQL operations.

### Step 2. Append query to Tunespace

After a query is added to the Tunespace, StarRocks will parse the query statement, and record the SPJG structure and metadata of the query in the Tunespace. You can optionally assign each query a name, which will be used to represent the query that can be accelerated by the recommended materialized view.

Syntax:

```SQL
ALTER TUNESPACE <tunespace_name> APPEND ["query_name"] <query_statement>
```

- `tunespace_name`: Name of the Tunespace.
- `query_name`: (Optional) Name of the query to be appended. The query name must be enclosed with double quotes.
- `query_statement`: The query to be appended.

Example:

```SQL
-- Append a query to test_ts and specify the query name as Q1.1.
ALTER TUNESPACE test_ts APPEND "Q1.1" 
SELECT SUM(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25;


-- Append a query to test_ts without specifying its name.
ALTER TUNESPACE test_ts APPEND 
SELECT SUM(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25;
```

### Step 3. Populate Tunespace with legacy materialized views

After a legacy materialized view is added to the Tunespace, StarRocks will also parse it and record its structure and metadata in the Tunespace. The legacy materialized views will be recommended if they can accelerate any queries recorded in the Tunespace, or StarRocks will recommend schemas for materialized views that merge the legacy materialized views when they are eligible. For detailed information on how materialized views are merged, see Materialized view merging.

You can populate the Tunespace with legacy materialized views under a database or from another Tunespace.

Syntax:

```SQL
-- Populate the Tunespace with legacy materialized views under a database.
ALTER TUNESPACE <tunespace_name> POPULATE FROM DATABASE <database_name>

-- Populate the Tunespace with legacy materialized views from another Tunespace.
ALTER TUNESPACE <tunespace_name> POPULATE FROM TUNESPACE <source_tunespace_name>
```

- `tunespace_name`: Name of the Tunespace.
- `database_name`: Name of the database whose materialized views are used to populate the Tunespace.
- `source_tunespace_name`: Name of the source Tunespace whose materialized views are used to populate the current Tunespace.

Example:

```SQL
-- Populate the Tunespace with legacy materialized views under the database ssb.
ALTER TUNESPACE test_ts POPULATE FROM DATABASE ssb;

-- Populate the Tunespace with legacy materialized views from another Tunespace src_ts.
ALTER TUNESPACE test_ts POPULATE FROM TUNESPACE src_ts;
```

### Step 4. Show materialized view recommendations

After the queries and legacy materialized views are added to the Tunespace, StarRocks will automatically calculate the schemas for the recommended materialized views. You can view the recommended materialized view schemas by executing the SHOW RECOMMENDATIONS statement.

Syntax:

```SQL
SHOW RECOMMENDATIONS FROM <tunespace_name> [LIMIT <INT>] [OFFSET <INT>]
```

`tunespace_name`: Name of the Tunespace.

Example:

```SQL
SHOW RECOMMENDATIONS FROM test_ts LIMIT 1\G
```

Return:

```SQL
*************************** 1. row ***************************
                   Id: 0
                 Name: _mv_20240813T150836_10da16e0ab28a28e580f570b7efef451567ecd6b
        RecommendedMV: CREATE MATERIALIZED VIEW _mv_20240813T150836_10da16e0ab28a28e580f570b7efef451567ecd6b (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "session.enable_spill" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "3"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(sum(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
           HaltReason: OVERALL
            TimeUsage: 3057
        SamplingRatio: 1.0
            CalcSteps: 8
          CardQuality: EXCELLENT
             RowCount: 6001215
          Cardinality: 222697
    CardRowCountRatio: 0.03710865216460334
              Benefit: 5778518.0
NumQueriesAccelerated: 2
         TotalBenefit: 1.1557036E7
   AcceleratedQueries: ["Q1.1", "_mv_20240813T144425_10da16e0ab28a28e580f570b7efef451567ecd6b"]
```

- `Id`: ID of the materialized view recommendation.
- `Name`: Name of the materialized view recommendation.
- `RecommendedMV`: Schema of the recommended materialized view.
- `HaltReason`: The reason that the multi-column joint sampling algorithm (materialized view dimension column cardinality estimation algorithm) halts, including:
  - ERROR: The algorithm encounters an error.
  - TIMEOUT: The algorithm execution timeout.
  - REACH_LIMIT: The algorithm execution reaches the maximum steps (`automv_calculate_steps`).
  - CONVERGENT: The algorithm is convergent (The relative error of the estimation between two consequent query samples does not exceed `automv_relative_error_bound`).
  - OVERALL: The full sampling of the algorithm is completed.
- `TimeUsage`: The time usage of the multi-column joint sampling algorithm.
- `SamplingRatio`: The maximum sampling ratio of the multi-column joint sampling algorithm before it halts.
- `CalcSteps`: The progressive sampling steps of the multi-column joint sampling algorithm before it halts.
- `CardQuality`: The quality of the cardinality estimation obtained via the multi-column joint sampling algorithm, including:
  - EXCELLENT
  - GOOD
  - PASS
  - FAIL
- `RowCount`: The number of input rows to be processed by aggregation calculation without materialized view acceleration. If aggregate queries are seen as aggregation calculations over a logical flat table, RowCount reflects the row count of the table. This field is one of the results of the multi-column joint sampling algorithm.
- `Cardinality`: The number of rows read from the materialized view with materialized view acceleration enabled. This field is one of the results of the multi-column joint sampling algorithm.
- `CardRowCountRatio`: It is equivalent to `Cardinality`/`RowCount`.
- `Benefit`: The benefit score of the materialized view.
- `NumQueriesAccelerated`: The number of queries can be accelerated by the materialized view.
- `TotalBenefit`: It is equivalent to `Benefit` * `NumQueriesAccelerated`.
- `AcceleratedQueries`: The name of the query that can be accelerated by the recommended materialized view, or the name of the materialized view that can be merged.

### Manage Tunespace

#### Delete records from Tunespace

You can delete the records of queries or materialized views from a Tunespace by their IDs.

Syntax:

```SQL
ALTER TUNESPACE <tunespace_name> DELETE WHERE id <operator> { id | id_list }
```

- `tunespace_name`: Name of the Tunespace.
- `operator`: The operator used in the DELETE condition. The supported operators are `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, and `NOT IN`.

Example:

```SQL
ALTER TUNESPACE test_ts DELETE WHERE id = 1;
```

#### Truncate Tunespace

Syntax:

```SQL
TRUNCATE TUNESPACE <tunespace_name>
```

`tunespace_name`: Name of the Tunespace.

Example:

```SQL
TRUNCATE TUNESPACE test_ts;
```

#### Drop Tunespace

Syntax:

```SQL
DROP TUNESPACE [IF EXISTS] <tunespace_name>
```

`tunespace_name`: Name of the Tunespace.

Example:

```SQL
DROP TUNESPACE test_ts;
```

## Materialized view merging

When StarRocks recommends materialized views for queries in a Tunespace, it treats each query as an aggregate query on a logical flat table. Different materialized views with aggregate queries on the same logical flat table can be merged into one materialized view. StarRocks will automatically parse the structure of the aggregate queries and infer the logical flat table, meaning the table is completely virtual and will not be created or recorded in the metadata.

StarRocks merges materialized views in two different ways: Consolidation and Covering.

- Consolidation is used when two materialized views share the same dimension column. The dimension column of the materialized view after merging is the dimension column of the original materialized views, and the metric column is the union of the metric columns of the two materialized views.

  -  In the following example, Q3 is the consolidation of Q1 and Q2:

  - ```SQL
    -- Q1
    select d0, d1, d2, sum(v0), count(v1)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d0,d1,d2;
    
    -- Q2
    select d0, d1, d2, max(v2), min(v3)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d0,d1,d2;
    
    -- Q3
    select d0, d1, d2,sum(v0), count(v1)，max(v2), min(v3)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d0,d1,d2;
    ```

- Covering is used when the dimension columns of two materialized views are different. The dimension column of the materialized view after merging is the union of the dimension columns of the original materialized views, and so does the metric column.

  -  In the following example, Q6 is the covering of Q4 and Q5:

  - ```SQL
    -- Q4
    select d0, d1, sum(v0), count(v1)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d0,d1
    
    -- Q5
    select d1, d2, max(v2), min(v3)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d1,d2;
    
    -- Q6
    select d0, d1, d2,sum(v0), count(v1)，max(v2), min(v3)
    from t0 inner join t1 on t0.c0 = t1.c1
    group by d0,d1,d2;
    ```

### Comparison of two merging methods

| Method        | Consolidation                                                | Covering                                                     |
| :------------ | :----------------------------------------------------------- | :----------------------------------------------------------- |
| Condition     | The legacy materialized views can be merged as long as they have the same dimension column. | The increase in the number of dimension columns after merging may lead to an increase in the number of rows in the recommended materialized view. Therefore, merging will be performed only when the cardinality ratios of the merged materialized view to the legacy materialized views (`ndv(d0,d1,d2)/ndv(d0,d1)` and `ndv(d0,d1,d2)/ndv(d1,d2)`) are close to 1.If the cardinality ratio of the materialized view to the logical flat table is greater than 0.5, the materialized view is not recommended. |
| On-off        | Constantly on                                                | You must set the session variable `automv_use_cardinality_estimation` to `true` to enable Covering. |
| Motivation    | To reduce the number of materialized views and the cost of refreshing them | Theoretically, both the legacy materialized views and the merged one can accelerate the query. However, extra rollup operations are needed when the merged materialized view is used to rewrite the query. Its acceleration effect will not be better than that of the legacy ones. However, merging can still reduce the number of materialized views and the cost to refresh and store them. |
| Good cases    | All cases are good cases.                                    | There are two types of good cases for covering.<br />Suppose there are two legacy materialized views: `mv0` (with dimension columns `d0`, `d1`, and `d2`) and `mv1` (with dimension columns `d0`, `d1`, and `d3`). They are merged into `mv` (with dimension columns `d0`, `d1`, `d2`, and `d3`).If the cardinality of `mv` after merging is equivalent to that of the pre-merged `mv0` and `mv1`, Covering is the most efficient.It is still a good case if the cardinality of `mv` after merging is very close to that of the pre-merged `mv0` and `mv1`, although not as good as the previous one. For example, when `ndv(d0,d1,d2,d3)/ndv(d0,d1,d2)` is less than or equal to 101%, and `ndv(d0,d1,d2,d3)/ndv(d0,d1,d3)` is less than or equal to 101%, the materialized view after covering is almost the same as the pre-merged ones, and the number of materialized views can be greatly reduced. |
| Special cases | Supports COUNT(DISTINCT)                                     | Rollup for aggregate functions is not supported. For example, COUNT(DISTINCT) is not supported for Covering.In certain cases, COUNT(DISTINCT) can be converted into bitmap, array_agg, or HLL deduplication. These cases support Covering because rollup is applicable.Covering supports aggregate functions that can be converted into those supports rollup. For example, avg can be converted into sum and count, and both sum and count support rollup. |

### Merging-related session variables

#### Multi-column joint sampling algorithm

##### automv_use_cardinality_estimation

- **Default**: true
- **Description**: Whether to enable multi-column cardinality estimation for automatic materialized view recommendation. Valid value:
  - `true`: Enable cardinality estimation for automatic materialized view recommendation. Materialized view Covering is enabled only when cardinality estimation is enabled.
  - `false`: Disable cardinality estimation and thereby Materialized view Covering.
  -  When cardinality estimation is enabled, materialized view merging is based on multi-column cardinality estimation. The recommendations are listed in descending order of the estimated costs. The order may be inaccurate if this variable is set to `false`. Please note that the calculation of multi-column cardinality estimation might be time-consuming or inaccurate. You can disable it whenever necessary.
- **Introduced in**: v3.3.2

##### automv_relative_error_bound

- **Default**: 0.05
- **Description**: During multi-column cardinality estimation, a series of queries are sampled. The data range of subsequent query samples is larger than that of previous ones. Sampling stops when the relative error between two consecutive samples is less than this value.
- **Introduced in**: v3.3.2

##### automv_sampling_timeout

- **Default**: 300000
- **Unit**: Milliseconds
- **Description**: The timeout duration of sampling for multi-column cardinality estimation on the original materialized views of the same logical flat table.
- **Introduced in**: v3.3.2

##### automv_calculate_steps

- **Default**: 2147483647
- **Description**: The maximum steps of the greedy algorithm for materialized view recommendation. The greedy algorithm complexity for materialized view recommendation is O(n³), which leads to significant time consumption. When there are a large number of candidate materialized views from the same logical flat table, the algorithm only executes up to `automv_calculate_steps` steps, and recommends the top `automv_calculate_steps` materialized views. This parameter should be used with the LIMIT clause in the `SHOW RECOMMENDATIONS` statement. Without the LIMIT clause, this variable will not take effect.
- **Introduced in**: v3.3.2

##### automv_sampling_ratio_low_bound

- **Default**: 0.01
- **Description**: Controls the algorithm convergence and quality of cardinality estimation. If the sampling rate does not reach this value, the algorithm does not converge, and the quality of cardinality estimation results is considered inadequate.
- **Introduced in**: v3.3.2

##### automv_min_sampling_rows

- **Default**: 1073741824
- **Description**: Controls the algorithm convergence and quality of cardinality estimation. If the number of sampled rows does not reach this value, the algorithm does not converge, and the quality of cardinality estimation results is considered inadequate.
- **Introduced in**: v3.3.2

##### automv_sampling_buckets

- **Default**: 512
- **Description**: The number of buckets for each column during multi-column joint cardinality estimation. The number of buckets increases progressively with the sampling SQL. Fewer buckets require fewer steps for algorithm convergence, and vice versa.
- **Introduced in**: v3.3.2

#### Materialized view merging and pruning

##### automv_card_rowcount_ratio_lwm

- **Default**: 0.05
- **Description**: The low watermark of `card_rowcount_ratio`, which is the cardinality ratio of materialized view to the logical flat table.
  - If `card_rowcount_ratio` is less than `automv_card_rowcount_ratio_lwm`, the materialized view is considered low cost and is directly added to the recommendation list.
  - If `card_rowcount_ratio` is greater than `automv_card_rowcount_ratio_hwm`, the materialized view is considered high cost and discarded.
  - If `card_rowcount_ratio` is between `automv_card_rowcount_ratio_lwm` and `automv_card_rowcount_ratio_hwm`, a greedy algorithm determines whether to recommend or discard the materialized view.
- **Introduced in**: v3.3.2

##### automv_card_rowcount_ratio_hwm

- **Default**: 0.5
- **Description**: The high watermark of `card_rowcount_ratio`. See `automv_card_rowcount_ratio_lwm` for detailed information.
- **Introduced in**: v3.3.2

##### automv_prune_rollup_unable_aggregate_with_conjuncts

- **Default**: true
- **Description**: In aggregate queries with non-rollable aggregate functions and WHERE predicates, the columns in the WHERE predicates cannot be included in the candidate materialized view's dimension columns. The predicates can only be included in the materialized view's WHERE clause. Generally, such materialized views have poor generalization ability and are not recommended by default. Enabling this session variable might recommend such materialized views.
- **Introduced in**: v3.3.2

#### Materialized view schema generation

##### automv_partial_rollup_min_agg_pieces

- **Default**: 3
- **Description**: In T+1 scenarios, users may only need to build materialized views for recent data. When the partition predicate can be converted to a range predicate, the partitioning column is not added to the dimension columns. Instead, the partition predicate is placed in the materialized view. This optimization is applied only when the number of queries with partition predicates that contain the same partition column is greater than or equal to `automv_partial_rollup_min_agg_pieces`. The partition predicates do not need to be identical. StarRocks will calculate an inclusive range. For example, the following are partition predicates of three queries:
  -  Q1: `dt > '2022-06-01'`
  - Q2: `dt > '2022-05-31'`
  - Q3: `dt in ('2022-05-28', '2022-07-01')`
  -  Then the partition predicate of the materialized view is `dt > '2022-05-28'`. Currently, only simple predicates containing partition columns are supported. Predicates with time functions like `date_trunc('month', dt) > '2022-07-01'` are not supported.
- **Introduced in**: v3.3.2

##### automv_max_order_by_columns

- **Default**: 3
- **Description**: The maximum number of dimension columns included in the materialized view's short key. Dimension columns used as filtering conditions are selected as short-key columns. If a date column is used as the base table's partition column, the date column is placed first in the short key.
- **Introduced in**: v3.3.2

#### Rollup conversion for deduplication

##### automv_use_array_agg_count_distinct

- **Default**: false
- **Description**: Whether to use array_agg deduplication to replace COUNT(DISTINCT).
- **Introduced in**: v3.3.2

##### automv_use_bitmap_count_distinct

- **Default**: false
- **Description**: Whether to use bitmap deduplication to replace COUNT(DISTINCT). Only TINYINT, SMALLINT, INT, and BIGINT types supported bitmap deduplication. Users must ensure that the values in the deduplication column are non-negative. Otherwise, the result will be incorrect.
- **Introduced in**: v3.3.2

##### automv_use_hll_count_distinct

- **Default**: false
- **Description**: Whether to use HLL for approximate COUNT(DISTINCT).
- **Introduced in**: v3.3.2

#### Partitioning strategy

##### automv_default_partition_by_time_granule

- **Default**: day
- **Description**: When the materialized view has a dimension column that can be used for partitioning, this column is preferentially used as the partitioning column. If the materialized view dimension column cannot be used as the partition column and all metric columns in the materialized view can be rewritten for rollup, the recommendation algorithm will try to add a base table partition column to the materialized view dimension columns as the default partition column. This parameter is used to set the default partitioning granularity of the materialized view. Valid values are `year`, `quarter`, `month`, `day`, `hour`, and `none` (which indicates disabling the default partitioning column supplement logic). Currently, only range partitioning is supported. List partitioning is not supported.
- **Introduced in**: v3.3.2

##### automv_prefer_range_partition

- **Default**: true
- **Description**: Whether to recommend range-partitioned materialized views preferentially when the base table is an external table, and its partitioning column is a string type whose values are dates (for example, `2024-01-01`). When this variable is set to `true`, StarRocks recommends range-partitioned materialized views preferentially. When this variable is set to `false`, StarRocks recommends list-partitioned materialized views.
- **Introduced in**: v3.4.0

##### automv_string_time_formats

- **Default**: %Y%m%d,%Y-%m-%d
- **Description**: The date formats of the external table partitioning column if the column is a string type whose values are dates. Multiple values are separated by commas (`,`). StarRocks can transform the partitioning column into the DATE type based on any of the formats listed here, and recommend range-partitioned materialized views. If no format is matched, the recommended materialized view will use list partitioning.
- **Introduced in**: v3.4.0

## Examples

#### Example 1. Add queries to a Tunespace and specify names for the queries

Add three queries to Tunespace `ts1` and specify the query names as `Q1.1`, `Q1.2`, and `Q1.3`.

```sql
CREATE TUNESPACE ts1;

ALTER TUNESPACE ts1 APPEND "Q1.1" SELECT sum(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25;

ALTER TUNESPACE ts1 APPEND "Q1.2" SELECT sum(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_yearmonthnum = 199401
AND lo_discount BETWEEN 4 and 6
AND lo_quantity BETWEEN 26 and 35;

ALTER TUNESPACE ts1 APPEND "Q1.3" SELECT sum(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_weeknuminyear = 6 
AND d_year = 1994
AND lo_discount BETWEEN 5 AND 7
AND lo_quantity BETWEEN 26 AND 35;
```

Show the top one recommendation of `ts1`. The `AcceleratedQueries` field shows that it can accelerate `Q1.1`, `Q1.2`, and `Q1.3`.

```sql
SHOW RECOMMENDATIONS FROM ts1 LIMIT 1\G

                   Id: 0
                 Name: _mv_20240716T211252_3f05a0e6f04a36bd61254f443081acaa7b0633b4
        RecommendedMV: CREATE MATERIALIZED VIEW _mv_20240716T211252_3f05a0e6f04a36bd61254f443081acaa7b0633b4 (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "1"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(sum(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
           HaltReason: OVERALL
            TimeUsage: 1225
        SamplingRatio: 1.0
            CalcSteps: 8
          CardQuality: EXCELLENT
             RowCount: 6001215
          Cardinality: 222697
    CardRowCountRatio: 0.03710865216460334
              Benefit: 5778518.0
NumQueriesAccelerated: 3
         TotalBenefit: 1.7335554E7
   AcceleratedQueries: ["Q1.1", "Q1.2", "Q1.3"]
```

#### Example 2. Add queries to a Tunespace without specifying query names

```sql
CREATE TUNESPACE ts2;

ALTER TUNESPACE ts2 APPEND SELECT sum(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25;

ALTER TUNESPACE ts2 APPEND SELECT sum(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_yearmonthnum = 199401
AND lo_discount BETWEEN 4 AND 6
AND lo_quantity BETWEEN 26 AND 35;

ALTER TUNESPACE ts2 APPEND SELECT sum(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_weeknuminyear = 6 
AND d_year = 1994
AND lo_discount BETWEEN 5 AND 7
AND lo_quantity BETWEEN 26 AND 35;
```

The system automatically generates a name for each query using their ID in the Tunespace: `id#1`, `id#100001`, and `id#2`.

```sql
SELECT id FROM ts2;

+--------+
| id     |
+--------+
| 100001 |
|      2 |
|      1 |
+--------+

SHOW RECOMMENDATIONS FROM ts2 LIMIT 1\G

                   Id: 0
                 Name: _mv_20240716T211908_3f05a0e6f04a36bd61254f443081acaa7b0633b4
        RecommendedMV: CREATE MATERIALIZED VIEW _mv_20240716T211908_3f05a0e6f04a36bd61254f443081acaa7b0633b4 (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "1"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(sum(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
           HaltReason: OVERALL
            TimeUsage: 1248
        SamplingRatio: 1.0
            CalcSteps: 8
          CardQuality: EXCELLENT
             RowCount: 6001215
          Cardinality: 222697
    CardRowCountRatio: 0.03710865216460334
              Benefit: 5778518.0
NumQueriesAccelerated: 3
         TotalBenefit: 1.7335554E7
   AcceleratedQueries: ["id#1", "id#100001", "id#2"]
```

#### Example 3. Recommend materialized views based on legacy materialized views

The name of the materialized view is used to represent the query it contains.

Create a materialized view `_mv_3f05a0e6f04a36bd61254f443081acaa7b0633b4`.

```sql
CREATE MATERIALIZED VIEW _mv_3f05a0e6f04a36bd61254f443081acaa7b0633b4 (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "1"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(SUM(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum;
```

Add `Q1.1`, `Q1.2`, and `Q1.3` to Tunespace `ts3`.

```sql
CREATE TUNESPACE ts3;

ALTER TUNESPACE ts3 APPEND "Q1.1" SELECT SUM(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25;

ALTER TUNESPACE ts3 APPEND "Q1.2" SELECT SUM(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_yearmonthnum = 199401
AND lo_discount BETWEEN 4 AND 6
AND lo_quantity BETWEEN 26 AND 35;

ALTER TUNESPACE ts3 APPEND "Q1.3" SELECT SUM(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_weeknuminyear = 6 
AND d_year = 1994
AND lo_discount BETWEEN 5 AND 7
AND lo_quantity BETWEEN 26 AND 35;
```

Populate `ts3` with the legacy materialized view you have created in the database `ssb`.

```sql
ALTER TUNESPACE ts3 POPULATE FROM DATABASE ssb;
```

The top one recommended materialized can accelerate `Q1.1`, `Q1.2`, and `Q1.3`, and merge the legacy materialized view `_mv_3f05a0e6f04a36bd61254f443081acaa7b0633b4`.

```sql
SHOW RECOMMENDATIONS FROM ts3 LIMIT 1\G

                   Id: 0
                 Name: _mv_20240716T212706_3f05a0e6f04a36bd61254f443081acaa7b0633b4
        RecommendedMV: CREATE MATERIALIZED VIEW _mv_20240716T212706_3f05a0e6f04a36bd61254f443081acaa7b0633b4 (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "1"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(SUM(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
           HaltReason: OVERALL
            TimeUsage: 1193
        SamplingRatio: 1.0
            CalcSteps: 8
          CardQuality: EXCELLENT
             RowCount: 6001215
          Cardinality: 222697
    CardRowCountRatio: 0.03710865216460334
              Benefit: 5778518.0
NumQueriesAccelerated: 4
         TotalBenefit: 2.3114072E7
   AcceleratedQueries: ["Q1.1", "Q1.2", "Q1.3", "_mv_3f05a0e6f04a36bd61254f443081acaa7b0633b4"]
```

#### Example 4. Recommend materialized views based on sub-queries of a complex query

When a complex query is not eligible for materialized view recommendation but its sub-queries are, StarRocks will abstract the sub-structures of the query, and recommend materialized views based on these sub-structures. The system automatically generates a name for each sub-structure.

Add a UNION ALL query of `Q1.1`, `Q1.2`, and `Q1.3`.

```sql
CREATE TUNESPACE ts4;

ALTER TUNESPACE ts4 APPEND "query" 
SELECT "Q1.1" AS label, SUM(lo_revenue) AS revenue
FROM lineorder 
JOIN dates ON lo_orderdate = d_datekey
WHERE d_year = 1993 
AND lo_discount BETWEEN 1 AND 3 
AND lo_quantity < 25
UNION ALL
SELECT "Q1.2" AS label, SUM(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_yearmonthnum = 199401
AND lo_discount BETWEEN 4 AND 6
AND lo_quantity BETWEEN 26 AND 35
UNION ALL
SELECT "Q1.3" AS label, SUM(lo_revenue) AS revenue
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
WHERE d_weeknuminyear = 6 
AND d_year = 1994
AND lo_discount BETWEEN 5 AND 7
AND lo_quantity BETWEEN 26 AND 35;
```

Query the name of the sub-queries.

```sql
SELECT id, traits->'name' FROM ts4;
+------+----------------+
| id   | traits->'name' |
+------+----------------+
|    2 | "query.part.1" |
|    3 | "query.part.2" |
|    1 | "query.part.0" |
+------+----------------+
```

The top one recommended materialized can accelerate `query.part.0`, `query.part.1`, `query.part.2`.

```sql
SHOW RECOMMENDATIONS FROM ts4 LIMIT 1\G
                   Id: 0
                 Name: _mv_20240716T213443_3f05a0e6f04a36bd61254f443081acaa7b0633b4
        RecommendedMV: CREATE MATERIALIZED VIEW _mv_20240716T213443_3f05a0e6f04a36bd61254f443081acaa7b0633b4 (
  lo_discount
  , lo_quantity
  , d_year
  , d_weeknuminyear
  , d_yearmonthnum
  , _ca0003
)
COMMENT "MV recommended by AutoMV"
DISTRIBUTED BY HASH (lo_discount, lo_quantity, d_year, d_weeknuminyear, d_yearmonthnum) BUCKETS 64
ORDER BY (lo_discount, lo_quantity, d_year)
REFRESH ASYNC START("2023-12-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "storage_medium" = "HDD",
  "replication_num" = "1"
)
AS
SELECT
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
  ,(SUM(_ta0000.lo_revenue)) AS _ca0003
FROM
  (
    SELECT
      `ssb`.`dates`.d_year
      ,`ssb`.`dates`.d_yearmonthnum
      ,`ssb`.`dates`.d_weeknuminyear
      ,`ssb`.`lineorder`.lo_quantity
      ,`ssb`.`lineorder`.lo_discount
      ,`ssb`.`lineorder`.lo_revenue
    FROM
      `ssb`.`lineorder`
      INNER JOIN
      `ssb`.`dates`
      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)
  ) _ta0000
GROUP BY
  _ta0000.lo_discount
  ,_ta0000.lo_quantity
  ,_ta0000.d_year
  ,_ta0000.d_weeknuminyear
  ,_ta0000.d_yearmonthnum
           HaltReason: OVERALL
            TimeUsage: 1237
        SamplingRatio: 1.0
            CalcSteps: 8
          CardQuality: EXCELLENT
             RowCount: 6001215
          Cardinality: 222697
    CardRowCountRatio: 0.03710865216460334
              Benefit: 5778518.0
NumQueriesAccelerated: 3
         TotalBenefit: 1.7335554E7
   AcceleratedQueries: ["query.part.0", "query.part.1", "query.part.2"]
```