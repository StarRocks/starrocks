---
displayed_sidebar: "English"
---

# CREATE MATERIALIZED VIEW

## Description

Creates a materialized view. For usage information about materialized views, see [Synchronous materialized view](../../../using_starrocks/Materialized_view-single_table.md) and [Asynchronous materialized view](../../../using_starrocks/Materialized_view.md).

> **CAUTION**
>
> Only users with the CREATE MATERIALIZED VIEW privilege in the database where the base table resides can create a materialized view.

Creating a materialized view is an asynchronous operation. Running this command successfully indicates that the task of creating the materialized view is submitted successfully. You can view the building status of a synchronous materialized view in a database via [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) command, and view that of an asynchronous materialized view by querying the metadata tables `tasks` and `task_runs` in [Information Schema](../../../administration/information_schema.md).

StarRocks supports asynchronous materialized views from v2.4. The major differences between asynchronous materialized views and synchronous materialized views in previous versions are as follows:

|                       | **Single-table aggregation** | **Multi-table join** | **Query rewrite** | **Refresh strategy** | **Base table** |
| --------------------- | ---------------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MV** | Yes | Yes | Yes | <ul><li>Asynchronous refresh</li><li>Manual refresh</li></ul> | Multiple tables from:<ul><li>Default catalog</li><li>External catalogs (v2.5)</li><li>Existing materialized views (v2.5)</li><li>Existing views (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | Limited choices of aggregate functions | No | Yes | Synchronous refresh during data loading | Single table in the default catalog |

## Synchronous materialized view

### Syntax

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

Parameters in brackets [] are optional.

### Parameters

**mv_name** (required)

The name of the materialized view. The naming requirements are as follows:

- The name must consist of letters (a-z or A-Z), digits (0-9), or underscores (\_), and it can only start with a letter.
- The length of the name cannot exceed 64 characters.
- The name is case-sensitive.

**COMMENT** (optional)

Comment on the materialized view. Note that `COMMENT` must be placed after `mv_name`. Otherwise, the materialized view cannot be created.

**query_statement** (required)

The query statement to create the materialized view. Its result is the data in the materialized view. The syntax is as follows:

```SQL
SELECT select_expr[, select_expr ...]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr (required)

  All columns in the query statement, that is, all columns in the materialized view schema. This parameter supports the following values:

  - Simple columns or aggregate columns such as `SELECT a, abs(b), min(c) FROM table_a`, where `a`, `b`, and `c` are the names of columns in the base table. If you do not specify column names for the materialized view, StarRocks automatically assigns names to the columns.
  - Expressions such as `SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a`, where `a+1`, `b+2` and `c*c` are the expressions that reference the columns in the base tables, and `x`, `y` and `z` are the aliases assigned to the columns in the materialized view.

  > **NOTE**
  >
  > - You must specify at least one column in `select_expr`.
  > - Synchronous materialized views only support aggregate functions on a single column. Query statements in the form of `sum(a+b)` are not supported.
  > - When creating a synchronous materialized view with an aggregate function, you must specify the GROUP BY clause, and specify at least one GROUP BY column in `select_expr`.
  > - Synchronous materialized views do not support clauses such as JOIN, WHERE, and the HAVING clause of GROUP BY.
  > - From v3.1 onwards, each synchronous materialized view can support more than one aggregate function for each column of the base table, for example, query statements such as `select b, sum(a), min(a) from table group by b`.
  > - From v3.1 onwards, synchronous materialized views support complex expressions for SELECT and aggregate functions, for example, query statements such as `select b, sum(a + 1) as sum_a1, min(cast (a as bigint)) as min_a from table group by b` or `select abs(b) as col1, a + 1 as col2, cast(a as bigint) as col3 from table`. The following restrictions are imposed on the complex expression used for synchronous materialized views:
  >   - Each complex expression must have an alias and different aliases must be assigned to different complex expressions among all the synchronous materialized views of a base table. For example, query statements `select b, sum(a + 1) as sum_a from table group by b` and `select b, sum(a) as sum_a from table group by b` cannot be used to create synchronous materialized views for a same base table.
  >   - Each complex expression can reference only one column. Query statements such as `a + b as col1` are not supported.
  >   - You can check whether your queries are rewritten by the synchronous materialized views created with complex expressions by executing `EXPLAIN <sql_statement>`. For more information, see [Query analysis](../../../administration/Query_planning.md).

- GROUP BY (optional)

  The GROUP BY column of the query. If this parameter is not specified, the data will not be grouped by default.

- ORDER BY (optional)

  The ORDER BY column of the query.

  - Columns in the ORDER BY clause must be declared in the same order as the columns in `select_expr`.
  - If the query statement contains a GROUP BY clause, the ORDER BY columns must be identical to the GROUP BY columns.
  - If this parameter is not specified, the system will automatically supplement the ORDER BY column according to the following rules:
    - If the materialized view is the AGGREGATE type, all GROUP BY columns are automatically used as sort keys.
    - If the materialized view is not the AGGREGATE type, StarRocks automatically selects sort keys based on the prefix columns.

### Query a synchronous materialized view

Because a synchronous materialized view is essentially an index of the base table rather than a physical table, you can only query a synchronous materialized view using the hint `[_SYNC_MV_]`:

```SQL
-- Do not omit the brackets [] in the hint.
SELECT * FROM <mv_name> [_SYNC_MV_];
```

> **CAUTION**
>
> Currently, StarRocks automatically generates names for columns in a synchronous materialized view even if you have specified aliases for them.

### Automatic query rewrite with synchronous materialized view

When a query that follows the pattern of a synchronous materialized view is executed, the original query statement is automatically rewritten and the intermediate results stored in the materialized view are used. 

The following table shows the correspondence between the aggregate function in the original query and the aggregate function used to construct the materialized view. You can select the corresponding aggregate function to build a materialized view according to your business scenario.

| **aggregate function in the original query**           | **aggregate function of the materialized view** |
| ------------------------------------------------------ | ----------------------------------------------- |
| sum                                                    | sum                                             |
| min                                                    | min                                             |
| max                                                    | max                                             |
| count                                                  | count                                           |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                    |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                       |
| percentile_approx, percentile_union                    | percentile_union                                |

## Asynchronous materialized view

### Syntax

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
    [ASYNC [START (<start_time>)] [EVERY (INTERVAL <refresh_interval>)] | MANUAL]
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

Parameters in brackets [] are optional.

### Parameters

**mv_name** (required)

The name of the materialized view. The naming requirements are as follows:

- The name must consist of letters (a-z or A-Z), digits (0-9), or underscores (\_), and it can only start with a letter.
- The length of the name cannot exceed 64 characters.
- The name is case-sensitive.

> **CAUTION**
>
> Multiple materialized views can be created on the same base table, but the names of the materialized views in the same database cannot be duplicated.

**COMMENT** (optional)

Comment on the materialized view. Note that `COMMENT` must be placed after `mv_name`. Otherwise, the materialized view cannot be created.

**distribution_desc** (optional)

The bucketing strategy of the asynchronous materialized view. StarRocks supports hash bucketing and random bucketing (from v3.1 onwards). If you do not specify this parameter, StarRocks uses the random bucketing strategy and automatically sets the number of buckets.

> **NOTE**
>
> While creating an asynchronous materialized view, you must specify either `distribution_desc` or `refresh_scheme`, or both.

- **Hash bucketing**:

  Syntax

  ```SQL
  DISTRIBUTED BY HASH (<bucket_key1>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
  ```

  For more information, see [Data distribution](../../../table_design/Data_distribution.md#data-distribution).

  > **NOTE**
  >
  > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets).

- **Random bucketing**:

  If you choose the random bucketing strategy and allow StarRocks to set the number of buckets automatically, you do not need to specify `distribution_desc`. However, if you want to set the number of buckets manually, you can refer to the following syntax:

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <bucket_number>
  ```

  > **CAUTION**
  >
  > Asynchronous materialized views with a random bucketing strategy cannot be assigned to a colocation group.

  For more information, see [Random bucketing](../../../table_design/Data_distribution.md#random-bucketing-since-v31)

**refresh_moment** (optional)

The refresh moment of the materialized view. Default value: `IMMEDIATE`. Valid values:

- `IMMEDIATE`: Refresh the asynchronous materialized view immediately after it is created.
- `DEFERRED`: The asynchronous materialized view is not refreshed after it is created. You can manually refresh the materialized view or schedule regular refresh tasks.

**refresh_scheme** (optional)

> **NOTE**
>
> While creating an asynchronous materialized view, you must specify either `distribution_desc` or `refresh_scheme`, or both.

The refresh strategy of the asynchronous materialized view. Valid values:

- `ASYNC`: Asynchronous refresh mode. Each time the base table data changes, the materialized view is automatically refreshed according to the pre-defined refresh interval. You can further specify the refresh start time as `START('yyyy-MM-dd hh:mm:ss')`, and specify the refresh interval as `EVERY (interval n day/hour/minute/second)` using the following units: `DAY`, `HOUR`, `MINUTE`, and `SECOND`. Example: `ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)`. If you do not specify the interval, the default value `10 MINUTE` is used.
- `MANUAL`: Manual refresh mode. The materialized view will not be automatically refreshed. The refresh tasks can only be triggered manually by users.

If this parameter is not specified, the default value `MANUAL` is used.

**partition_expression** (optional)

The partitioning strategy of the asynchronous materialized view. As for the current version of StarRocks, only one partition expression is supported when creating an asynchronous materialized view.

> **CAUTION**
>
> Currently, asynchronous materialized views do not support the list partitioning strategy.

Valid values:

- `column_name`: The name of the column used for partitioning. The expression `PARTITION BY dt` means to partition the materialized view according to the `dt` column.
- date_trunc function: The function used to truncate the time unit. `PARTITION BY date_trunc("MONTH", dt)` means that the `dt` column is truncated to month as the unit for partitioning. The date_trunc function supports truncating time to units including `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`.
- time_slice or date_slice functions: From v3.1 onwards, you can further use these functions to convert the given time into the beginning or end of a time interval based on the specified time granularity, for example, `PARTITION BY date_trunc("MONTH", time_slice(dt, INTERVAL 7 DAY))` where time_slice and date_slice must have a finer granularity than date_trunc. You can use them to specify a GROUP BY column with a finer granularity than that of the partitioning key, for example, `GROUP BY time_slice(dt, INTERVAL 1 MINUTE) PARTITION BY date_trunc('DAY', ts)`.

If this parameter is not specified, no partitioning strategy is adopted by default.

**order_by_expression** (optional)

The sort key of the asynchronous materialized view. If you do not specify the sort key, StarRocks chooses some of the prefix columns from SELECT columns as the sort keys. For example, in `select a, b, c, d`, sort keys can be `a` and `b`. This parameter is supported from StarRocks v3.0 onwards.

**PROPERTIES** (optional)

Properties of the asynchronous materialized view. You can modify the properties of an existing materialized view using [ALTER MATERIALIZED VIEW](./ALTER_MATERIALIZED_VIEW.md).

- `replication_num`: The number of materialized view replicas to create.
- `storage_medium`: Storage medium type. Valid values: `HDD` and `SSD`.
- `storage_cooldown_time`: the storage cooldown time for a partition. If both HDD and SSD storage mediums are used, data in the SSD storage is moved to the HDD storage after the time specified by this property. Format: "yyyy-MM-dd HH:mm:ss". The specified time must be later than the current time. If this property is not explicitly specified, the storage cooldown is not performed by default.
- `partition_ttl`: The time-to-live (TTL) for partitions. Partitions whose data is within the specified time range are retained. Expired partitions are deleted automatically. Unit: `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`. For example, you can specify this property as `2 MONTH`. This property is recommended over `partition_ttl_number`. It is supported from v3.1.5 onwards.
- `partition_ttl_number`: The number of most recent materialized view partitions to retain. For the partitions with a start time earlier than the current time, after the number of these partitions exceeds this value, less recent partitions will be deleted. StarRocks will periodically check materialized view partitions according to the time interval specified in the FE configuration item `dynamic_partition_check_interval_seconds`, and automatically delete expired partitions. If you enabled the [dynamic partitioning](../../../table_design/dynamic_partitioning.md) strategy, the partitions created in advance are not counted in. When the value is `-1`, all partitions of the materialized view will be preserved. Default: `-1`.
- `partition_refresh_number`: In a single refresh, the maximum number of partitions to refresh. If the number of partitions to be refreshed exceeds this value, StarRocks will split the refresh task and complete it in batches. Only when the previous batch of partitions is refreshed successfully, StarRocks will continue to refresh the next batch of partitions until all partitions are refreshed. If any of the partitions fail to be refreshed, no subsequent refresh tasks will be generated. When the value is `-1`, the refresh task will not be split. Default: `-1`.
- `excluded_trigger_tables`: If a base table of the materialized view is listed here, the automatic refresh task will not be triggered when the data in the base table is changed. This parameter only applies to load-triggered refresh strategy, and is usually used together with the property `auto_refresh_partitions_limit`. Format: `[db_name.]table_name`. When the value is an empty string, any data change in all base tables triggers the refresh of the corresponding materialized view. The default value is an empty string.
- `auto_refresh_partitions_limit`: The number of most recent materialized view partitions that need to be refreshed when a materialized view refresh is triggered. You can use this property to limit the refresh range and reduce the refresh cost. However, because not all the partitions are refreshed, the data in the materialized view may not be consistent with the base table. Default: `-1`. When the value is `-1`, all partitions will be refreshed. When the value is a positive integer N, StarRocks sorts the existing partitions in chronological order, and refreshes N partitions from the most recent partition. If the number of partitions is less than N, StarRocks refreshes all existing partitions. If there are dynamic partitions created in advance in your materialized view, StarRocks refreshes the pre-created partitions first, and then the existing partitions. Therefore, when setting this parameter, make sure that you have reserved margins for pre-created dynamic partitions.
- `mv_rewrite_staleness_second`: If the materialized view's last refresh is within the time interval specified in this property, this materialized view can be used directly for query rewrite, regardless of whether the data in the base tables changes. If the last refresh is before this time interval, StarRocks checks whether the base tables have been updated to determine whether the materialized view can be used for query rewrite. Unit: Second. This property is supported from v3.0.
- `colocate_with`: The colocation group of the asynchronous materialized view. See [Colocate Join](../../../using_starrocks/Colocate_join.md) for further information. This property is supported from v3.0.
- `unique_constraints` and `foreign_key_constraints`: The Unique Key constraints and Foreign Key constraints when you create an asynchronous materialized view for query rewrite in the View Delta Join scenario. See [Asynchronous materialized view - Rewrite queries in View Delta Join scenario](../../../using_starrocks/Materialized_view.md#rewrite-queries-in-view-delta-join-scenario) for further information. This property is supported from v3.0.
- `resource_group`: The resource group to which the refresh tasks of the materialized view belong. For more about resource groups see [Resource group](../../../administration/resource_group.md).
- `query_rewrite_consistency`: The query rewrite rule for the asynchronous materialized views. This property is supported from v3.2. Valid values:
  - `disable`: Disable automatic query rewrite of the asynchronous materialized view.
  - `checked` (Default value): Enable automatic query rewrite only when the materialized view meets the timeliness requirement, which means:
    - If `mv_rewrite_staleness_second` is not specified, the materialized view can be used for query rewrite only when its data is consistent with the data in all base tables.
    - If `mv_rewrite_staleness_second` is specified, the materialized view can be used for query rewrite when its last refresh is within the staleness time interval.
  - `loose`: Enable automatic query rewrite directly, and no consistency check is required.
- `force_external_table_query_rewrite`: Whether to enable query rewrite for external catalog-based materialized views. This property is supported from v3.2. Valid values:
  - `true`: Enable query rewrite for external catalog-based materialized views.
  - `false` (Default value): Disable query rewrite for external catalog-based materialized views.

  Because strong data consistency is not guaranteed between base tables and external catalog-based materialized views, this feature is set to `false` by default. When this feature is enabled, the materialized view is used for query rewrite in accordance with the rule specified in `query_rewrite_consistency`.

> **CAUTION**
>
> The Unique Key constraints and Foreign Key constraints are only used for query rewrite. The Foreign Key constraint checks are not guaranteed when data is loaded into the table. You must ensure the data loaded into the table meets the constraints.

**query_statement** (required)

The query statement to create the asynchronous materialized view.

> **CAUTION**
>
> Currently, StarRocks does not support creating asynchronous materialized views with base tables created with the list partitioning strategy.

### Query an asynchronous materialized view

An asynchronous materialized view is a physical table. You can operate it as any regular table **except that you cannot directly load data into an asynchronous materialized view**.

### Automatic query rewrite with asynchronous materialized view

StarRocks v2.5 supports automatic and transparent query rewrite based on the SPJG-type asynchronous materialized views. The SPJG-type materialized views refer to materialized views whose plan only includes Scan, Filter, Project, and Aggregate types of operators. The SPJG-type materialized views query rewrite includes single table query rewrite, Join query rewrite, aggregation query rewrite, Union query rewrite and query rewrite based on nested materialized views.

See [Asynchronous materialized view -  Rewrite queries with the asynchronous materialized view](../../../using_starrocks/Materialized_view.md#rewrite_queries_with_the_asynchronous_materialized_view) for further information.

### Supported data types

- Asynchronous materialized views created based on the StarRocks default catalog support the following data types:

  - **Date**: DATE, DATETIME
  - **String**: CHAR, VARCHAR
  - **Numeric**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, PERCENTILE
  - **Semi-structured**: ARRAY, JSON, MAP (from v3.1 onwards), STRUCT (from v3.1 onwards)
  - **Other**: BITMAP, HLL

> **NOTE**
>
> BITMAP, HLL, and PERCENTILE have been supported since v2.4.5.

- Asynchronous materialized views created based on the StarRocks external catalogs support the following data types:

  - Hive Catalog

    - **Numeric**: INT/INTEGER, BIGINT, DOUBLE, FLOAT, DECIMAL
    - **Date**: TIMESTAMP
    - **String**: STRING, VARCHAR, CHAR
    - **Semi-structured**: ARRAY

  - Hudi Catalog

    - **Numeric**: BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL
    - **Date**: DATE, TimeMillis/TimeMicros, TimestampMillis/TimestampMicros
    - **String**: STRING
    - **Semi-structured**: ARRAY

  - Iceberg Catalog

    - **Numeric**: BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL(P, S)
    - **Date**: DATE, TIME, TIMESTAMP
    - **String**: STRING, UUID, FIXED(L), BINARY
    - **Semi-structured**: LIST

## Usage notes

- The current version of StarRocks does not support creating multiple materialized views at the same time. A new materialized view can only be created when the one before is completed.

- About synchronous materialized views:

  - Synchronous materialized views only support aggregate functions on a single column. Query statements in the form of `sum(a+b)` are not supported.
  - Synchronous materialized views support only one aggregate function for each column of the base table. Query statements such as `select sum(a), min(a) from table` are not supported.
  - When creating a synchronous materialized view with an aggregate function, you must specify the GROUP BY clause, and specify at least one GROUP BY column in SELECT.
  - Synchronous materialized views do not support clauses such as JOIN, WHERE, and the HAVING clause of GROUP BY.
  - When using ALTER TABLE DROP COLUMN to drop a specific column in a base table, you must ensure that all synchronous materialized views of the base table do not contain the dropped column, otherwise, the drop operation will fail. Before you drop the column, you must first drop all synchronous materialized views that contain the column.
  - Creating too many synchronous materialized views for a table will affect the data load efficiency. When data is being loaded to the base table, the data in the synchronous materialized view and the base table will be updated synchronously. If a base table contains `n` synchronous materialized views, the efficiency of loading data into the base table is about the same as the efficiency of loading data into `n` tables.

- About nested asynchronous materialized views:

  - The refresh strategy for each materialized view only applies to the corresponding materialized view.
  - Currently, StarRocks does not limit the number of nesting levels. In a production environment, we recommend that the number of nesting layers not exceed THREE.

- About external catalog asynchronous materialized views:

  - External catalog materialized view only support async fixed-interval refresh and manual refresh.
  - Strict consistency is not guaranteed between the materialized view and the base tables in the external catalog.
  - Currently, building materialized views based on external resources is not supported.
  - Currently, StarRocks cannot perceive if the base table data in the external catalog has changed, so all partitions will be refreshed by default every time the base table is refreshed. You can manually refresh only some of partitions using [REFRESH MATERIALIZED VIEW](../data-manipulation/REFRESH_MATERIALIZED_VIEW.md).

## Examples

### Examples of synchronous materialized views

The schema of the base table is as follows:

```Plain Text
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

Example 1: Create a synchronous materialized view that only contains the columns of the original table (k1, k2).

```sql
create materialized view k1_k2 as
select k1, k2 from duplicate_table;
```

The materialized view contains only two columns k1 and k2 without any aggregation.

```plain text
+-----------------+-------+--------+------+------+---------+-------+
| IndexName       | Field | Type   | Null | Key  | Default | Extra |
+-----------------+-------+--------+------+------+---------+-------+
| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
|                 | k2    | INT    | Yes  | true | N/A     |       |
+-----------------+-------+--------+------+------+---------+-------+
```

Example 2: Create a synchronous materialized view sorted by k2.

```sql
create materialized view k2_order as
select k2, k1 from duplicate_table order by k2;
```

The materialized view's schema is shown below. The materialized view contains only two columns k2 and k1, where column k2 is a sort column without any aggregation.

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
+-----------------+-------+--------+------+-------+---------+-------+
```

Example 3: Create a synchronous materialized view grouped by k1 and k2, and a SUM aggregation on k3.

```sql
create materialized view k1_k2_sumk3 as
select k1, k2, sum(k3) from duplicate_table group by k1, k2;
```

The materialized view's schema is shown below. The materialized view contains three columns k1, k2 and sum (k3), where k1, k2 are grouped columns, and sum (k3) is the sum of the k3 columns grouped according to k1 and k2.

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
+-----------------+-------+--------+------+-------+---------+-------+
```

Because the materialized view does not declare a sort column, and it adopts an aggregation function, StarRocks supplements the grouped columns k1 and k2 by default.

Example 4: Create a synchronous materialized view to remove duplicate rows.

```sql
create materialized view deduplicate as
select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
```

The materialized view's schema is shown below. The materialized view contains k1, k2, k3, and k4 columns, and there are no duplicate rows.

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | true  | N/A     |       |
|                 | k4    | BIGINT | Yes  | true  | N/A     |       |
+-----------------+-------+--------+------+-------+---------+-------+
```

Example 5: Create a non-aggregated synchronous materialized view that does not declare a sort column.

The schema of the base table is shown below:

```plain text
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

The materialized view contains k3, k4, k5, k6, and k7 columns, and no sort column is declared. Create the materialized view with the following statement:

```sql
create materialized view mv_1 as
select k3, k4, k5, k6, k7 from all_type_table;
```

StarRocks automatically uses k3, k4, and k5 as the sort columns by default. The sum of the bytes occupied by these three column types is 4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 < 36. So these three columns are added as sort columns.

The materialized view's schema is as follows.

```plain text
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

It can be observed that the `key` field of the k3, k4, and k5 columns is `true`, which indicates that they are the sort keys. The key field of the k6, and k7 columns is `false`, which indicates that they are not the sort keys.

### Examples of asynchronous materialized views

The following examples are based on the base tables below:

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

Example 1: Create a non-partitioned materialized view.

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

Example 2: Create a partitioned materialized view.

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

-- Use the date_trunc() function to partition the materialized view by month.
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

Example 3: Create an asynchronous materialized view.

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
