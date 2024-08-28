---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# Feature Support: Asynchronous Materialized Views

Asynchronous materialized views are supported from StarRocks v2.4 onwards. Asynchronous materialized views are designed to accelerate complex queries with joins or aggregations on large tables within StarRocks or in data lakes. The performance difference can be significant when a query is run frequently or is sufficiently complex. Additionally, asynchronous materialized views are especially useful for building mathematical models upon your data warehouse.

This document outlines the boundaries of competence for asynchronous materialized views and the supported version of the features involved.

## DDL Features

| Feature                       | Description                                              | Supported Version(s) |
| :-------------------------------- | :----------------------------------------------------------- | :----------------------- |
| Auto Analyze                      | Automatically collects statistics after the materialized view is created to avoid rewrite failures. | v3.0+                           |
| Random Bucketing                  | Enables random bucketing strategy for materialized views by default. | v3.1+                           |
| Deferred Refresh                  | Supports specifying whether to refresh the materialized view immediately after being created by using DEFERRED or IMMEDIATE in CREATE MATERIALIZED VIEW. | v3.0+                           |
| Order By                          | Supports specifying the sort key for materialized views using ORDER BY. | v3.1+                           |
| Window/CTE/Union/Subquery         | Supports using window functions, CTEs, Unions, and subqueries in materialized views. | v2.5+                           |
| ALTER ACTIVE                      | Activates invalid materialized views after Schema Changes in base tables using the ACTIVE keyword in ALTER MATERIALIZED VIEW. | v2.5.7+<br />v3.0.1+<br />v3.1+ |
| REFRESH SYNC MODE                 | Supports synchronous execution for materialized view refresh tasks by using WITH SYNC MODE keywords in REFRESH MATERIALIZED VIEW. | v2.5.8+<br />v3.0.4+<br />v3.1+ |
| Intermediate Result Spilling      | Supports enabling Intermediate Result Spilling using the `enable_spill` property to avoid OOM during materialized view construction. | v3.1+                           |
| Resource Group                    | Supports specifying resource groups using the `resource_group` property for materialized view construction to achieve resource isolation. | v3.1+                           |
| Materialized View on View         | Supports creating materialized views based on logical views. | v3.1+                           |
| Swap Materialized View            | Supports atomically replacing materialized view using the SWAP WITH keywords in ALTER MATERIALIZED VIEW. | v3.1+                           |
| CREATE INDEX ON Materialized View | Supports creating indexes on materialized views to accelerate point queries. | v3.0.7+<br />v3.1.4+<br />v3.2+ |
| AUTO ACTIVE                       | Automatically activates invalid materialized views in the background with exponential backoff, stopping after the interval reaches 60 minutes. | v3.1.4+<br />v3.2+              |
| Backup and Restore                | Supports Backup and Restore for materialized views.          | v3.2+                           |
| Object Dependencies               | Provides a system-defined view `sys.object_dependencies` to clarify the dependency relationship between materialized views and base tables. | v3.2+                           |

## Variables

| Variable                                    | Description                                              | Default | Supported Version(s) |
| :---------------------------------------------- | :----------------------------------------------------------- | :---------- | :------------------------ |
| enable_materialized_view_rewrite                | Whether to enable materialized view query rewrite.           | true        | v2.5+                                                        |
| enable_materialized_view_for_insert             | Whether to enable materialized view query rewrite for INSERT statements. | false       | v2.5.18+<br />v3.0.9+<br />v3.1.7+<br />v3.2.2+              |
| materialized_view_rewrite_mode                  | Mode of materialized view query rewrite.                     | DEFAULT     | v3.2+                                                        |
| optimizer_materialized_view_timelimit           | Maximum time can be used for materialized view query rewrite, after which query rewrite is abandoned and the Optimizer process continues. | 1000        | v3.1.9+<br />v3.2.5+                                         |
| analyze_mv                                      | Method of collecting statistics after the materialized view is refreshed. | SAMPLE      | v3.0+                                                        |
| enable_materialized_view_plan_cache             | Whether to enable plan cache for materialized views. By default, 1000 materialized view plans are cached. | TRUE        | v2.5.13+<br />v3.0.7+<br />v3.1.4+<br />v3.2.0+<br />v3.3.0+ |
| query_including_mv_names                        | Whitelist of the materialized views that can be used for query rewrite. |             | v3.1.11+<br />v3.2.5+                                        |
| query_excluding_mv_names                        | Blacklist of the materialized views that can be used for query rewrite. |             | v3.1.11+<br />v3.2.5+                                        |
| cbo_materialized_view_rewrite_related_mvs_limit | Maximum number of candidate materialized views in the Plan stage. | 64          | v3.1.9+<br /> v3.2.5+                                        |

## Properties

| Property                       | Description                                              | Supported Version(s) |
| :--------------------------------- | :----------------------------------------------------------- | :----------------------- |
| `session.<property_name>`          | Prefix of session variables used for materialized view construction, for example, `session.query_timeout` and `session.query_mem_limit`. | v3.0+                    |
| auto_refresh_partitions_limit      | Maximum number of materialized view partitions to be refreshed each time an automatic refresh is triggered. | v2.5+                    |
| excluded_trigger_tables            | Base tables whose updates will not trigger the materialized view automatic refresh. | v2.5+                    |
| partition_refresh_number           | Number of partitions to be refreshed in each batch when the refresh task is executed in batches. | v2.5+                    |
| partition_ttl_number               | The number of most recent materialized view partitions to retain. | v2.5+                    |
| partition_ttl                      | The time-to-live (TTL) for materialized view partitions. This property is recommended over `partition_ttl_number`. | v3.1.4+<br />v3.2+       |
| force_external_table_query_rewrite | Whether to enable query rewrite for external catalog-based materialized views. | v2.5+                    |
| query_rewrite_consistency          | The query rewrite rule for materialized views built on internal tables. | v3.0.5+<br />v3.1+       |
| resource_group                     | The resource group to which the refresh tasks of the materialized view belong. | v3.1+                    |
| colocate_with                      | The colocation group of the materialized view.               | v3.1+                    |
| foreign_key_constraints            | The Foreign Key constraints when you create a materialized view for query rewrite in the View Delta Join scenario. | v2.5.4+<br />v3.0+       |
| unique_constraints                 | The Unique Key constraints when you create a materialized view for query rewrite in the View Delta Join scenario. | v2.5.4+<br />v3.0+       |
| mv_rewrite_staleness_second        | Staleness tolerance for materialized view data during query rewrite. | v3.1+                    |
| enable_query_rewrite               | Whether the materialized view can be used for query rewrite. | v3.3+                    |

## Partitioning

| Alignment                                                | Use Case                                                 | Supported Version(s) |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------- |
| [Align partitions one-to-one (Date types)](./create_partitioned_materialized_view.md#align-partitions-one-to-one)                 | Create a materialized view whose partitions correspond to the partitions of the base table one-to-one by using the same Partitioning Key. The Partitioning Key must be the DATE or DATETIME type. | v2.5+                    |
| [Align partitions one-to-one (STRING type)](./create_partitioned_materialized_view.md#align-partitions-one-to-one)                | Create a materialized view whose partitions correspond to the partitions of the base table one-to-one by using the same Partitioning Key. The Partitioning Key must be the STRING type. | v3.1.4+<br />v3.2+       |
| [Align partitions with time granularity rollup (Date types)](./create_partitioned_materialized_view.md#align-partitions-with-time-granularity-rollup) | Create a materialized view whose partitioning granularity is larger than that of the base table by using the `date_trunc` function on the Partitioning Key. The Partitioning Key must be the DATE or DATETIME type. | v2.5+                    |
| [Align partitions with time granularity rollup (STRING type)](./create_partitioned_materialized_view.md#align-partitions-with-time-granularity-rollup) | Create a materialized view whose partitioning granularity is larger than that of the base table by using the `date_trunc` function on the Partitioning Key. The Partitioning Key must be the STRING type. | v3.1.4+<br />v3.2+       |
| [Align partitions at a customized time granularity](./create_partitioned_materialized_view.md#align-partitions-at-a-customized-time-granularity)        | Create a materialized view and customize the time granularity for its partitions by using the `date_trunc` function with the `time_slice` or `date_slice` function. | v3.2+                    |
| [Align partitions with multiple base tables](./create_partitioned_materialized_view.md#align-partitions-with-multiple-base-tables)               | Create a materialized view whose partitions are aligned with those of multiple base tables, as long as the base tables use the same type of Partitioning Key. | v3.3+                    |

**Different Join Methods**

- **Single Fact Table (v2.4+)**:  Establishing a partition mapping between the materialized view and the fact table ensures the automatic refresh of materialized view partitions when the fact table is updated.
- **Multiple Fact Tables (v3.3+)**: Establishing a partition mapping between the materialized view and multiple fact tables that are joined/unioned at the same time granularity ensures the automatic refresh of materialized view partitions when any of the fact tables are updated.
- **Temporal Dimension Table (v3.3+)**: Suppose the dimension table stores historical version data and is partitioned at a specific time granularity, and a fact table joins the dimension table at the same time granularity. Establishing a partition mapping between the materialized view and both the fact table and the dimension table ensures the automatic refresh of materialized view partitions when either table is updated.

## Materialized views on external catalogs

| External Data Source | Supported Scenario and Version(s)                        | Stable Version(s) |
| :----------------------- | :----------------------------------------------------------- | :-------------------- |
| Hive                         | <ul><li>Non-partitioned table: v2.5.4 & v3.0+</li><li>DATE and DATETIME-type partition: v2.5.4 & v3.0+</li><li>Transforming STRING-type Partition Key to DATE-type: v3.1.4 & v3.2+</li><li>Materialized views on Hive View: To be supported</li><li>Multi-level partitioning: To be supported</li></ul> | v2.5.13+<br />v3.0.6+<br />v3.1.5+<br />v3.2+ |
| Iceberg                      | <ul><li>Non-partitioned table: v3.0+</li><li>DATE and DATETIME-type partition: v3.1.4 & v3.2+</li><li>Transforming STRING-type Partition Key to DATE-type: v3.1.4 & v3.2+</li><li>Materialized views on Iceberg View: To be supported</li><li>Partition Transform: v3.2.3</li><li>Partition-level refresh: v3.1.7 & v3.2.3</li><li>Multi-level partitioning: To be supported</li></ul> | v3.1.5+<br />v3.2+                            |
| Hudi                         | <ul><li>Non-partitioned table: v3.2+</li><li>DATE and DATETIME-type partition: v3.2+</li><li>Multi-level partitioning: To be supported</li></ul> | Not Stable                                    |
| Paimon                       | <ul><li>Non-partitioned table: v2.5.4 & v3.0+</li><li>DATE and DATETIME-type partition: To be supported</li><li>Multi-level partitioning: To be supported</li></ul> | Not Stable                                    |
| DeltaLake                    | <ul><li>Non-partitioned table: v3.2+</li><li>Partitioned table: To be supported</li><li>Multi-level partitioning: To be supported</li></ul> | Not Stable                                    |
| JDBC                         | <ul><li>Non-partitioned table: v3.0+</li><li>Partitioned table: MySQL RangeColumn Partition v3.1.4</li></ul> | Not Stable                                    |

## Query Rewrite

| Feature                         | Description                                              | Supported Version(s)        |
| :---------------------------------- | :----------------------------------------------------------- | :------------------------------ |
| Single Table Rewrite                | Query rewrite with materialized views built on a single internal table. | v2.5+                           |
| Inner Join Rewrite                  | Query rewrite for INNER/CROSS JOIN on internal tables.       | v2.5+                           |
| Aggregate Rewrite                   | Query rewrite for Joins with basic aggregations.             | v2.5+                           |
| UNION Rewrite                       | Predicate UNION compensation rewrite and partition UNION compensation rewrite on internal tables. | v2.5+                           |
| Nested Materialized View Rewrite    | Query rewrite with nested materialized views on internal tables. | v2.5+                           |
| Count Distinct Rewrite (bitmap/hll) | Query rewrite for COUNT DISTINCT calculations into bitmap- or HLL-based calculations. | v2.5.6+<br />v3.0+              |
| View Delta Join Rewrite             | Rewrite queries that join the tables that are the subset of the tables the materialized view joins. | v2.5.4+<br />v3.0+              |
| Join Derivability Rewrite           | Query rewrite between different join types.                  | v2.5.8+<br />v3.0.4+<br />v3.1+ |
| Full Outer Join and Other Joins     | Query rewrite for Full Outer Join, Semi Join, and Anti Join. | v3.1+                           |
| Avg to Sum/Count Rewrite            | Query rewrite for avg() to sum() / count()                   | v3.1+                           |
| View-based Rewrite                  | Query rewrite with materialized views built upon views without transcribing queries against the view into queries against the base tables of the view. | v3.2.2+                         |
| Count Distinct Rewrite (ArrayAgg)   | Query rewrite for COUNT DISTINCT calculations into calculations with the `array_agg_distinct` function. | v3.2.5+<br />v3.3+              |
| Text-based Query Rewrite            | Rewrite the query that has the identical abstract syntax tree with that of the materialized view's definition. | v3.3+                           |

## Diagnostic Features

| Feature        | Usage Scenario                                           | Supported Version(s)         |
| :----------------- | :----------------------------------------------------------- | :------------------------------- |
| TRACE REWRITE      | Use the TRACE REWRITE statement to diagnose rewrite issues.  | v2.5.10+<br />v3.0.5+<br />v3.1+ |
| Query Dump         | Dump the information of a materialized view once it is queried. | v3.1+                            |
| Refresh Audit Log  | Record the SQL executed in the Audit Log when a materialized view is refreshed. | v2.5.8+<br />v3.0.3+<br />v3.1+  |
| Hit Audit Log      | Record the hit materialized view and candidate materialized views in the Audit Log when a query is rewritten to a materialized view. | v3.1.4+<br />v3.2+               |
| Monitoring Metrics | Dedicated monitoring metrics for materialized views.         | v3.1.4+<br />v3.2+               |

