---
displayed_sidebar: docs
---

# StarRocks version 4.1

## 4.1.0-RC

Release Date: February 28, 2026

### Shared-data Architecture

- **New Multi-Tenant Data Management**
  Shared-data clusters now support range-based data distribution and automatic splitting and merging of tablets. Tablets can be automatically split when they become oversized or hotspots, without requiring schema changes, SQL modifications, or data re-ingestion. This feature can significantly improve usability, directly addressing data skew and hotspot issues in multi-tenant workloads. [#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743)
- **Large-Capacity Tablet Support (Phase 1)**
  Supports significantly larger per-tablet data capacity for shared-data clusters, with a long-term target of 100 GB per tablet. Phase 1 focuses on enabling parallel Compaction and parallel MemTable finalization within a single Lake tablet, reducing ingestion and Compaction overhead as tablet size grows. [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#68677](https://github.com/StarRocks/starrocks/pull/68677)
- **Fast Schema Evolution V2**
  Shared-data clusters now support Fast Schema Evolution V2, which enables second-level DDL execution for schema operations, and further extends the support to materialized views. [#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)
- **[Beta] Inverted Index on shared-data**
  Enables built-in inverted indexes for shared-data clusters to accelerate text filtering and full-text search workloads. [#66541](https://github.com/StarRocks/starrocks/pull/66541)
- **Cache Observability**
  Cache hit ratio metrics are exposed in audit logs and the monitoring system for better cache transparency and latency predictability. Detailed Data Cache metrics include memory and disk quota, page cache statistics, and per-table hit rates. [#63964](https://github.com/StarRocks/starrocks/pull/63964)
- Added segment metadata filter for Lake tables to skip irrelevant segments based on sort key range during scans, reducing I/O for range-predicate queries. [#68124](https://github.com/StarRocks/starrocks/pull/68124)
- Supports fast cancel for Lake DeltaWriter, reducing latency for cancelled ingestion jobs in shared-data clusters. [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- Added support for interval-based scheduling for automated cluster snapshots. [#67525](https://github.com/StarRocks/starrocks/pull/67525)

### Data Lake Analytics

- **Iceberg DELETE Support**
  Supports writing position delete files for Iceberg tables, enabling DELETE operations on Iceberg tables directly from StarRocks. The support covers the full pipeline of Plan, Sink, Commit, and Audit. [#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)
- **TRUNCATE for Hive and Iceberg Tables**
  Supports TRUNCATE TABLE on external Hive and Iceberg tables. [#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)
- **Incremental materialized view on Iceberg and Paimon**
  Extends the support for incremental materialized view refresh to Iceberg append-only tables and Paimon tables, enabling query acceleration without full table refresh. [#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)
- Supports reading file path and row position metadata columns from Iceberg tables. [#67003](https://github.com/StarRocks/starrocks/pull/67003)
- Supports reading `_row_id` from Iceberg v3 tables, and supports global late materialization for Iceberg v3. [#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- Supports creating Iceberg views with custom properties, and displays properties in SHOW CREATE VIEW output. [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Supports querying Paimon tables with a specific branch, tag, version, or timestamp. [#63316](https://github.com/StarRocks/starrocks/pull/63316)
- Enabled additional optimizations in ETL execution mode by default, improving performance for INSERT INTO SELECT, CREATE TABLE AS SELECT, and similar batch operations without explicit configuration. [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- Added commit audit information for INSERT and DELETE operations on Iceberg tables. [#69198](https://github.com/StarRocks/starrocks/pull/69198)
- Supports enabling or disabling view endpoint operations in Iceberg REST Catalog. [#66083](https://github.com/StarRocks/starrocks/pull/66083)
- Optimized cache lookup efficiency in CachingIcebergCatalog. [#66388](https://github.com/StarRocks/starrocks/pull/66388)
- Supports EXPLAIN on various Iceberg catalog types. [#66563](https://github.com/StarRocks/starrocks/pull/66563)

### Query Engine

- **ASOF JOIN**
  Introduces ASOF JOIN for time-series and event correlation queries, enabling efficient matching of the nearest record across two datasets by a temporal or ordered key. [#63070](https://github.com/StarRocks/starrocks/pull/63070) [#63236](https://github.com/StarRocks/starrocks/pull/63236)
- **VARIANT Type for Semi-Structured Data**
  Introduces the VARIANT data type for flexible, schema-on-read storage and querying of semi-structured data. Supports read, write, type casting, and Parquet integration. [#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)
- **Recursive CTE**
  Supports Recursive Common Table Expressions for hierarchical traversals, graph queries, and iterative SQL computations. [#65932](https://github.com/StarRocks/starrocks/pull/65932)
- Improved Skew Join v2 rewrite with statistics-based skew detection, histogram support, and NULL-skew awareness. [#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- Improved COUNT DISTINCT over windows and added support for fused multi-distinct aggregations. [#67453](https://github.com/StarRocks/starrocks/pull/67453)

### Functions and SQL Syntax

- Added the following functions:
  - `array_top_n`: Returns the top N elements from an array ranked by value. [#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`: Combines multiple arrays element-wise into an array of structs. [#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: Formats a JSON string with indentation. [#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: Sets a value at a specified path within a JSON string. [#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: Converts the first letter of each word to uppercase. [#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: Sums MAP values across rows with the same key. [#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: Returns the current session timezone. [#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: Returns the name of the current warehouse. [#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: Converts the number of seconds to a TIME value. [#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: Calls an external AI model from SQL for inference workloads. [#61583](https://github.com/StarRocks/starrocks/pull/61583)
- Provides the following function or syntactic extensions:
  - Supports a lambda comparator in `array_sort` for custom sort ordering. [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - Supports USING clause for FULL OUTER JOIN with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - Supports DISTINCT aggregation over framed window functions with ORDER BY/PARTITION BY. [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - Supports ARRAY type in `lead`/`lag`/`first_value`/`last_value` window functions. [#63547](https://github.com/StarRocks/starrocks/pull/63547)

### Management & Observability

- Supports `warehouses`, `cpu_weight_percent`, and `exclusive_cpu_weight` attributes for resource groups to improve multi-warehouse CPU resource isolation. [#66947](https://github.com/StarRocks/starrocks/pull/66947)
- Introduces the `information_schema.fe_threads` system view to inspect the FE thread state. [#65431](https://github.com/StarRocks/starrocks/pull/65431)
- Supports SQL Digest Blacklist to block specific query patterns at the cluster level. [#66499](https://github.com/StarRocks/starrocks/pull/66499)
- Supports Arrow Flight Data Retrieval from nodes that are otherwise inaccessible due to network topology constraints. [#66348](https://github.com/StarRocks/starrocks/pull/66348)
- Introduces the REFRESH CONNECTIONS command to propagate global variable changes to existing connections without reconnecting. [#64964](https://github.com/StarRocks/starrocks/pull/64964)
- Added built-in UI functions to analyze query profiles and view formatted SQL, making query tuning more accessible. [#63867](https://github.com/StarRocks/starrocks/pull/63867)
- Implements `ClusterSummaryActionV2` API endpoint to provide a structured cluster overview. [#68836](https://github.com/StarRocks/starrocks/pull/68836)
- Added a global read-only system variable `@@run_mode` to query the current cluster run mode (shared-data or shared-nothing). [#69247](https://github.com/StarRocks/starrocks/pull/69247)

### Behavior Changes

- ETL execution mode optimizations are now enabled by default. This benefits INSERT INTO SELECT, CREATE TABLE AS SELECT, and similar batch workloads without explicit configuration changes. [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- The third argument of `lag`/`lead` window functions now supports column references in addition to constant values. [#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING now follows SQL-standard semantics: the USING column appears once in the output instead of twice. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
