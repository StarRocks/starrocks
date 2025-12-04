---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

After upgrading StarRocks to v4.0, DO NOT downgrade it directly to v3.5.0 & v3.5.1, otherwise it will cause metadata incompatibility and FE crash. You must downgrade the cluster to v3.5.2 or later to prevent these issues.

:::

## 4.0.2

Release Date: December 4, 2025

### New Features

- Introduced a new resource group attribute, `mem_pool`, allowing multiple resource groups to share the same memory pool and enforce a joint memory limit for the pool. This feature is backward compatible. `default_mem_pool` is used if `mem_pool` is not specified. [#64112](https://github.com/StarRocks/starrocks/pull/64112)

### Improvements

- Reduced remote storage access during Vacuum after File Bundling is enabled. [#65793](https://github.com/StarRocks/starrocks/pull/65793)
- The File Bundling feature caches the latest tablet metadata. [#65640](https://github.com/StarRocks/starrocks/pull/65640)
- Improved safety and stability for long-string scenarios. [#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)  
- Optimized the `SplitTopNAggregateRule` logic to avoid performance regression. [#65478](https://github.com/StarRocks/starrocks/pull/65478)  
- Applied the Iceberg/DeltaLake table statistics collection strategy to other external data sources to avoid collecting statistics when the table is a single table. [#65430](https://github.com/StarRocks/starrocks/pull/65430)  
- Added Page Cache metrics to the Data Cache HTTP API `api/datacache/app_stat`. [#65341](https://github.com/StarRocks/starrocks/pull/65341)  
- Supports ORC file splitting to enable parallel scanning of a single large ORC file. [#65188](https://github.com/StarRocks/starrocks/pull/65188)  
- Added selectivity estimation for IF predicates in the optimizer. [#64962](https://github.com/StarRocks/starrocks/pull/64962)  
- Supports constant evaluation of `hour`, `minute`, and `second` for `DATE` and `DATETIME` types in the FE. [#64953](https://github.com/StarRocks/starrocks/pull/64953)  
- Enabled rewrite of simple aggregation to MetaScan by default. [#64698](https://github.com/StarRocks/starrocks/pull/64698)  
- Improved multiple-replica assignment handling in shared-data clusters for enhanced reliability. [#64245](https://github.com/StarRocks/starrocks/pull/64245)  
- Exposes cache hit ratio in audit logs and metrics. [#63964](https://github.com/StarRocks/starrocks/pull/63964)  
- Estimates per-bucket distinct counts for histograms using HyperLogLog or sampling to provide more accurate NDV for predicates and joins. [#58516](https://github.com/StarRocks/starrocks/pull/58516)  
- Supports FULL OUTER JOIN USING with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)  
- Prints memory information when Optimizer times out for diagnostics. [#65206](https://github.com/StarRocks/starrocks/pull/65206)

### Bug Fixes

The following issues have been fixed:

- DECIMAL56 `mod`-related issue. [#65795](https://github.com/StarRocks/starrocks/pull/65795)  
- Issue related to Iceberg scan range handling. [#65658](https://github.com/StarRocks/starrocks/pull/65658)  
- MetaScan rewrite issues on temporary partitions and random buckets. [#65617](https://github.com/StarRocks/starrocks/pull/65617)  
- `JsonPathRewriteRule` uses the wrong table after transparent materialized view rewrite. [#65597](https://github.com/StarRocks/starrocks/pull/65597)  
- Materialized view refresh failures when `partition_retention_condition` referenced generated columns. [#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg min/max value typing issue. [#65551](https://github.com/StarRocks/starrocks/pull/65551)  
- Issue with queries against `information_schema.tables` and `views` across different databases when `enable_evaluate_schema_scan_rule` is set to `true`. [#65533](https://github.com/StarRocks/starrocks/pull/65533)  
- Integer overflow in JSON array comparison. [#64981](https://github.com/StarRocks/starrocks/pull/64981)  
- MySQL Reader does not support SSL. [#65291](https://github.com/StarRocks/starrocks/pull/65291)  
- ARM build issue caused by SVE build incompatibility. [#65268](https://github.com/StarRocks/starrocks/pull/65268)  
- Queries based on bucket-aware execution may get stuck for bucketed Iceberg tables. [#65261](https://github.com/StarRocks/starrocks/pull/65261)  
- Robust error propagation and memory safety issues for the lack of memory limit checks in OLAP table scan. [#65131](https://github.com/StarRocks/starrocks/pull/65131)

### Behavior Changes

- When a materialized view is inactivated, the system recursively inactivates its dependent materialized views. [#65317](https://github.com/StarRocks/starrocks/pull/65317)  
- Uses the original materialized view query SQL (including comments/formatting) when generating SHOW CREATE output. [#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

Release Date: November 17, 2025

### Improvements

- Optimized TaskRun session variable handling to process known variables only. [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- Supports collecting statistics of Iceberg and Delta Lake tables from metadata by default. [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- Supports collecting statistics of Iceberg tables with bucket and truncate partition transform. [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- Supports inspecting FE `/proc` profile for debugging. [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- Enhanced OAuth2 and JWT authentication support for Iceberg REST catalogs. [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- Improved bundle tablet metadata validation and recovery handling. [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- Improved scan-range memory estimation logic. [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### Bug Fixes

The following issues have been fixed:

- Transaction logs were deleted when publishing bundle tablets. [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- The join algorithm cannot guarantee the sort property because, after joining, the sort property is not reset. [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- Issues related to transparent materialized view rewrite. [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### Behavior Changes

- Added the property `enable_iceberg_table_cache` to Iceberg Catalogs to optionally disable Iceberg table cache and allow it always to read the latest data. [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- Ensured `INSERT ... SELECT` reads the freshest metadata by refreshing external tables before planning. [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- Increased lock table slots to 256 and added `rid` to slow-lock logs. [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- Temporarily disabled `shared_scan` due to incompatibility with event-based scheduling. [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- Changed the default Hive Catalog cache TTL to 24 hours and removed unused parameters. [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- Automatically determine the Partial Update mode based on the session variable and the number of inserted columns. [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

Release date: October 17, 2025

### Data Lake Analytics

- Unified Page Cache and Data Cache for BE metadata, and adopted an adaptive strategy for scaling. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Optimized metadata file parsing for Iceberg statistics to avoid repetitive parsing. [#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Optimized COUNT/MIN/MAX queries against Iceberg metadata by efficiently skipping over data file scans, significantly improving aggregation query performance on large partitioned tables and reducing resource consumption. [#60385](https://github.com/StarRocks/starrocks/pull/60385)
- Supports compaction for Iceberg tables via procedure `rewrite_data_files`. 
- Supports Iceberg tables with hidden partitions, including creating, writing, and reading the tables. [#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Supports setting sort keys when creating Iceberg tables.
- Optimizes sink performance for Iceberg tables.
  - Iceberg Sink supports spilling large operators, global shuffle, and local sorting to optimize memory usage and address small file issues. [#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg Sink optimizes local sorting based on Spill Partition Writer to improve write efficiency. [#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg Sink supports global shuffle for partitions to further reduce small files. [#62123](https://github.com/StarRocks/starrocks/pull/62123)
- Enhanced bucket-aware execution for Iceberg tables to improve concurrency and distribution capabilities of bucketed tables. [#61756](https://github.com/StarRocks/starrocks/pull/61756)
- Supports the TIME data type in the Paimon catalog. [#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Upgraded Iceberg version to 1.10.0. [#63667](https://github.com/StarRocks/starrocks/pull/63667)

### Security and Authentication

- In scenarios where JWT authentication and the Iceberg REST Catalog are used, StarRocks supports the passthrough of user login information to Iceberg via the REST Session Catalog for subsequent data access authentication. [#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Supports vended credentials for the Iceberg catalog.
- Supports granting StarRocks internal roles to external groups obtained via Group Provider. [#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- Added REFRESH privilege to external tables to control the permission to refresh them. [#63385](https://github.com/StarRocks/starrocks/pull/62636)

<!--
- Supports HTTPS via configuring certificates on the StarRocks FE side, enhancing system access security to meet encrypted transmission requirements on the cloud or intranet. [#56394](https://github.com/StarRocks/starrocks/pull/56394)
- Supports HTTPS communication between BE nodes to ensure the encryption and integrity of data transmission, preventing internal data leakage and Man-in-the-Middle attacks.[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### Storage Optimization and Cluster Management

- Introduced  the File Bundling optimization for the cloud-native table in shared-data clusters to automatically bundle the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. File Bundling is enabled by default for tables created in v4.0 or later. [#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Supports Multi-Table Write-Write Transaction to allow users to control the atomic submission of INSERT, UPDATE, and DELETE operations. The transaction supports Stream Load and INSERT INTO interfaces, effectively guaranteeing cross-table consistency in ETL and real-time write scenarios. [#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Supports Kafka 4.0 for Routine Load.
- Supports full-text inverted indexes on Primary Key tables in shared-nothing clusters.
- Supports modifying aggregate keys of Aggregate tables. [#62253](https://github.com/StarRocks/starrocks/issues/62253)
- Supports enabling case-insensitive processing on names of catalogs, databases, tables, views, and materialized views. [#61136](https://github.com/StarRocks/starrocks/pull/61136)
- Supports blacklisting Compute Nodes in shared-data clusters. [#60830](https://github.com/StarRocks/starrocks/pull/60830)
- Supports global connection ID. [#57256](https://github.com/StarRocks/starrocks/pull/57276)
- Added the `recyclebin_catalogs` metadata view to Information Schema to display recoverable deleted metadata. [#51007](https://github.com/StarRocks/starrocks/pull/51007)

### Query and Performance Improvement

- Supports DECIMAL256 data type, expanding the upper limit of precision from 38 to 76 bits. Its 256-bit storage provides better adaptability to high-precision financial and scientific computing scenarios, effectively mitigating DECIMAL128's precision overflow problem in very large aggregations and high-order operations. [#59645](https://github.com/StarRocks/starrocks/issues/59645)
- Improved the performance for basic operators.[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- Optimized the performance of the JOIN and AGG operators. [#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] Introduced SQL Plan Manager to allow users to bind a query plan to a query, thereby preventing the query plan from changing due to system state changes (mainly data updates and statistics updates), thus stabilizing query performance. [#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Introduced Partition-wise Spillable Aggregate/Distinct operators to replace the original Spill implementation based on sorted aggregation, significantly improving aggregation performance and reducing read/write overhead in complex and high-cardinality GROUP BY scenarios. [#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2:
  - Supports configuring Flat JSON on the table level. [#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - Enhance JSON columnar storage by retaining the V1 mechanism while adding page- and segment-level indexes (ZoneMaps, Bloom filters), predicate pushdown with late materialization, dictionary encoding, and integration of a low-cardinality global dictionary to significantly boost execution efficiency. [#60953](https://github.com/StarRocks/starrocks/issues/60953)
- Supports an adaptive ZoneMap index creation strategy for the STRING data type. [#61960](https://github.com/StarRocks/starrocks/issues/61960)
- Enhanced query observability:
  - Optimized EXPLAIN ANALYZE output to display the execution metrics by group and by operator for better readability. [#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` and `QueryProfileActionV2` now support JSON format, enhancing cross-FE query capabilities. [#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - Supports retrieving Query Profile information across all FEs. [#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST statements display Catalog, Query ID, and other information. [#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - Enhanced query queue and process monitoring, supporting display of Running/Pending statuses.[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- Materialized view rewrites consider the distribution and sort keys of the original table, improving the selection of optimal materialized views. [#62830](https://github.com/StarRocks/starrocks/pull/62830)

### Functions and SQL Syntax

- Added the following functions:
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414 ](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` and `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` and `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- Provides the following syntactic extensions:
  - Supports IF NOT EXISTS keywords in CREATE ANALYZE FULL TABLE. [#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - Supports EXCLUDE clauses in SELECT.  [#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - Supports FILTER clauses in aggregate functions, improving readability and execution efficiency of conditional aggregations. [#58937](https://github.com/StarRocks/starrocks/pull/58937)

### Behavior Changes

- Adjust the logic of the materialized view parameter `auto_partition_refresh_number` to limit the number of partitions to refresh regardless of auto refresh or manual refresh. [#62301](https://github.com/StarRocks/starrocks/pull/62301)
- Flat JSON is enabled by default. [#62097](https://github.com/StarRocks/starrocks/pull/62097)
- The default value of the system variable `enable_materialized_view_agg_pushdown_rewrite` is set to `true`, indicating that aggregation pushdown for materialized view query rewrite is enabled by default. [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- Changed the type of some columns in `information_schema.materialized_views` to better align with the corresponding data. [#60054](https://github.com/StarRocks/starrocks/pull/60054)
- The `split_part` function returns NULL when the delimiter is not matched. [#56967](https://github.com/StarRocks/starrocks/pull/56967)
- Use STRING to replace fixed-length CHAR in CTAS/CREATE MATERIALIZED VIEW to avoid deducing the wrong column length, which may cause materialized view refresh failures. [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- Data Cache-related configurations are simplified. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` and `datacache_disk_size` are now effective.
  - `storage_page_cache_limit`, `block_cache_mem_size`, `block_cache_disk_size` are deprecated.
- Added new catalog properties (`remote_file_cache_memory_ratio` for Hive, and `iceberg_data_file_cache_memory_usage_ratio` and `iceberg_delete_file_cache_memory_usage_ratio` for Iceberg) to limit the memory resources used for Hive and Iceberg metadata cache, and set the default values to `0.1` (10%). Adjust the metadata cache TTL to 24 hours. [#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION now will not merge the statistics of all materialized indexes with the same bucket sequence number. It only shows data distribution at the materialized index level. [#59656](https://github.com/StarRocks/starrocks/pull/59656)
- The default bucket size for automatic bucket tables is changed from 4GB to 1GB to improve performance and resource utilization. [#63168](https://github.com/StarRocks/starrocks/pull/63168)
-  The system determines the Partial Update mode based on the corresponding session variable and the number of columns in the INSERT statement. [#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Optimized the `fe_tablet_schedules` view in the Information Schema. [#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - Renamed the `TABLET_STATUS` column to `SCHEDULE_REASON`, the `CLONE_SRC` column to `SRC_BE_ID`, and the `CLONE_DEST` column to `DEST_BE_ID`.
  - The data types of the `CREATE_TIME`, `SCHEDULE_TIME` and `FINISH_TIME` columns have been changed from `DOUBLE` to `DATETIME`.
- The `is_leader` label has been added to some FE metrics. [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Shared-data clusters using Microsoft Azure Blob Storage and Data Lake Storage Gen 2 as object storage will experience Data Cache failure after being upgraded to v4.0. The system will automatically reload the cache.
