---
displayed_sidebar: "English"
---

# StarRocks version 3.3

## 3.3.0

Release date: June 21, 2024

### New Features and Improvements

#### Shared-data Cluster

- Optimized the performance of Schema Evolution in shared-data clusters, reducing the time consumption of DDL changes to a sub-second level. For more information, see [Schema Evolution](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/#set-fast-schema-evolution).
- To satisfy the requirement for data migration from shared-nothing clusters to shared-data clusters, the community officially released the [StarRocks Data Migration Tool](https://docs.starrocks.io/docs/administration/data_migration_tool/). It can also be used for data synchronization and disaster recovery between shared-nothing clusters.
- [Preview] AWS Express One Zone Storage can be used as storage volumes, significantly improving read and write performance. For more information, see [CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/#properties).
- Optimized the garbage collection (GC) mechanism in shared-data clusters. Supports manual compaction for data in object storage. For more information, see [Manual Compaction](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/#manual-compaction-from-31).
- Optimized the Publish execution of Compaction transactions for Primary Key tables in shared-data clusters, reducing I/O and memory overhead by avoiding reading primary key indexes.
- Supports Internal Parallel Scan within tablets. This optimizes query performance in scenarios where there are very few buckets in the table, which limits query parallelism to the number of tablets. Users can enable the Parallel Scan feature by setting the following system variables:

  ```SQL
  SET GLOBAL enable_lake_tablet_internal_parallel = true;
  SET GLOBAL tablet_internal_parallel_mode = "force_split";
  ```

#### Data Lake Analytics

- **Data Cache enhancements**
  - Added the [Data Cache Warmup](https://docs.starrocks.io/docs/data_source/data_cache_warmup/) command CACHE SELECT to fetch hotspot data from data lakes, which speeds up queries and minimizes resource usage. CACHE SELECT can work with SUBMIT TASK to achieve periodic cache warmup. This feature supports both tables in external catalogs and internal tables in shared-data clusters.
  - Added metrics and monitoring methods to enhance the [observability of Data Cache](https://docs.starrocks.io/docs/data_source/data_cache_observe/).
- **Parquet reader performance enhancements**
  - Optimized Page Index, significantly reducing the data scan size.
  - Reduced the occurrence of reading unnecessary pages when Page Index is used.
  - Uses SIMD to accelerate the computation to determine whether data rows are empty.
- **ORC reader performance enhancements**
  - Uses column ID for predicate pushdown to read ORC files after Schema Change.
  - Optimized the processing logic for ORC tiny stripes.
- **Iceberg table format enhancements**
  - Significantly improved the metadata access performance of the Iceberg Catalog by refactoring the parallel Scan logic. Resolved the single-threaded I/O bottleneck in the native Iceberg SDK when handling large volumes of metadata files. As a result, queries with metadata bottlenecks now experience more than a 10-fold performance increase.
  - Queries on Parquet-formatted Iceberg v2 tables support [equality deletes](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/#usage-notes).
- **[Experimental] Paimon Catalog enhancements**
  - Materialized views created based on the Paimon external tables now support automatic query rewriting.
  - Optimized Scan Range scheduling for queries against the Paimon Catalog, improving I/O concurrency.
  - Support for querying Paimon system tables.
  - Paimon external tables now support DELETE Vectors, enhancing query efficiency in update and delete scenarios.
- **[Enhancements in collecting external table statistics](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#collect-statistics-of-hiveiceberghudi-tables)**
  - ANALYZE TABLE can be used to collect histograms of external tables, which helps prevent data skews.
  - Supports collecting statistics of STRUCT subfields.
- **Table sink enhancements**
  - The performance of the Sink operator is doubled compared to Trino.
  - Data can be sunk to Textfile- and ORC-formatted tables in [Hive catalogs](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) and storage systems such as HDFS and cloud storage like AWS S3.
- [Preview] Supports Alibaba Cloud [MaxCompute catalogs](https://docs.starrocks.io/docs/data_source/catalog/maxcompute_catalog/), with which you can query data from MaxCompute without ingestion and directly transform and load the data from MaxCompute by using INSERT INTO.
- [Experimental] Supports ClickHouse Catalog.
- [Experimental] Supports [Kudu Catalog](https://docs.starrocks.io/docs/data_source/catalog/kudu_catalog/).

#### Performance Improvement and Query Optimization

- **Optimized performance on ARM.**
  -  Significantly optimized performance for ARM architecture instruction sets. Performance tests under AWS Graviton instances showed that the ARM architecture was 11% faster than the x86 architecture in the SSB 100G test, 39% faster in the Clickbench test, 13% faster in the TPC-H 100G test, and 35% faster in the TPC-DS 100G test.
- **Spill to Disk is in GA.** Optimized the memory usage of complex queries and improved spill scheduling, allowing large queries to run stably without OOM.
- [Preview] Supports [spilling intermediate results to object storage](https://docs.starrocks.io/docs/administration/management/resource_management/spill_to_disk/).
- **Supports more indexes.**
  - [Preview] Supports [full-text inverted index](https://docs.starrocks.io/docs/table_design/indexes/inverted_index/) to accelerate full-text searches.
  - [Preview] Supports [N-Gram bloom filter index](https://docs.starrocks.io/docs/table_design/indexes/Ngram_Bloom_Filter_Index/) to speed up `LIKE` queries and the computation speed of `ngram_search` and `ngram_search_case_insensitive` functions.
- Improved the performance and memory usage of Bitmap functions. Added the capability to export Bitmap data to Hive by using [Hive Bitmap UDFs](https://docs.starrocks.io/docs/integrations/hive_bitmap_udf/).
- **[Preview] Supports [Flat JSON](https://docs.starrocks.io/docs/using_starrocks/Flat_json/).** This feature automatically detects JSON data during data loading, extracts common fields from the JSON data, and stores these fields in a columnar manner. This improves JSON query performance, comparable to querying STRUCT data.
- **[Preview] Optimized global dictionary.** Provides a dictionary object to store the mapping of key-value pairs from a dictionary table in the BE memory. A new `dictionary_get()` function is now used to directly query the dictionary object in the BE memory, accelerating the speed of querying the dictionary table compared to using the `dict_mapping()` function. Furthermore, the dictionary object can also serve as a dimension table. Dimension values can be obtained by directly querying the dictionary object using `dictionary_get()`, resulting in faster query speeds than the original method of performing JOIN operations on the dimension table to obtain dimension values.
- [Preview] Supports Colocate Group Execution. Significantly reduces memory usage for executing Join and Agg operators on the colocated tables, which ensures that large queries can be executed more stably.
- Optimized the performance of CodeGen. JIT is enabled by default, which achieves a 5X performance improvement for complex expression calculations.
- Supports using vectorization technology to implement regular expression matching, which reduces the CPU consumption of the `regexp_replace` function.
- Optimized Broadcast Join so that the Broadcast Join operation can be terminated in advance when the right table is empty.
- Optimized Shuffle Join in scenarios of data skew to prevent OOM.
- When an aggregate query contains `Limit`, multiple Pipeline threads can share the `Limit` condition to prevent compute resource consumption.

#### Storage Optimization and Cluster Management

- **[Enhanced flexibility of range partitioning](https://docs.starrocks.io/docs/table_design/Data_distribution/#range-partitioning).** Three time functions can be used as partitioning columns. These functions convert timestamps or strings in the partitioning columns into date values and then the data can be partitioned based on the converted date values.
- **FE memory observability.** Provides detailed memory usage metrics for each module within the FE to better manage resources.
- **[Optimized metadata locks in FE](https://docs.starrocks.io/docs/administration/management/FE_configuration/#lock_manager_enabled).** Provides Lock manager to achieve centralized management for metadata locks in FE. For example, it can refine the granularity of metadata lock from the database level to the table level, which improves load and query concurrency. In a scenario of 100 concurrent load jobs, the load time can be reduced by 35%.
- **[Supports adding labels on BEs](https://docs.starrocks.io/docs/administration/management/resource_management/be_label/).** Supports adding labels on BEs based on information such as the racks and data centers where BEs are located. It ensures even data distribution among racks and data centers, and facilitates disaster recovery in case of power failures in certain racks or faults in data centers.
- **[Optimized the sort key](https://docs.starrocks.io/docs/table_design/indexes/Prefix_index_sort_key/#usage-notes).** Duplicate Key tables, Aggregate tables, and Unique Key tables all support specifying sort keys through the `ORDER BY` clause.
- **[Experimental] Optimized the storage efficiency of non-string scalar data.** This type of data supports dictionary encoding, reducing storage space usage by 12%.
- **Supports size-tiered compaction for Primary Key tables.** Reduces write I/O and memory overhead during compaction. This improvement is supported in both shared-data and shared-nothing clusters.
- **Optimized read I/O for persistent indexes in Primary Key tables.** Supports reading persistent indexes by a smaller granularity (page) and improves the persistent index's bloom filter. This improvement is supported in both shared-data and shared-nothing clusters.
- Supports for IPv6. StarRocks now supports deployment on IPv6 networks.

#### Materialized Views

- **Supports view-based query rewrite.** With this feature enabled, queries against views can be rewritten to materialized views created upon those views. For more information, see [View-based materialized view rewrite](https://docs.starrocks.io/docs/using_starrocks/query_rewrite_with_materialized_views/#view-based-materialized-view-rewrite).
- **Supports text-based query rewrite.** With this feature enabled, queries (or their sub-queries) that have the same abstract syntax trees (AST) as the materialized views can be transparently rewritten. For more information, see [Text-based materialized view rewrite](https://docs.starrocks.io/docs/using_starrocks/query_rewrite_with_materialized_views/#text-based-materialized-view-rewrite).
- **[Preview] Supports setting transparent rewrite mode for queries directly against the materialized view.** When the `transparent_mv_rewrite_mode` property is enabled, StarRocks will automatically rewrite queries to materialized views. It will merge data from refreshed materialized view partitions with the raw data corresponding to the unrefreshed partitions using an automatic UNION operation. This mode is suitable for modeling scenarios where data consistency must be maintained while also aiming to control refresh frequency and reduce refresh costs. For more information, see [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters-1).
- Supports aggregation pushdown for materialized view query rewrite: When the `enable_materialized_view_agg_pushdown_rewrite` variable is enabled, users can use single-table asynchronous materialized views with [Aggregation Rollup](https://docs.starrocks.io/docs/using_starrocks/query_rewrite_with_materialized_views/#aggregation-rollup-rewrite) to accelerate multi-table join scenarios. Aggregate functions will be pushed down to the Scan Operator during query execution and rewritten by the materialized view before the Join Operator is executed, significantly improving query efficiency. For more information, see [Aggregation pushdown](https://docs.starrocks.io/docs/using_starrocks/query_rewrite_with_materialized_views/#aggregation-pushdown).
- **Supports a new property to control materialized view rewrite.** Users can set the `enable_query_rewrite` property to `false` to disable query rewrite based on a specific materialized view, reducing query rewrite overhead. If a materialized view is used only for direct query after modeling and not for query rewrite, users can disable query rewrite for this materialized view. For more information, see [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters-1).
- **Optimized the cost of materialized view rewrite.** Supports specifying the number of candidate materialized views and enhanced the filter algorithms. Introduced materialized view plan cache to reduce the time consumption of the Optimizer at the query rewrite phase. For more information, see `cbo_materialized_view_rewrite_related_mvs_limit`.
- **Optimized materialized views created upon Iceberg catalogs.** Materialized views based on Iceberg catalogs now support incremental refresh triggered by partition updates and partition alignment for Iceberg tables using Partition Transforms. For more information, see [Data lake query acceleration with materialized views](https://docs.starrocks.io/docs/using_starrocks/data_lake_query_acceleration_with_materialized_views/#choose-a-suitable-refresh-strategy).
- **Enhanced the observability of materialized views.** Improved the monitoring and management of materialized views for better system insights. For more information, see [Metrics for asynchronous materialized views](https://docs.starrocks.io/docs/administration/management/monitoring/metrics/#metrics-for-asynchronous-materialized-views).
- **Improved the efficiency of large-scale materialized view refresh.** Supports global FIFO scheduling, optimized the cascading refresh strategy for nested materialized views, and fixed some issues that occur in high-frequency refresh scenarios.
- **Supports refresh triggered by multiple fact tables.** Materialized views created upon multiple fact tables now support partition-level incremental refresh when data in any of the fact tables is updated, increasing data management flexibility. For more information, see [Align partitions with multiple base tables](https://docs.starrocks.io/docs/using_starrocks/create_partitioned_materialized_view/#align-partitions-with-multiple-base-tables).

#### SQL Functions

- DATETIME fields support microsecond precision. The new time unit is supported in related time functions and during data loading.
- Added the following functions:
  - [String functions](https://docs.starrocks.io/docs/cover_pages/functions_string/): crc32, url_extract_host, ngram_search
  - Array functions: [array_contains_seq](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - Date and time functions: [yearweek](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/yearweek/)
  - Math functions: [cbrt](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cbrt/)

#### Ecosystem Support

- [Experimental] Provides [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer), a new tool for converting the syntax in ClickHouse to the syntax in StarRocks.
- The Flink connector v1.2.9 provided by StarRocks is integrated with the Flink CDC 3.0 framework, which can build a streaming ELT pipeline from CDC data sources to StarRocks. The pipeline can synchronize the entire database, sharded tables, and schema changes in the sources to StarRocks. For more information, see [Synchronize data with Flink CDC 3.0 (with schema change supported)](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#synchronize-data-with-flink-cdc-30-with-schema-change-supported).

### Behavior Changes (To be updated)

- The default value of the materialized view property `partition_refresh_num` has been changed from `-1` to `1`. When a partitioned materialized view needs to be refreshed, instead of refreshing all partitions in a single task, the new behavior will incrementally refresh one partition at a time. This change is intended to prevent excessive resource consumption caused by the original behavior. The default behavior can be adjusted using the FE configuration `default_mv_partition_refresh_number`.
- Originally, the database consistency checker was scheduled based on GMT+8 time zone. Database consistency checker is scheduled based on the local time zone now. [#45748](https://github.com/StarRocks/starrocks/issues/45748)
- By default, Data Cache is enabled to accelerate data lake queries. Users can manually disable it by executing `SET enable_scan_datacache = false`. 
- If users want to re-use the cached data in Block Cache after downgrading a shared-data cluster from v3.3 to v3.2.8 and earlier, they need to manually rename the Blockfile in the directory **starlet_cache** by changing the file name format from `blockfile_{n}.{version}` to `blockfile_{n}`, that is, to remove the suffix of version information. For more information, refer to the [Block Cache Usage Notes](https://docs.starrocks.io/docs/using_starrocks/block_cache/#usage-notes). v3.2.9 and later versions are compatible with the file name format in v3.3, so users do not need to perform this operation manually.

### Parameter Changes (To be updated)

- Supports dynamically modifying FE parameter `sys_log_level`. [#45062](https://github.com/StarRocks/starrocks/issues/45062)

### Bug Fixes

Fixed the following issues:

- Query results are incorrect when queries are rewritten to materialized views created by using UNION ALL. [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- Extra columns are read when queries with predicates are rewritten to materialized views during query execution. [#45272](https://github.com/StarRocks/starrocks/issues/45272)
- The results of functions `next_day` and `previous_day` are incorrect. [#45343](https://github.com/StarRocks/starrocks/issues/45343)
- Schema change fails because of replica migration. [#45384](https://github.com/StarRocks/starrocks/issues/45384)
- Restoring a table with full-text inverted index causes BEs to crash. [#45010](https://github.com/StarRocks/starrocks/issues/45010)
- Duplicate data rows are returned when an Iceberg catalog is used to query data. [#44753](https://github.com/StarRocks/starrocks/issues/44753)
- Low cardinality dictionary optimization does not take effect on `ARRAY<VARCHAR>`-type columns in Aggregate tables. [#44702](https://github.com/StarRocks/starrocks/issues/44702)
- Query results are incorrect when queries are rewritten to materialized views created by using UNION ALL. [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- If BEs are compiled with ASAN, BEs crash when the cluster is started and the `be.warning` log shows `dict_func_expr == nullptr`. [#44551](https://github.com/StarRocks/starrocks/issues/44551)
- Query results are incorrect when aggregate queries are performed on single-replica tables. [#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join rewrite fails. [#43788](https://github.com/StarRocks/starrocks/issues/43788)
- BEs crash after the column type is modified from VARCHAR to DECIMAL. [#44406](https://github.com/StarRocks/starrocks/issues/44406)
- When a table with List partitioning is queried by using a not-equal operator, partitions are incorrectly pruned, resulting in wrong query results. [#42907](https://github.com/StarRocks/starrocks/issues/42907)
- Leader FE's heap size increases quickly as many Stream Load jobs using non-transactional interface finishes. [#43715](https://github.com/StarRocks/starrocks/issues/43715)

### Downgrade notes

To downgrade a cluster from v3.3.0 or later to v3.2, users must follow these steps:

1. Ensure that all ALTER TABLE SCHEMA CHANGE transactions initiated in the v3.3 cluster are either completed or canceled before downgrading.
2. Clear all transaction history by executing the following command:

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. Verify that there are no remaining historical records by running the following command:

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```

4. Execute the following command to create an image file for your metadata:

   ```sql
   ALTER SYSTEM CREATE IMAGE;
   ```

5. After the new image file is transmitted to the directory **meta/image** of all FE nodes, you can first downgrade a Follower FE node. If no error is returned, you can then downgrade other nodes in the cluster.
