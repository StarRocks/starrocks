---
displayed_sidebar: "English"
---

# StarRocks version 3.3

## 3.3.0-RC01

Release date: May 10, 2024

### Shared-data Cluster

- Optimized the performance of Schema Evolution in shared-data clusters, reducing the time consumption of DDL changes to a sub-second level. For more information, see [Schema Evolution](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_TABLE/#set-fast-schema-evolution).
- Optimized the garbage collection (GC) mechanism in shared-data clusters. Supports manual compaction for data in object storage. For more information, see [Manual Compaction](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/#manual-compaction-from-31).
- To satisfy the requirement for data migration from shared-nothing clusters to shared-data clusters, the community officially released the [StarRocks Data Migration Tool](https://docs.starrocks.io/docs/3.3/administration/data_migration_tool/). It can also be used for data synchronization and disaster recovery between shared-nothing clusters.
- AWS Express One Zone Storage can be used as storage volumes, significantly improving read and write performance. For more information, see [CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/#properties).
- Optimized the Publish execution of Compaction transactions for Primary Key tables in shared-data clusters, reducing I/O and memory overhead by avoiding reading primary key indexes.

### Data Lake Analytics

- **Data Cache enhancements**
  - Added the [Data Cache Warmup](https://docs.starrocks.io/docs/3.3/data_source/data_cache_warmup/) command CACHE SELECT to fetch hotspot data from data lakes, which speeds up queries and minimizes resource usage. CACHE SELECT can work with SUBMIT TASK to achieve periodic cache warmup.
  - Added metrics and monitoring methods to enhance the [observability of Data Cache](https://docs.starrocks.io/docs/3.3/data_source/data_cache_observe/).
- **Parquet reader performance enhancements**
  - Optimized Page Index, significantly reducing the data scan size.
  - Reduced the occurrences of reading unnecessary pages when Page Index is used.
  - Uses SIMD to accelerate the computation to determine whether data rows are empty.
- **ORC reader performance enhancements**
  - Uses column ID for predicate pushdown to read ORC files after Schema Change.
  - Optimized the processing logic for ORC tiny stripes.
- **Iceberg table format enhancements**
  - Queries on Parquet-formatted Iceberg v2 tables support [equality deletes](https://docs.starrocks.io/docs/3.3/data_source/catalog/iceberg_catalog/#usage-notes).
- **[Enhancements in collecting external table statistics](https://docs.starrocks.io/docs/3.3/using_starrocks/Cost_based_optimizer/#collect-statistics-of-hiveiceberghudi-tables)**
  - ANALYZE TABLE can be used to collect histograms of external tables, which helps prevent data skews.
  - Supports collecting statistics of STRUCT subfields.
- **Table sink enhancements**
  - The performance of the Sink operator is doubled compared to Trino.
  - Data can be sunk to Textfile- and ORC-formatted tables in [Hive catalogs](https://docs.starrocks.io/docs/3.3/data_source/catalog/hive_catalog/) and storage systems such as HDFS and cloud storage like AWS S3.
- Supports Alibaba Cloud [MaxCompute catalogs](https://docs.starrocks.io/docs/3.3/data_source/catalog/maxcompute_catalog/), with which you can query data from MaxCompute without ingestion and directly transform and load the data from MaxCompute by using INSERT INTO.

### Performance Improvement and Query Optimization

- **Spill to Disk is in GA:** Optimized the memory usage of complex queries and improved spill scheduling, allowing large queries to run stably without OOM.
- **Supports more indexes:**
  - Supports [full-text inverted index](https://docs.starrocks.io/docs/3.3/table_design/indexes/inverted_index/) to accelerate full-text searches.
  - Supports [N-Gram bloom filter index](https://docs.starrocks.io/docs/3.3/table_design/indexes/Ngram_Bloom_Filter_Index/) to speed up `LIKE` queries and the computation speed of `ngram_search` and `ngram_search_case_insensitive` functions.
- Improved the performance and memory usage of Bitmap functions. Added the capability to export Bitmap data to Hive by using [Hive Bitmap UDFs](https://docs.starrocks.io/docs/3.3/integrations/hive_bitmap_udf/).
- **Supports [Flat JSON](https://docs.starrocks.io/docs/3.3/using_starrocks/Flat_json/):** This feature automatically detects JSON data during data loading, extracts common fields from the JSON data, and stores these fields in a columnar manner. This improves JSON query performance, comparable to querying STRUCT data.
- **Optimized global dictionary:** provides a dictionary object to store the mapping of key-value pairs from a dictionary table in the BE memory. A new `dictionary_get()` function is now used to directly query the dictionary object in the BE memory, accelerating the speed of querying the dictionary table compared to using the `dict_mapping()` function. Furthermore, the dictionary object can also serve as a dimension table. Dimension values can be obtained by directly querying the dictionary object using `dictionary_get()`, resulting in faster query speeds than the original method of performing JOIN operations on the dimension table to obtain dimension values.
- Supports Colocate Group Execution: significantly reduces memory usage for executing Join and Agg operators on the colocated tables, which ensures that large queries can be executed more stably.
- Optimized the performance of CodeGen: JIT is enabled by default, which achieves a 5X performance improvement for complex expression calculations.
- Supports using vectorization technology to implement regular expression matching, which reduces the CPU consumption of the `regexp_replace` function.
- Optimized Broadcast Join so that the Broadcast Join operation can be terminated in advance when the right table is empty.
- Optimized Shuffle Join in scenarios of data skew to prevent OOM.
- When an aggregate query contains `Limit`, multiple Pipeline threads can share the `Limit` condition to prevent compute resource consumption.

### Storage Optimization and Cluster Management

- **[Enhanced flexibility of range partitioning](https://docs.starrocks.io/docs/3.3/table_design/Data_distribution/#range-partitioning):** Three time functions can be used as partitioning columns. These functions convert timestamps or strings in the partitioning columns into date values and then the data can be partitioned based on the converted date values.
- **FE memory observability**: Provides detailed memory usage metrics for each module within the FE to better manage resources.
- **[Optimized metadata locks in FE](https://docs.starrocks.io/docs/3.3/administration/management/FE_configuration/#lock_manager_enabled)**: Provides Lock manager to achieve centralized management for metadata locks in FE. For example, it can refine the granularity of metadata lock from the database level to the table level, which improves load and query concurrency. In a scenario of 100 concurrent load jobs, the load time can be reduced by 35%.
- **[Supports adding labels on BEs](https://docs.starrocks.io/docs/3.3/administration/management/resource_management/be_label/):** Supports adding labels on BEs based on information such as the racks and data centers where BEs are located. It ensures even data distribution among racks and data centers, and facilitates disaster recovery in case of power failures in certain racks or faults in data centers.
- **[Optimized the sort key](https://docs.starrocks.io/docs/3.3/table_design/indexes/Prefix_index_sort_key/#usage-notes):** Duplicate Key tables, Aggregate tables, and Unique Key tables all support specifying sort keys through the `ORDER BY` clause.
- **Optimized the storage efficiency of non-string scalar data:** This type of data supports dictionary encoding, reducing storage space usage by 12%.
- **Supports size-tiered compaction for Primary Key tables:** Reduces write I/O and memory overhead during compaction. This improvement is supported in both shared-data and shared-nothing clusters.
- **Optimized read I/O for persistent indexes in Primary Key tables:** Supports reading persistent indexes by a smaller granularity (page) and improves the persistent index's bloom filter. This improvement is supported in both shared-data and shared-nothing clusters.

### Materialized Views

- **Supports view-based query rewrite:** With this feature enabled, queries against views can be rewritten to materialized views created upon those views. For more information, see [View-based materialized view rewrite](https://docs.starrocks.io/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#view-based-materialized-view-rewrite).
- **Supports text-based query rewrite:** With this feature enabled, queries (or their sub-queries) that have the same abstract syntax trees (AST) as the materialized views can be transparently rewritten. For more information, see [Text-based materialized view rewrite](https://docs.starrocks.io/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#text-based-materialized-view-rewrite).
- **Supports a new property to control materialized view rewrite:** Users can set the `enable_query_rewrite` property to `false` to disable query rewrite based on a specific materialized view, reducing query rewrite overhead. For more information, see [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters-1).
- **Optimized the cost of materialized view rewrite.**
  - Supports specifying the number of candidate materialized views and enhanced the filter algorithms. For more information, see [`cbo_materialized_view_rewrite_related_mvs_limit`](https://docs.starrocks.io/docs/3.3/reference/System_variable/#cbo_materialized_view_rewrite_related_mvs_limit).
  - Supports materialized view plan cache to reduce the time consumption of the Optimizer at the query rewrite phase.
- **Optimized materialized views created upon Iceberg catalogs:** Materialized views based on Iceberg catalogs now support incremental refresh triggered by partition updates and partition alignment for Iceberg tables using Partition Transforms. For more information, see [Data lake query acceleration with materialized views](https://docs.starrocks.io/docs/3.3/using_starrocks/data_lake_query_acceleration_with_materialized_views/#choose-a-suitable-refresh-strategy).
- **Enhanced the observability of materialized views:** Improved the monitoring and management of materialized views for better system insights. For more information, see [Metrics for asynchronous materialized views](https://docs.starrocks.io/docs/3.3/administration/management/monitoring/metrics/#metrics-for-asynchronous-materialized-views).
- **Improved the efficiency of large-scale materialized view refresh.**
- **Supports refresh triggered by multiple fact tables:** Materialized views created upon multiple fact tables now support partition-level incremental refresh when data in any of the fact tables is updated, increasing data management flexibility. For more information, see [Align partitions with multiple base tables](https://docs.starrocks.io/docs/3.3/using_starrocks/create_partitioned_materialized_view/#align-partitions-with-multiple-base-tables).

### SQL Functions

- DATETIME fields support microsecond precision. The new time unit is supported in related time functions and during data loading.
- Added the following functions:
  - [String functions](https://docs.starrocks.io/docs/3.3/cover_pages/functions_string/): crc32, url_extract_host, ngram_search
  - Array functions: [array_contains_seq](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - Date and time functions: [yearweek](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/date-time-functions/yearweek/)
  - Math functions: [cbrt](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/math-functions/cbrt/)

### Ecosystem Support

- Provides [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer), a new tool for converting the syntax in Clickhouse to the syntax in StarRocks.
- The Flink connector v1.2.9 provided by StarRocks is integrated with the Flink CDC 3.0 framework, which can build a streaming ELT pipeline from CDC data sources to StarRocks. The pipeline can synchronize the entire database, sharded tables, and schema changes in the sources to StarRocks. For more information, see [Synchronize data with Flink CDC 3.0 (with schema change supported)](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#synchronize-data-with-flink-cdc-30-with-schema-change-supported).

### Bug Fixes

Fixed the following issues:

- Query results are incorrect when queries are rewritten to materialized views created by using UNION ALL. [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- If BEs are compiled with ASAN, BEs crash when the cluster is started and the `be.warning` log shows `dict_func_expr == nullptr`. [#44551](https://github.com/StarRocks/starrocks/issues/44551)
- Query results are incorrect when aggregate queries are performed on single-replica tables. [#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join rewriting fails. [#43788](https://github.com/StarRocks/starrocks/issues/43788)
- BEs crash after the column type is modified from VARCHAR to DECIMAL. [#44406](https://github.com/StarRocks/starrocks/issues/44406)
- When a table with List partitioning is queried by using a not equal operator, partitions are incorrectly pruned, resulting in wrong query results. [#42907](https://github.com/StarRocks/starrocks/issues/42907)
- Leader FE's heap size increases quickly as many Stream Load jobs using non-transactional interface finishes. [#43715](https://github.com/StarRocks/starrocks/issues/43715)
