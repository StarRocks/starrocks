---
displayed_sidebar: "English"
---

# StarRocks version 3.3

## 3.3.0 RC01

Release date: May 10, 2024

### Shared-data Cluster

- Optimized the performance of Schema Evolution in shared-data clusters, reducing the time consumption of DDL changes to a sub-second level. For more information, refer to [Schema Evolution](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_TABLE/#set-fast-schema-evolution).
- Optimized the garbage collection (GC) mechanism in shared-data clusters. Supports manual compaction for data in object storage. For more information, refer to [Manual Compaction](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/#manual-compaction-from-31).
- To satisfy the requirement for data migration from shared-nothing clusters to shared-data clusters, the community officially released the [StarRocks Data Migration Tool](https://docs.starrocks.io/docs/3.3/administration/data_migration_tool/). It also can be used for data synchronization and disaster recovery between shared-nothing clusters.
- Storage volumes support AWS Express One Zone Storage, significantly improving read and write performance. For more information, refer to [CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/#properties).
- Optimized the Publish execution of Compaction transactions for Primary Key tables in shared-data clusters, reducing I/O and memory overhead by avoiding reading primary key indexes.

### Data Lake Analytics

- **Data Cache enhancements**
  - Added the [Data Cache Warmup](https://docs.starrocks.io/docs/3.3/data_source/data_cache_warmup/) command CACHE SELECT to fetch hotspot data from data lakes, which speeds up queries and minimizes resource usage. CACHE SELECT can work with SUBMIT TASK to achieve periodic cache warmup.
  - Added many metrics and monitoring methods to enhance the [observability of Data Cache](https://docs.starrocks.io/docs/3.3/data_source/data_cache_observe/.).
- **Iceberg table format enhancements**
  -  Queries on Parquet-formatted Iceberg v2 tables support [equality deletes](https://docs.starrocks.io/docs/3.3/data_source/catalog/iceberg_catalog/#usage-notes.).
- **Enhancements in collecting external table statistics**
  - ANALYZE TABLE can be used to collect histograms of external tables, which is efficient in preventing data skews.
  - Supports collecting statistics of STRUCT subfields.
- **Table sink enhancements**
  - The performance of the Sink operator is doubled compared to Trino.
  - Data can be sunk to Textfile- and ORC-formatted tables in [Hive catalogs](https://docs.starrocks.io/docs/3.3/data_source/catalog/hive_catalog/) and storage systems such as HDFS and cloud storage like AWS S3.
- Supports Alibaba Cloud [MaxCompute catalogs](https://docs.starrocks.io/docs/3.3/data_source/catalog/maxcompute_catalog/), with which you can query data from MaxCompute without ingestion and directly transform and load the data from MaxCompute by using INSERT INTO.
- Memory Consumption Management: Implement measures to limit memory consumption during high concurrency operations.

### Performance Improvement and Query Optimization

- **GA for Spill to Disk:** optimized the memory usage of complex queries, and improved spill scheduling, allowing large queries to be stably executed without OOM.
- **Supports more indexes:**
  - Supports [full-text inverted index](https://docs.starrocks.io/docs/3.3/table_design/indexes/inverted_index/) to accelerate full-text searches.
  - Supports [N-Gram bloom filter index](https://docs.starrocks.io/docs/3.3/table_design/indexes/Ngram_Bloom_Filter_Index/) to speed up `LIKE` queries or the computation speed of `ngram_search` and `ngram_search_case_insensitive` functions.
- Improved the performance and memory usage of Bitmap functions. Added the capability to export Bitmap data to Hive by using [Hive Bitmap UDFs](https://docs.starrocks.io/docs/3.3/integrations/hive_bitmap_udf/.).
- **Supports [Flat JSON](https://docs.starrocks.io/docs/3.3/using_starrocks/Flat_json/:):** Automatically detects JSON data during data loading, extracts common fields from the JSON data, and stores these fields in a columnar manner. This improves JSON query performance, comparable to querying STRUCT data.
- **Optimized global dictionary:** provides a dictionary object to store the mapping of key-value pairs from a dictionary table in the memory of each BE node. By using the `dictionary_get()` function to directly query the dictionary object in the BE memory, the speed is faster compared to using the `dict_mapping` function to query the dictionary table. Furthermore, the dictionary object can also serve as a dimension table. Dimension values can be obtained by directly querying the dictionary object through the `dictionary_get()` function, resulting in faster query speeds compared to the original method of performing JOIN operations on the dimension table to obtain dimension values.
- Supports Colocate Group Execution: significantly reduced memory usage for conducting Join and Agg operators on the colocated tables, which ensures that large queries can be executed more stably.
- Optimized the performance of CodeGen: JIT is enabled by default, which can achieve a 5X performance improvement for complex expression calculations.
- Supports using vectorization technology to implement regular expressions matching, which can reduce the CPU consumption of the `regexp_replace` function.
- Optimized Broadcast Join so that the Broadcast Join operation can be terminated in advance when the right table is empty.
- Optimized Shuffle Join in scenarios of data skew to prevent OOM.
- When an aggregate query contains `Limit`, multiple Pipeline threads can share the `Limit` condition to prevent wasting computational resources.

### Storage Optimization and Cluster Management

- **[Enhanced flexibility of range partitioning](https://docs.starrocks.io/docs/3.3/table_design/Data_distribution/#range-partitioning):** three specific time functions can be used as partitioning columns, so that these functions can convert timestamps or strings in the  partitioning columns into date values and then the data can be partitioned based on the converted date values.
- **FE memory observability**: Provides detailed memory usage metrics for each module within the FE to better manage resources.
- **[Optimized metadata locks in FE](https://docs.starrocks.io/zh/docs/3.3/administration/management/FE_configuration/#lock_manager_enabled)**: provides Lock manager to perform centralized management for metadata locks in FE. For example, it can refine the granularity of metadata lock from the database level to the table level, which can improve load and queries concurrency. In a scenario of 100 concurrent data loading jobs, loading time can be reduced by 35%.
- **[Supports adding labels on BEs](https://docs.starrocks.io/docs/3.3/administration/management/resource_management/be_label/):** supports adding labels on BEs based on information such as the racks and data centers where BEs are located. It ensures that data can be evenly distributed among racks, data centers or others, to address disaster recovery requirements in case that certain racks lose power or data centers encounter failures.
- **[Optimized the sort key](https://docs.starrocks.io/docs/3.3/table_design/indexes/Prefix_index_sort_key/#usage-notes):** Duplicate Key tables, Aggregate tables, and Unique Key tables all support specifying sort keys through the `ORDER BY` clause.
- **Optimized the storage efficiency of non-string scalar data:** This type of data supports dictionary encoding, which reduces storage space by 12%.
- **Supports the Size-tiered compaction strategy for primary key tables:** reduced write IO and memory overhead during compaction. This improvement is supported in both shared-data and shared-nothing StarRocks clusters.
- **Optimized read IO for persistent index in primary key tables:** supports reading persistent index by a smaller granularity (page) and improves the persistent index's bloom filter. This improvement is supported in both shared-data and shared-nothing StarRocks clusters.

### Materialized Views

- **Supports view-based query rewrite.** With this feature enabled, queries against views can be rewritten to materialized views created upon those views. For more information, refer to [View-based materialized view rewrite](https://docs.starrocks.io/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#view-based-materialized-view-rewrite).
- **Supports text-based query rewrite.** With this feature enabled, queries (or their sub-queries) that have the same abstract syntax trees as the materialized views can be transparently rewritten. For more information, refer to [Text-based materialized view rewrite](https://docs.starrocks.io/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#text-based-materialized-view-rewrite).
- **Supports a new property for materialized view rewrite control.** Users can set the `enable_query_rewrite` property to `false` to disable query rewrite based on a specific materialized view, reducing query rewrite overhead. For more information, refer to [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters-1).
- **Optimized the cost of materialized view rewrite.**
  - Supports specifying the number of candidate materialized views and enhanced filtering algorithms. For more information, refer to [`cbo_materialized_view_rewrite_related_mvs_limit`](https://docs.starrocks.io/docs/3.3/reference/System_variable/#cbo_materialized_view_rewrite_related_mvs_limit).
  - Supports materialized view plan cache to reduce the time consumption of the Optimizer at the query rewrite phase.
- **Optimized materialized views created upon Iceberg catalogs.** Materialized views based on Iceberg catalogs now support incremental refresh triggered by partition updates and partition alignment for Iceberg tables with Partition Transforms. For more information, refer to [Data lake query acceleration with materialized views](https://docs.starrocks.io/docs/3.3/using_starrocks/data_lake_query_acceleration_with_materialized_views/#choose-a-suitable-refresh-strategy).
- **Enhanced the observability of materialized views.** Improved the monitoring and management of materialized views for better system insights. For more information, refer to [Metrics for asynchronous materialized views](https://docs.starrocks.io/docs/3.3/administration/management/monitoring/metrics/#metrics-for-asynchronous-materialized-views).
- **Improved the efficiency of large-scale materialized view refresh.**
- **Supports refresh triggered by multiple fact tables.** Materialized views created upon multiple fact tables now support partition-level incremental refresh when data updates in any of the fact tables, increasing data management flexibility. For more information, refer to [Align partitions with multiple base tables](https://docs.starrocks.io/docs/3.3/using_starrocks/create_partitioned_materialized_view/#align-partitions-with-multiple-base-tables).

### SQL Functions

- DATETIME fields support microsecond precision. The new unit is supported in related time functions and data ingestion.
- Added the following functions:
  - [String functions](https://docs.starrocks.io/docs/3.3/cover_pages/functions_string/:): crc32, url_extract_host, ngram_search
  - Array functions: [array_contains_seq](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - Date and time functions: [yearweek](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/date-time-functions/yearweek/)
  - Math functions: [crbt](https://docs.starrocks.io/docs/3.3/sql-reference/sql-functions/math-functions/cbrt/)

### Ecosystem Support

- Provides [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer), a new tool for converting the syntax in Clickhouse to the syntax in StarRocks.
- The Flink connector v1.2.9 provided by StarRocks is integrated with the Flink CDC 3.0 framework, which can construct a streaming ELT pipeline from CDC data sources to StarRocks. The pipeline can synchronize whole database, merged sharding tables, and schema changes from sources to StarRocks. For more information,  see [Synchronize data with Flink CDC 3.0 (with schema change supported)](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#synchronize-data-with-flink-cdc-30-with-schema-change-supported).

### Bug Fixes

Fixed the following issues:

- Query results are incorrect when queries are rewritten to materialized views created by using UNION ALL. [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- If  BEs are compiled with ASAN, BEs crash when the cluster is used and the be.warning log shows `dict_func_expr == nullptr`. [#44551](https://github.com/StarRocks/starrocks/issues/44551)
- Query results are incorrect when aggregate queries are performed on single-replica tables. [#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join rewriting fails. [#43788](https://github.com/StarRocks/starrocks/issues/43788)
- BEs crash after the type of a column is modified from VARCHAR to DECIMAL. [#44406](https://github.com/StarRocks/starrocks/issues/44406)
- When a table with List partitioning is queried by using a not equal operator, partitions are incorrectly pruned, resulting in wrong query results. [#42907](https://github.com/StarRocks/starrocks/issues/42907)
- Leader FE's heap size increases quickly as many Stream Load jobs with non-transactional  interface finishes. [#43715](https://github.com/StarRocks/starrocks/issues/43715)
