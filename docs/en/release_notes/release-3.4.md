---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.0 RC-01

Release date: January 13, 2025

### Data Lake Analytics

- Optimized Iceberg V2 query performance and lowered memory usage by reducing repeated reads of delete-files.
- Supports column mapping for Delta Lake tables, allowing queries against data after Delta Schema Evolution.
- Data Cache related improvements:
  - Introduces a Segmented LRU (SLRU) Cache eviction strategy, which significantly defends against cache pollution from occasional large queries, improves cache hit rate, and reduces fluctuations in query performance. In simulated test cases with large queries, SLRU-based query performance can be improved by 70% or even higher. 
  - Unified the Data Cache instance used in both shared-data architecture and data lake query scenarios to simplify the configuration and improve resource utilization.
  - Provides an adaptive I/O strategy optimization for Data Cache, which flexibly routes some query requests to remote storage based on the cache disk's load and performance, thereby enhancing overall access throughput.
- Supports automatic collection of external table statistics through automatic ANALYZE tasks triggered by queries. It can provide more accurate NDV information compared to metadata files, thereby optimizing the query plan and improving query performance.

<!--
- Provides Time Travel query capability for Iceberg, allowing data to be read from a specified BRANCH or TAG by specifying TIMESTAMP or VERSION.
- Supports asynchronous delivery of query fragments for data lake queries. It avoids the restriction that FE must obtain all files to be queried before BE can execute a query, thus allowing FE to fetch query files and BE to execute queries in parallel, and reducing the overall latency of data lake queries involving a large number of files that are not in the cache. Meanwhile, it reduces the memory load on FE due to caching the file list and improves query stability. (Currently, the optimization for Hudi and Delta Lake is implemented, while the optimization for Iceberg is still under development.)
-->

### Performance Improvement and Query Optimization

- [Experimental] Offers a preliminary Query Feedback feature for automatic optimization of slow queries. The system will collect the execution details of slow queries, automatically analyze its query plan for potential opportunities for optimization, and generate a tailored optimization guide for the query. If CBO generates the same bad plan for subsequent identical queries, the system will locally optimize this query plan based on the guide.
- [Experimental] Supports Python UDFs, offering more convenient function customization compared to Java UDFs.

<!--
- [Experimental] Supports Arrow Flight interface for more efficient reading of large data volumes in query results. It also allows BE, instead of FE, to process the returned results, greatly reducing the pressure on FE. It is especially suitable for business scenarios involving big data analysis and processing, and machine learning.
- Enables the pushdown of multi-column OR predicates, allowing queries with multi-column OR conditions (for example, `a = xxx OR b = yyy`) to utilize certain column indexes, thus reducing data read volume and improving query performance.
- Optimized TPC-DS query performance by roughly 20% under the TPC-DS 1TB Iceberg dataset. Optimization methods include table pruning and aggregated column pruning using primary and foreign keys, and aggregation pushdown.
-->

### Shared-data Enhancements

- Supports Query Cache, aligning the shared-nothing architecture.
- Supports synchronous materialized views, aligning the shared-nothing architecture.

### Storage Engine

- Unified the expression partitioning syntax to support multi-level partitioning, where each level can be any expression.

<!--
- [Preview] Supports all native aggregate functions in Aggregate tables. By introducing a generic aggregate function state storage framework, all native aggregate functions supported by StarRocks can be used to define an Aggregate table.
- Supports vector indexes, enabling fast approximate nearest neighbor searches (ANNS) of large-scale, high-dimensional vectors, which are commonly required in deep learning and machine learning scenarios. Currently, StarRocks supports two types of vector indexes: IVFPQ and HNSW.
-->

## Loading

- INSERT OVERWRITE now supports a new semantic - Dynamic Overwrite. When this semantic is enabled, the ingested data will either create new partitions or overwrite existing partitions that correspond to the new data records. Partitions not involved will not be truncated or deleted. This semantic is especially useful when users want to recover data in specific partitions without specifying the partition names.
- Optimized the data ingestion with INSERT from FILES to replace Broker Load as the preferred loading method:
  - FILES now supports listing files in remote storage, and providing basic statistics of the files.
  - INSERT now supports matching columns by name, which is especially useful when users load data from columns with identical names. (The default behavior matches columns by their position.)
  - INSERT supports specifying PROPERTIES, aligning with other loading methods. Users can specify `strict_mode`, `max_filter_ratio`, and `timeout` for INSERT operations to control and behavior and quality of the data ingestion.
  - INSERT from FILES supports pushing down the target table schema check to the Scan stage of FILES to infer a more accurate source data schema.
  - FILES supports unionizing files with different schema. The schema of Parquet and ORC files are unionized based on the column names, and that of CSV files are unionized based on the position (order) of the columns. When there are mismatched columns, users can choose to fill the columns with NULL or return an error by specifying the property `fill_mismatch_column_with`.
  - FILES supports inferring the STRUCT type data from Parquet files. (In earlier versions, STRUCT data is inferred as STRING type.)

<!--
- Supports merging multiple concurrent Stream Load requests into a single transaction and committing data in a batch, thus improving the throughput of real-time data ingestion. It is designed for high-concurrency, small-batch (from KB to tens of MB) real-time loading scenarios. It can reduce the excessive data versions caused by frequent loading operations, resource consumption during Compaction, and IOPS and I/O latency brought by excessive small files.
-->

### Others

- Optimized the graceful exit process of BE and CN by accurately displaying the status of BE or CN nodes during a graceful exit as `SHUTDOWN`.
- Optimized log printing to avoid excessive disk space being occupied.

<!--
- Shared-nothing clusters now support backing up and restoring more objects: logical view, external catalog metadata, and partitions created with expression partitioning and list partitioning strategies.
- [Preview] Supports CheckPoint on Follower FE to avoid excessive memory on Leader FE during CheckPoint, thereby improving the stability of Leader FE.
-->

### Downgrade Notes

- Clusters can be downgraded from v3.4.0 only to v3.3.9 and later.
