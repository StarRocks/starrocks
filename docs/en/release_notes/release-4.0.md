---
displayed_sidebar: docs
---

# StarRocks version 4.0

## 4.0.0-RC02

Release Date: September 29, 2025

### New Features

- Supports setting sort keys when creating Iceberg tables.
- Supports Multi-Table Write-Write Transactions, allowing users to atomically commit `INSERT`, `UPDATE`, and `DELETE` operations. These transactions are compatible with both Stream Load and `INSERT INTO` interfaces, ensuring cross-table consistency in ETL and real-time ingestion scenarios.
- Supports modifying aggregation keys of aggregate tables.

### Improvements

- Optimized Delta Lake Catalog cache configuration: adjusted default values of `DELTA_LAKE_JSON_META_CACHE_TTL` and `DELTA_LAKE_CHECKPOINT_META_CACHE_TTL` to 24 hours, and simplified Parquet handler logic. [#63441](https://github.com/StarRocks/starrocks/pull/63441)
- Improved Delta Lake Catalog error log format and content for better debugging. [#63389](https://github.com/StarRocks/starrocks/pull/63389)
- External groups (e.g., LDAP Group) now support role grant/revoke and display, improving SQL syntax and test coverage for stronger access control. [#63385](https://github.com/StarRocks/starrocks/pull/63385)
- Strengthened Stream Load parameter consistency checks to reduce risks caused by parameter drift. [#63347](https://github.com/StarRocks/starrocks/pull/63347)
- Optimized Stream Load label passing mechanism to reduce dependencies. [#63334](https://github.com/StarRocks/starrocks/pull/63334)
- Improved `ANALYZE PROFILE` format: ExplainAnalyzer now supports grouping metrics by operator. [#63326](https://github.com/StarRocks/starrocks/pull/63326)
- Enhanced `QueryDetailActionV2` and `QueryProfileActionV2` APIs to return results in JSON format. [#63235](https://github.com/StarRocks/starrocks/pull/63235)
- Improved predicate parsing in scenarios with large numbers of CompoundPredicates. [#63139](https://github.com/StarRocks/starrocks/pull/63139)
- Adjusted certain FE metrics to be leader-aware. [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Enhanced `SHOW PROCESS LIST` with Catalog and Query ID information. [#62552](https://github.com/StarRocks/starrocks/pull/62552)
- Improved BE JVM memory monitoring metrics. [#62210](https://github.com/StarRocks/starrocks/pull/62210)
- Optimized materialized view rewrite logic and log outputs. [#62985](https://github.com/StarRocks/starrocks/pull/62985)
- Optimized random bucketing strategy. [#63168](https://github.com/StarRocks/starrocks/pull/63168)
- Supports resetting `AUTO_INCREMENT` start value with `ALTER TABLE <table_name> AUTO_INCREMENT = 10000;`. [#62767](https://github.com/StarRocks/starrocks/pull/62767)
- Group Provider now supports matching groups by DN. [#62711](https://github.com/StarRocks/starrocks/pull/62711)

### Bug Fixes

The following issues have been fixed:

- Incomplete `Left Join` results caused by ARRAY low-cardinality optimization. [#63419](https://github.com/StarRocks/starrocks/pull/63419)
- Incorrect execution plan generated after materialized view aggregate pushdown rewrite. [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- Redundant warning logs printed in JSON field pruning scenarios when schema fields were not found. [#63414](https://github.com/StarRocks/starrocks/pull/63414)
- Infinite loop caused by SIMD Batch parameter errors when inserting DECIMAL256 data in ARM environments. [#63406](https://github.com/StarRocks/starrocks/pull/63406)
- Three storage-related issues: [#63398](https://github.com/StarRocks/starrocks/pull/63398)
  - Cache exception when disk path is empty.
  - Incorrect Azure cache key prefix.
  - S3 multipart upload failure.
- ZoneMap filter invalidation after CHAR-to-VARCHAR schema change with Fast Schema Evolution. [#63377](https://github.com/StarRocks/starrocks/pull/63377)
- ARRAY aggregation type analysis error caused by intermediate type `ARRAY<NULL_TYPE>`. [#63371](https://github.com/StarRocks/starrocks/pull/63371)
- Metadata inconsistency in partial updates based on auto-increment columns. [#63370](https://github.com/StarRocks/starrocks/pull/63370)
- Metadata inconsistency when deleting tablets or querying concurrently. [#63291](https://github.com/StarRocks/starrocks/pull/63291)
- Failure to create `spill` directory during Iceberg table writes. [#63278](https://github.com/StarRocks/starrocks/pull/63278)
- Ranger Hive Service permission changes not taking effect. [#63251](https://github.com/StarRocks/starrocks/pull/63251)
- Group Provider did not support `IF NOT EXISTS` and `IF EXISTS` clauses. [#63248](https://github.com/StarRocks/starrocks/pull/63248)
- Errors caused by using reserved keywords in Iceberg partitions. [#63243](https://github.com/StarRocks/starrocks/pull/63243)
- Prometheus metric format issue. [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- Version check failure when starting replication transactions with Compaction enabled. [#62663](https://github.com/StarRocks/starrocks/pull/62663)
- Missing Compaction Profile when File Bunding was enabled. [#62638](https://github.com/StarRocks/starrocks/pull/62638)
- Issues handling redundant replicas after Clone. [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- Delta Lake tables failed to find partition columns. [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- Materialized views did not support Colocation in shared-data clusters. [#62941](https://github.com/StarRocks/starrocks/pull/62941)
- Issues reading NULL partitions in Iceberg tables. [#62934](https://github.com/StarRocks/starrocks/pull/62934)
- SQL syntax error caused by single quotes in Histogram statistics MCV (Most Common Values). [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- `KILL ANALYZE` command not working. [#62842](https://github.com/StarRocks/starrocks/pull/62842)
- Failure collecting Stream Load profiles. [#62802](https://github.com/StarRocks/starrocks/pull/62802)
- Incorrect CTE reuse plan extraction. [#62784](https://github.com/StarRocks/starrocks/pull/62784)
- Rebalance failure due to incorrect BE selection. [#62776](https://github.com/StarRocks/starrocks/pull/62776)
- `User Property` priority is lower than `Session Variable`. [#63173](https://github.com/StarRocks/starrocks/pull/63173)

## 4.0.0-RC

Release date: September 9, 2025

### Data Lake Analytics

- Unified Page Cache and Data Cache for BE metadata, and adopted an adaptive strategy for scaling. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Optimized metadata file parsing for Iceberg statistics to avoid repetitive parsing. [#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Optimized COUNT/MIN/MAX queries against Iceberg metadata by efficiently skipping over data file scans, significantly improving aggregation query performance on large partitioned tables and reducing resource consumption. [#60385](https://github.com/StarRocks/starrocks/pull/60385)
- Supports compaction for Iceberg tables via procedure `rewrite_data_files`. 
- Supports Iceberg tables with hidden partitions, including creating, writing, and reading the tables. [#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Supports the TIME data type in the Paimon catalog. [#58292](https://github.com/StarRocks/starrocks/pull/58292)

<!--
- Optimized sorting on Iceberg tables.
-->

### Security and Authentication

- In scenarios where JWT authentication and the Iceberg REST Catalog are used, StarRocks supports the passthrough of user login information to Iceberg via the REST Session Catalog for subsequent data access authentication. [#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Supports vended credentials for the Iceberg catalog.

<!--
- Supports HTTPS via configuring certificates on the StarRocks FE side, enhancing system access security to meet encrypted transmission requirements on the cloud or intranet. [#56394](https://github.com/StarRocks/starrocks/pull/56394)
- Supports HTTPS communication between BE nodes to ensure the encryption and integrity of data transmission, preventing internal data leakage and Man-in-the-Middle attacks.[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### Storage Optimization and Cluster Management

- Introduced  the File Bundling optimization for the cloud-native table in shared-data clusters to automatically bundle the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. [#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Supports Kafka 4.0 for Routine Load.
- Supports full-text inverted indexes on Primary Key tables in shared-nothing clusters.
- Supports enabling case-insensitive processing on names of catalogs, databases, tables, views, and materialized views. [#61136](https://github.com/StarRocks/starrocks/pull/61136)
- Supports blacklisting Compute Nodes in shared-data clusters. [#60830](https://github.com/StarRocks/starrocks/pull/60830)
- Supports global connection ID. [#57256](https://github.com/StarRocks/starrocks/pull/57276)

<!--
- Supports Multi-Table Write-Write Transaction to allow users to control the atomic submission of INSERT, UPDATE, and DELETE operations. The transaction supports Stream Load and INSERT INTO interfaces, effectively guaranteeing cross-table consistency in ETL and real-time write scenarios.
- Supports modifying aggregate keys of Aggregate tables.
-->

### Query and Performance Improvement

- Supports DECIMAL256 data type, expanding the upper limit of precision from 38 to 76 bits. Its 256-bit storage provides better adaptability to high-precision financial and scientific computing scenarios, effectively mitigating DECIMAL128's precision overflow problem in very large aggregations and high-order operations. [#59645](https://github.com/StarRocks/starrocks/issues/59645)
- Optimized the performance of the JOIN and AGG operators. [#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] Introduced SQL Plan Manager to allow users to bind a query plan to a query, thereby preventing the query plan from changing due to system state changes (mainly data updates and statistics updates), thus stabilizing query performance. [#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Introduced Partition-wise Spillable Aggregate/Distinct operators to replace the original Spill implementation based on sorted aggregation, significantly improving aggregation performance and reducing read/write overhead in complex and high-cardinality GROUP BY scenarios. [#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2:
  - Supports configuring Flat JSON on the table level. [#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - Enhance JSON columnar storage by retaining the V1 mechanism while adding page- and segment-level indexes (ZoneMaps, Bloom filters), predicate pushdown with late materialization, dictionary encoding, and integration of a low-cardinality global dictionary to significantly boost execution efficiency. [#60953](https://github.com/StarRocks/starrocks/issues/60953)
- Supports an adaptive ZoneMap index creation strategy for the STRING data type. [#61960](https://github.com/StarRocks/starrocks/issues/61960)

### Functions and SQL Syntax

- Added the following functions:
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414 ](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` and `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
- Provides the following syntactic extensions:
  - Supports IF NOT EXISTS keywords in CREATE ANALYZE FULL TABLE. [#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - Supports EXCLUDE clauses in SELECT.  [#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - Supports FILTER clauses in aggregate functions, improving readability and execution efficiency of conditional aggregations. [#58937](https://github.com/StarRocks/starrocks/pull/58937)
