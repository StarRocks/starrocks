---
displayed_sidebar: docs
---

# StarRocks version 3.2

## 3.2.15

Release date: February 14, 2025

### New Features

- Window functions support `max_by` and `min_by`. [#54961](https://github.com/StarRocks/starrocks/pull/54961)

### Improvements

- Added StarClient timeout parameters. [#54496](https://github.com/StarRocks/starrocks/pull/54496)
  - star_client_read_timeout_seconds
  - star_client_list_timeout_seconds
  - star_client_write_timeout_seconds
- Tables with List partitioning strategies support partition pruning for DELETE statements. [#55400](https://github.com/StarRocks/starrocks/pull/55400)

### Bug Fixes

Fixed the following issues:

- Stream Load fails when a node with an Alive status of false was scheduled. [#55371](https://github.com/StarRocks/starrocks/pull/55371)
- An error is returned during partial updates on Primary Key tables with Stream Load. [#53403](https://github.com/StarRocks/starrocks/pull/55430)
- bRPC error persists after BE node restart. [#40229](https://github.com/StarRocks/starrocks/pull/40229)

## 3.2.14

Release date: January 8, 2025

### Improvements

- Supports collecting statistics of Paimon tables. [#52858](https://github.com/StarRocks/starrocks/pull/52858)
- Included node information and histogram metrics in JSON metrics. [#53735](https://github.com/StarRocks/starrocks/pull/53735)

### Bug Fixes

Fixed the following issues:

- The score of the Primary Key table index was not updated in the Commit phase. [#41737](https://github.com/StarRocks/starrocks/pull/41737)
- Incorrect execution plans for `max(count(distinct))` when low-cardinality optimization is enabled. [#53403](https://github.com/StarRocks/starrocks/pull/53403)
- When the List partition column has NULL values, queries against the Min/Max value of the partition column will lead to incorrect partition pruning. [#53235](https://github.com/StarRocks/starrocks/pull/53235)
- Upload retries fail when backing up data to HDFS. [#53679](https://github.com/StarRocks/starrocks/pull/53679)

## 3.2.13

Release date: December 13, 2024

### Improvements

- Supports setting a time range within which Base Compaction is forbidden for a specific table. [#50120](https://github.com/StarRocks/starrocks/pull/50120)

### Bug Fixes

Fixed the following issues:

- The `loadRowsRate` field returned `0` after executing SHOW ROUTINE LOAD. [#52151](https://github.com/StarRocks/starrocks/pull/52151)
- The `Files()` function read columns that were not queried. [#52210](https://github.com/StarRocks/starrocks/pull/52210)
- Prometheus failed to parse materialized view metrics with special characters in their names. (Now materialized view metrics support tags.) [#52782](https://github.com/StarRocks/starrocks/pull/52782)
- The `array_map` function caused BE to crash. [#52909](https://github.com/StarRocks/starrocks/pull/52909)
- Metadata Cache issues caused BE to crash. [#52968](https://github.com/StarRocks/starrocks/pull/52968)
- Routine Load tasks were canceled due to expired transactions. (Now tasks are canceled only if the database or table no longer exists). [#50334](https://github.com/StarRocks/starrocks/pull/50334)
- Stream Load failures when submitted using HTTP 1.0. [#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- Issues related to Glue and S3 integration: [#48433](https://github.com/StarRocks/starrocks/pull/48433)
  - Some error messages did not display the root cause.
  - Error messages for writing to a Hive partitioned table with the partition column of type STRING when Glue was used as the metadata service.
  - Dropping Hive tables failed without proper error messages when the user lacked sufficient permissions.
- The `storage_cooldown_time` property for materialized views did not take effect when set to `maximum`. [#52079](https://github.com/StarRocks/starrocks/pull/52079)

## 3.2.12

Release date: October 23, 2024

### Improvements

- Optimized memory allocation and statistics in BE for certain complex query scenarios to avoid OOM. [#51382](https://github.com/StarRocks/starrocks/pull/51382)
- Optimized memory usage in FE in Schema Change scenarios. [#50855](https://github.com/StarRocks/starrocks/pull/50855)
- Optimized the job status display when querying the system-defined view `information_schema.routine_load_jobs` from Follower FE nodes. [#51763](https://github.com/StarRocks/starrocks/pull/51763)
- Supports Backup and Restore of with the List partitioned tables. [#51993](https://github.com/StarRocks/starrocks/pull/51993)

### Bug Fixes

Fixed the following issues:

- The error message was lost after writing to Hive failed. [#33167](https://github.com/StarRocks/starrocks/pull/33167)
- The `array_map` function causes a crash when excessive constant parameters are used. [#51244](https://github.com/StarRocks/starrocks/pull/51244)
- Special characters in the PARTITION BY columns of expression partitioned tables cause FE CheckPoint failures. [#51677](https://github.com/StarRocks/starrocks/pull/51677)
- Accessing the system-defined view `information_schema.fe_locks` causes a crash. [#51742](https://github.com/StarRocks/starrocks/pull/51742)
- Querying generated columns causes an error. [#51755](https://github.com/StarRocks/starrocks/pull/51755)
- Optimize Table fails when the table name contains special characters. [#51755](https://github.com/StarRocks/starrocks/pull/51755)
- Tablets could not be balanced in certain scenarios. [#51828](https://github.com/StarRocks/starrocks/pull/51828)

### Behavior Changes

- Supports dynamic modification of Backup and Restore-related parameters.[#52111](https://github.com/StarRocks/starrocks/pull/52111)

## 3.2.11

Release date: September 9, 2024

### Improvements

- Supports masking authentication information for Files() and PIPE. [#47629](https://github.com/StarRocks/starrocks/pull/47629)
- Support automatic inference for the STRUCT type when reading Parquet files through Files(). [#50481](https://github.com/StarRocks/starrocks/pull/50481)

### Bug Fixes

Fixed the following issues:

- An error is returned for equi-join queries because they failed to be rewritten by the global dictionary. [#50690](https://github.com/StarRocks/starrocks/pull/50690)
- The error "version has been compacted" caused by an infinite loop on the FE side during Tablet Clone. [#50561](https://github.com/StarRocks/starrocks/pull/50561)
- Incorrect scheduling for unhealthy replica repairs after distributing data based on labels. [#50331](https://github.com/StarRocks/starrocks/pull/50331)
- An error in the statistics collection log: "Unknown column '%s' in '%s." [#50785](https://github.com/StarRocks/starrocks/pull/50785)
- Incorrect timezone usage when reading complex types like TIMESTAMP from Parquet files via Files(). [#50448](https://github.com/StarRocks/starrocks/pull/50448)

### Behavior Changes

- When downgrading StarRocks from v3.3.x to v3.2.11, the system will ignore it if there is incompatible metadata. [#49636](https://github.com/StarRocks/starrocks/pull/49636)

## 3.2.10

Release date: August 23, 2024

### Improvements

- Files() will automatically convert `BYTE_ARRAY` data with a `logical_type` of `JSON` in Parquet files to the JSON type in StarRocks. [#49385](https://github.com/StarRocks/starrocks/pull/49385)
- Optimized error messages for Files() when Access Key ID and Secret Access Key are missing. [#49090](https://github.com/StarRocks/starrocks/pull/49090)
- `information_schema.columns` supports the `GENERATION_EXPRESSION` field. [#49734](https://github.com/StarRocks/starrocks/pull/49734)

### Bug Fixes

Fixed the following issues:

- Downgrading a v3.3 shared-data cluster to v3.2 after setting the Primary Key table property `"persistent_index_type" = "CLOUD_NATIVE"` causes a crash. [#48149](https://github.com/StarRocks/starrocks/pull/48149)
- Exporting data to CSV files using SELECT INTO OUTFILE may cause data inconsistency. [#48052](https://github.com/StarRocks/starrocks/pull/48052)
- Queries encounter failures during concurrent query execution. [#48180](https://github.com/StarRocks/starrocks/pull/48180)
- Queries would hang due to a timeout in the Plan phase without exiting. [#48405](https://github.com/StarRocks/starrocks/pull/48405)
- After disabling index compression for Primary Key tables in older versions and then upgrading to v3.2.9, accessing `page_off` information causes an array out-of-bounds crash. [#48230](https://github.com/StarRocks/starrocks/pull/48230)
- BE crash caused by concurrent execution of ADD/DROP COLUMN operations. [#49355](https://github.com/StarRocks/starrocks/pull/49355)
- Queries against negative `TINYINT` values in ORC format files return `None` on the aarch64 architecture. [#49517](https://github.com/StarRocks/starrocks/pull/49517)
- If the disk write operation fails, failures of `l0` snapshots for Primary Key Persistent Index may cause data loss. [#48045](https://github.com/StarRocks/starrocks/pull/48045)
- Partial Update in Column mode for Primary Key tables fails under scenarios with large-volume data updates. [#49054](https://github.com/StarRocks/starrocks/pull/49054)
- BE crash caused by Fast Schema Evolution when downgrading a v3.3.0 shared-data cluster to v3.2.9. [#42737](https://github.com/StarRocks/starrocks/pull/42737)
- `partition_linve_nubmer` does not take effect. [#49213](https://github.com/StarRocks/starrocks/pull/49213)
- The conflict between index persistence and compaction in Primary Key tables could cause clone failures. [#49341](https://github.com/StarRocks/starrocks/pull/49341)
- Modifications of `partition_line_number` using ALTER TABLE do not take effect. [#49437](https://github.com/StarRocks/starrocks/pull/49437)
- Rewrite of CTE distinct grouping sets generates an invalid plan. [#48765](https://github.com/StarRocks/starrocks/pull/48765)
- RPC failures polluted the thread pool. [#49619](https://github.com/StarRocks/starrocks/pull/49619)
- authentication failure issues when loading files from AWS S3 via PIPE. [#49837](https://github.com/StarRocks/starrocks/pull/49837)

### Behavior Changes

- Added a check for the `meta` directory in the FE startup script. If the directory does not exist, it will be automatically created.  [#48940](https://github.com/StarRocks/starrocks/pull/48940)
- Added a memory limit parameter `load_process_max_memory_hard_limit_ratio` for data loading. If memory usage exceeds the limit, subsequent loading tasks will fail. [#48495](https://github.com/StarRocks/starrocks/pull/48495)

## 3.2.9

Release date: July 11, 2024

### New Features

- Paimon tables now support DELETE Vectors. [#45866](https://github.com/StarRocks/starrocks/issues/45866)
- Supports Column-level access control through Apache Ranger. [#47702](https://github.com/StarRocks/starrocks/pull/47702)
- Stream Load can automatically convert JSON strings into STRUCT/MAP/ARRAY types during loading. [#45406](https://github.com/StarRocks/starrocks/pull/45406)
- JDBC Catalog now supports Oracle and SQL Server. [#35691](https://github.com/StarRocks/starrocks/issues/35691)

### Improvements

- Improved privilege management by restricting `user_admin` role users from resetting the password of the root user. [#47801](https://github.com/StarRocks/starrocks/pull/47801)
- Stream Load now supports using `\t` and `\n` as row and column delimiters. Users do not need to convert them to their hexadecimal ASCII codes. [#47302](https://github.com/StarRocks/starrocks/pull/47302)
- Optimized memory usage during data loading. [#47047](https://github.com/StarRocks/starrocks/pull/47047)
- Supports masking authentication information for the Files() function in audit logs. [#46893](https://github.com/StarRocks/starrocks/pull/46893)
- Hive tables now support the `skip.header.line.count` property. [#47001](https://github.com/StarRocks/starrocks/pull/47001)
- JDBC Catalog supports more data types. [#47618](https://github.com/StarRocks/starrocks/pull/47618)

### Behavior Changes

- Changed the value inheritance order of the `JAVA_OPTS` parameters. If versions other than JDK_9 or JDK_11 are used, users need to configure `JAVA_OPTS` directly. [#47495](https://github.com/StarRocks/starrocks/pull/47495)
- When users create a non-partitioned table without specifying the bucket number, the minimum bucket number the system sets for the table is `16` (instead of `2` based on the formula `2*BE or CN count`). If users want to set a smaller bucket number when creating a small table, they must set it explicitly. [#47005](https://github.com/StarRocks/starrocks/pull/47005)
- When users create a partitioned table without specifying the bucket number, if the number of partitions exceeds 5, the rule for setting the bucket count is changed to `max(2*BE or CN count, bucket number calculated based on the largest historical partition data volume)`. The previous rule was to calculate the bucket number based on the largest historical partition data volume. [#47949](https://github.com/StarRocks/starrocks/pull/47949)

### Bug Fixes

Fixed the following issues:

- BE crash caused by ALTER TABLE ADD COLUMN after upgrading a shared-data cluster from v3.2.x to v3.3.0 and then rolling it back. [#47826](https://github.com/StarRocks/starrocks/pull/47826)
- Tasks initiated through SUBMIT TASK showed a Running status indefinitely in the QueryDetail interface. [#47619](https://github.com/StarRocks/starrocks/pull/47619)
- Forwarding queries to the FE Leader node caused a null pointer exception. [#47559](https://github.com/StarRocks/starrocks/pull/47559)
- SHOW MATERIALIZED VIEWS with WHERE conditions caused a null pointer exception. [#47811](https://github.com/StarRocks/starrocks/pull/47811)
- Vertical Compaction fails for Primary Key tables in shared-data clusters. [#47192](https://github.com/StarRocks/starrocks/pull/47192)
- Improper handling of I/O Error when sinking data to Hive or Iceberg tables. [#46979](https://github.com/StarRocks/starrocks/pull/46979)
- Table properties do not take effect when whitespaces are added to their values. [#47119](https://github.com/StarRocks/starrocks/pull/47119)
- BE crash caused by concurrent migration and Index Compaction operations on Primary Key tables. [#46675](https://github.com/StarRocks/starrocks/pull/46675)

## 3.2.8

Release date: June 7, 2024

### New Features

- **[Supports adding labels on BEs](https://docs.starrocks.io/docs/3.2/administration/management/resource_management/be_label/)**: Supports adding labels on BEs based on information such as the racks and data centers where BEs are located. It ensures even data distribution among racks and data centers, and facilitates disaster recovery in case of power failures in certain racks or faults in data centers. [#38833](https://github.com/StarRocks/starrocks/pull/38833)

### Bug Fixes

Fixed the following issues:

- An error is returned when users DELETE data rows from tables that use the expression partitioning method with str2date. [#45939](https://github.com/StarRocks/starrocks/pull/45939)
- BEs in the destination cluster crash when the StarRocks Cross-cluster Data Migration Tool fails to retrieve the Schema information from the source cluster. [#46068](https://github.com/StarRocks/starrocks/pull/46068)
- The error `Multiple entries with same key` is returned to queries with non-deterministic functions. [#46602](https://github.com/StarRocks/starrocks/pull/46602)

## 3.2.7

Release date: May 24, 2024

### New Features

- Stream Load supports data compression during transmission, reducing network bandwidth overhead. Users can specify different compression algorithms using parameters `compression` and `Content-Encoding`. Supported compression algorithms including GZIP, BZIP2, LZ4_FRAME, and ZSTD. [#43732](https://github.com/StarRocks/starrocks/pull/43732)
- Optimized the garbage collection (GC) mechanism in shared-data clusters. Supports manual compaction for tables or partitions stored in object storage. [#39532](https://github.com/StarRocks/starrocks/issues/39532)
- Flink connector supports reading complex data types ARRAY, MAP, and STRUCT from StarRocks. [#42932](https://github.com/StarRocks/starrocks/pull/42932) [#347](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/347)
- Supports populating Data Cache asynchronously during queries, reducing the impact of populating cache on query performance. [#40489](https://github.com/StarRocks/starrocks/pull/40489)
- ANALYZE TABLE supports collecting histograms for external tables, effectively addressing data skews. For more information, see [CBO statistics](https://docs.starrocks.io/docs/3.2/using_starrocks/Cost_based_optimizer/#collect-statistics-of-hiveiceberghudi-tables). [#42693](https://github.com/StarRocks/starrocks/pull/42693)
- Lateral Join with [UNNEST](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/unnest/) supports LEFT JOIN. [#43973](https://github.com/StarRocks/starrocks/pull/43973)
- Query Pool supports configuring memory usage threshold that triggers spilling via BE static parameter `query_pool_spill_mem_limit_threshold`. Once the threshold is reached, intermediate results of queries will be spilled to disks to reduce memory usage, thus avoiding OOM.
- Supports creating asynchronous materialized views based on Hive views.

### Improvements

- Optimized the error message returned for Broker Load tasks when there is no data under the specified HDFS paths. [#43839](https://github.com/StarRocks/starrocks/pull/43839)
- Optimized the error message returned when the Files function is used to read data from AWS S3 without Access Key and Secret Key specified. [#42450](https://github.com/StarRocks/starrocks/pull/42450)
- Optimized the error message returned for Broker Load tasks that load no data to any partitions. [#44292](https://github.com/StarRocks/starrocks/pull/44292)
- Optimized the error message returned for INSERT INTO SELECT tasks when the column count of the destination table does not match that in the SELECT statement. [#44331](https://github.com/StarRocks/starrocks/pull/44331)

### Bug Fixes

Fixed the following issues:

- Concurrent read or write of the BITMAP-type data may cause BE to crash. [#44167](https://github.com/StarRocks/starrocks/pull/44167)
- Primary key indexes may cause BE to crash. [#43793](https://github.com/StarRocks/starrocks/pull/43793) [#43569](https://github.com/StarRocks/starrocks/pull/43569) [#44034](https://github.com/StarRocks/starrocks/pull/44034)
- Under high query concurrency scenarios, the str_to_map function may cause BE to crash. [#43901](https://github.com/StarRocks/starrocks/pull/43901)
- When the Masking policy of Apache Ranger is used, an error is returned when table aliases are specified in queries. [#44445](https://github.com/StarRocks/starrocks/pull/44445)
- In shared-data clusters, query execution cannot be routed to a backup node when the current node encounters exceptions. The corresponding error message is optimized for this issue. [#43489](https://github.com/StarRocks/starrocks/pull/43489)
- Memory information is incorrect in the container environment. [#43225](https://github.com/StarRocks/starrocks/issues/43225)
- An exception is thrown when INSERT tasks are canceled. [#44239](https://github.com/StarRocks/starrocks/pull/44239)
- Expression-based dynamic partitions cannot be automatically created. [#44163](https://github.com/StarRocks/starrocks/pull/44163)
- Creating partitions may cause FE deadlock. [#44974](https://github.com/StarRocks/starrocks/pull/44974)

## 3.2.6

Release date: April 18, 2024

### Bug Fixes

Fixed the following issue:

- The privileges of external tables cannot be found due to incompatibility issues. [#44030](https://github.com/StarRocks/starrocks/pull/44030)

## 3.2.5 (Yanked)

Release date: April 12, 2024

:::tip

This version has been taken offline due to privilege issues in querying external tables in external catalogs such as Hive and Iceberg.

- **Problem**: When a user queries data from an external table in an external catalog, access to this table is denied even when the user has the SELECT privilege on this table. SHOW GRANTS also shows that the user has this privilege.

- **Impact scope**: This problem only affects queries on external tables in external catalogs. Other queries are not affected.

- **Temporary workaround**: The query succeeds after the SELECT privilege on this table is granted to the user again. But `SHOW GRANTS` will return duplicate privilege entries. After an upgrade to v3.2.6, users can run `REVOKE` to remove one of the privilege entries.

:::

### New Features

- Supports the [dict_mapping](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/dict-functions/dict_mapping/) column property, which can significantly facilitate the loading process during the construction of a global dictionary, accelerating the exact COUNT DISTINCT calculation.

### Behavior Changes

- When null values in JSON data are evaluated based on the `IS NULL` operator, they are considered NULL values following SQL language. For example, `true` is returned for `SELECT parse_json('{"a": null}') -> 'a' IS NULL` (before this behavior change, `false` is returned). [#42765](https://github.com/StarRocks/starrocks/pull/42765)

### Improvements

- Optimized the column type unionization rules for automatic schema detection in the FILES table function. When columns with the same name but different types exist in separate files, FILES will attempt to merge them by selecting the type with the larger granularity as the final type. For example, if there are columns with the same name but of types FLOAT and INT respectively, FILES will return DOUBLE as the final type. [#40959](https://github.com/StarRocks/starrocks/pull/40959)
- Primary Key tables support Size-tiered Compaction to reduce the I/O amplification. [#41130](https://github.com/StarRocks/starrocks/pull/41130)
- When Broker Load is used to load data from ORC files that contain TIMESTAMP-type data, StarRocks supports retaining microseconds in the timestamps when converting the timestamps to match its own DATETIME data type. [#42179](https://github.com/StarRocks/starrocks/pull/42179)
- Optimized the error messages for Routine Load. [#41306](https://github.com/StarRocks/starrocks/pull/41306)
- Optimized the error messages when the FILES table function is used to convert invalid data types. [#42717](https://github.com/StarRocks/starrocks/pull/42717)

### Bug Fixes

Fixed the following issues:

- FEs fail to start after system-defined views are dropped. Dropping system-defined views is now prohibited. [#43552](https://github.com/StarRocks/starrocks/pull/43552)
- BEs crash when duplicate sort key columns exist in Primary Key tables. Duplicate sort key columns are now prohibited. [#43206](https://github.com/StarRocks/starrocks/pull/43206)
- An error, instead of NULL, is returned when the input value of the to_json() function is NULL. [#42171](https://github.com/StarRocks/starrocks/pull/42171)
- In shared-data mode, the garbage collection and thread eviction mechanisms for handling persistent indexes created on Primary Key tables cannot take effect on CN nodes. As a result, obsolete data cannot be deleted. [#41955](https://github.com/StarRocks/starrocks/pull/41955)
- In shared-data mode, an error is returned when users modify the `enable_persistent_index` property of a Primary Key table. [#42890](https://github.com/StarRocks/starrocks/pull/42890)
- In shared-data mode, NULL values are given to columns that are not supposed to be changed when users update a Primary Key table with partial updates in column mode. [#42355](https://github.com/StarRocks/starrocks/pull/42355)
- Queries cannot be rewritten with asynchronous materialized views created on logical views. [#42173](https://github.com/StarRocks/starrocks/pull/42173)
- CNs crash when the Cross-cluster Data Migration Tool is used to migrate Primary Key tables to a shared-data cluster. [#42260](https://github.com/StarRocks/starrocks/pull/42260)
- The partition ranges of the external catalog-based asynchronous materialized views are not consecutive. [#41957](https://github.com/StarRocks/starrocks/pull/41957)

## 3.2.4 (Yanked)

Release date: March 12, 2024

:::tip

This version has been taken offline due to privilege issues in querying external tables in external catalogs such as Hive and Iceberg.

- **Problem**: When a user queries data from an external table in an external catalog, access to this table is denied even when the user has the SELECT privilege on this table. SHOW GRANTS also shows that the user has this privilege.

- **Impact scope**: This problem only affects queries on external tables in external catalogs. Other queries are not affected.

- **Temporary workaround**: The query succeeds after the SELECT privilege on this table is granted to the user again. But `SHOW GRANTS` will return duplicate privilege entries. After an upgrade to v3.2.6, users can run `REVOKE` to remove one of the privilege entries.

:::

### New Features

- Cloud-native Primary Key tables in shared-data clusters support Size-tiered Compaction to reduce the write I/O amplification. [#41034](https://github.com/StarRocks/starrocks/pull/41034)
- Added the date function `milliseconds_diff`. [#38171](https://github.com/StarRocks/starrocks/pull/38171)
- Added the session variable `catalog`, which specifies the catalog to which the session belongs. [#41329](https://github.com/StarRocks/starrocks/pull/41329)
- Supports [setting user-defined variables in hints](https://docs.starrocks.io/docs/3.2/administration/Query_planning/#user-defined-variable-hint). [#40746](https://github.com/StarRocks/starrocks/pull/40746)
- Supports CREATE TABLE LIKE in Hive catalogs. [#37685](https://github.com/StarRocks/starrocks/pull/37685) 
- Added the view `information_schema.partitions_meta`, which records detailed metadata of partitions. [#39265](https://github.com/StarRocks/starrocks/pull/39265)
- Added the view `sys.fe_memory_usage`, which records the memory usage for StarRocks. [#40464](https://github.com/StarRocks/starrocks/pull/40464)

### Behavior Changes

- `cbo_decimal_cast_string_strict` is used to control how CBO converts data from the DECIMAL type to the STRING type. The default value `true` indicates that the logic built in v2.5.x and later versions prevails and the system implements strict conversion (namely, the system truncates the generated string and fills 0s based on the scale length). The DECIMAL type is not strictly filled in earlier versions, causing different results when comparing the DECIMAL type and the STRING type. [#40619](https://github.com/StarRocks/starrocks/pull/40619)
- The default value of the Iceberg Catalog parameter `enable_iceberg_metadata_cache` has been changed to `false`. From v3.2.1 to v3.2.3, this parameter is set to `true` by default, regardless of what metastore service is used. In v3.2.4 and later, if the Iceberg cluster uses AWS Glue as metastore, this parameter still defaults to `true`. However, if the Iceberg cluster uses other metastore service such as Hive metastore, this parameter defaults to `false`. [#41826](https://github.com/StarRocks/starrocks/pull/41826)
- The user who can refresh materialized views is changed from the `root` user to the user who creates the materialized views. This change does not affect existing materialized views. [#40670](https://github.com/StarRocks/starrocks/pull/40670)  
- By default, when comparing columns of constant and string types, StarRocks compares them as strings. Users can use the session variable `cbo_eq_base_type` to adjust the rule used for the comparison. For example, users can set `cbo_eq_base_type` to `decimal`, and StarRocks then compares the columns as numeric values. [#40619](https://github.com/StarRocks/starrocks/pull/40619)

### Improvements

- Shared-data StarRocks clusters support the Partitioned Prefix feature for S3-compatible object storage systems. When this feature is enabled, StarRocks stores the data into multiple, uniformly prefixed partitions (sub-paths) under the bucket. This improves the read and write efficiency on data files in S3-compatible object storages. [#41627](https://github.com/StarRocks/starrocks/pull/41627)
- StarRocks supports using the parameter `s3_compatible_fs_list` to specify which S3-compatible object storage can be accessed via AWS SDK, and supports using the parameter `fallback_to_hadoop_fs_list` to specify non-S3-compatible object storages that require access via HDFS Schema (this method requires the use of vendor-provided JAR packages). [#41123](https://github.com/StarRocks/starrocks/pull/41123)
- Optimized compatibility with Trino. Supports syntax conversion from the following Trino functions: current_catalog, current_schema, to_char, from_hex, to_date, to_timestamp, and index. [#41217](https://github.com/StarRocks/starrocks/pull/41217) [#41319](https://github.com/StarRocks/starrocks/pull/41319) [#40803](https://github.com/StarRocks/starrocks/pull/40803)
- Optimized the query rewrite logic of materialized views. StarRocks can rewrite queries with materialized views created upon logical views. [#42173](https://github.com/StarRocks/starrocks/pull/42173)
- Improved the efficiency of converting the STRING type to the DATETIME type by 35% to 40%. [#41464](https://github.com/StarRocks/starrocks/pull/41464)
- The `agg_type` of BITMAP-type columns in an Aggregate table can be set to `replace_if_not_null` in order to support updates only to a few columns of the table. [#42034](https://github.com/StarRocks/starrocks/pull/42034)
- Improved the Broker Load performance when loading small ORC files. [#41765](https://github.com/StarRocks/starrocks/pull/41765)
- The tables with hybrid row-column storage support Schema Change. [#40851](https://github.com/StarRocks/starrocks/pull/40851)
- The tables with hybrid row-column storage support complex types including BITMAP, HLL, JSON, ARRAY, MAP, and STRUCT. [#41476](https://github.com/StarRocks/starrocks/pull/41476)
- A new internal SQL log file is added to record log data related to statistics and materialized views. [#40453](https://github.com/StarRocks/starrocks/pull/40453)

### Bug Fixes

Fixed the following issues:

- "Analyze Error" is thrown if inconsistent letter cases are assigned to the names or aliases of tables or views queried in the creation of a Hive view. [#40921](https://github.com/StarRocks/starrocks/pull/40921)
- I/O usage reaches the upper limit if persistent indexes are created on Primary Key tables. [#39959](https://github.com/StarRocks/starrocks/pull/39959)
- In shared-data clusters, primary key index directories are deleted every 5 hours. [#40745](https://github.com/StarRocks/starrocks/pull/40745)
- After users execute ALTER TABLE COMPACT by hand, the memory usage statistics for compaction operations are abnormal. [#41150](https://github.com/StarRocks/starrocks/pull/41150)
- Retries of the Publish phase may hang for Primary Key tables. [#39890](https://github.com/StarRocks/starrocks/pull/39890)

## 3.2.3

Release date: February 8, 2024

### New Features

- [Preview] Supports hybrid row-column storage for tables. It allows better performance for high-concurrency, low-latency point lookups against Primary Key tables and partial data updates. Currently, this feature does not support modification via ALTER TABLE, changing Sort Key, and partial updates in column mode.
- Supports backing up and restoring asynchronous materialized views.
- Broker Load supports loading JSON-type data.
- Supports query rewrite using asynchronous materialized views created upon views. Queries against a view can be rewritten based on materialized views that are created upon that view.
- Supports CREATE OR REPLACE PIPE. [#37658](https://github.com/StarRocks/starrocks/pull/37658)

### Behavior Changes

- Added the session variable `enable_strict_order_by`. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries. [#37910](https://github.com/StarRocks/starrocks/pull/37910)
- Added the session variable `enable_materialized_view_for_insert`, which controls whether materialized views rewrite the queries in INSERT INTO SELECT statements. The default value is `false`. [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- When a single query is executed within the Pipeline framework, its memory limit is now constrained by the variable `query_mem_limit` instead of `exec_mem_limit`. Setting the value of `query_mem_limit` to `0` indicates no limit. [#34120](https://github.com/StarRocks/starrocks/pull/34120) 

### Parameter Changes

- Added the FE configuration item `http_worker_threads_num`, which specifies the number of threads for HTTP server to deal with HTTP requests. The default value is `0`. If the value for this parameter is set to a negative value or `0`, the actual thread number is twice the number of CPU cores. [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- Added the BE configuration item `lake_pk_compaction_max_input_rowsets`, which controls the maximum number of input rowsets allowed in a Primary Key table compaction task in a shared-data StarRocks cluster. This helps optimize resource consumption for compaction tasks. [#39611](https://github.com/StarRocks/starrocks/pull/39611)
- Added the session variable `connector_sink_compression_codec`, which specifies the compression algorithm used for writing data into Hive tables or Iceberg tables, or exporting data with Files(). Valid algorithms include GZIP, BROTLI, ZSTD, and LZ4. [#37912](https://github.com/StarRocks/starrocks/pull/37912)
- Added the FE configuration item `routine_load_unstable_threshold_second`. [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- Added the BE configuration item `pindex_major_compaction_limit_per_disk` to configure the maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks. The default value is `1`. [#36681](https://github.com/StarRocks/starrocks/pull/36681)
- Added the BE configuration item `enable_lazy_delta_column_compaction`. The default value is `true`, indicating that StarRocks does not perform frequent compaction operations on delta columns. [#36654](https://github.com/StarRocks/starrocks/pull/36654)
- Added the FE configuration item `default_mv_refresh_immediate`, which specifies whether to immediately refresh the materialized view after the materialized view is created. The default value is `true`. [#37093](https://github.com/StarRocks/starrocks/pull/37093)
- Changed the default value of the FE configuration item `default_mv_refresh_partition_num`to `1`. This indicates that when multiple partitions need to be updated during a materialized view refresh, the task will be split in batches, refreshing only one partition at a time. This helps reduce resource consumption during each refresh. [#36560](https://github.com/StarRocks/starrocks/pull/36560)
- Changed the default value of the BE/CN configuration item `starlet_use_star_cache` to `true`. This indicates that Data Cache is enabled by default in shared-data clusters. If, before upgrading, you have manually configured the BE/CN configuration item `starlet_cache_evict_high_water` to `X`, you must configure the BE/CN configuration item `starlet_star_cache_disk_size_percent` to `(1.0 - X) * 100`. For example, if you have set `starlet_cache_evict_high_water` to `0.3` before upgrading, you must set `starlet_star_cache_disk_size_percent` to `70`. This ensures that both file data cache and Data Cache will not exceed the disk capacity limit. [#38200](https://github.com/StarRocks/starrocks/pull/38200)

### Improvements

- Added date formats `yyyy-MM-ddTHH:mm` and `yyyy-MM-dd HH:mm` to support TIMESTAMP partition fields in Apache Iceberg tables. [#39986](https://github.com/StarRocks/starrocks/pull/39986)
- Added Data Cache-related metrics to the monitoring API. [#40375](https://github.com/StarRocks/starrocks/pull/40375)
- Optimized BE log printing to prevent too many irrelevant logs. [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- Added the field `storage_medium` to the view `information_schema.be_tablets`. [#37070](https://github.com/StarRocks/starrocks/pull/37070)
- Supports `SET_VAR` in multiple sub-queries. [#36871](https://github.com/StarRocks/starrocks/pull/36871)
- A new field `LatestSourcePosition` is added to the return result of SHOW ROUTINE LOAD to record the position of the latest message in each partition of the Kafka topic, helping check the latencies of data loading. [#38298](https://github.com/StarRocks/starrocks/pull/38298)
- When the string on the right side of the LIKE operator within the WHERE clause does not include `%` or `_`, the LIKE operator is converted into the `=` operator. [#37515](https://github.com/StarRocks/starrocks/pull/37515)
- The default retention period of trash files is changed to 1 day from the original 3 days. [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- Supports collecting statistics from Iceberg tables with Partition Transform. [#39907](https://github.com/StarRocks/starrocks/pull/39907)
- The scheduling policy for Routine Load is optimized, so that slow tasks do not block the execution of the other normal tasks. [#37638](https://github.com/StarRocks/starrocks/pull/37638)

### Bug Fixes

Fixed the following issues:

- The execution of ANALYZE TABLE gets stuck occasionally. [#36836](https://github.com/StarRocks/starrocks/pull/36836)
- The memory consumption by PageCache exceeds the threshold specified by the BE dynamic parameter `storage_page_cache_limit` in certain circumstances. [#37740](https://github.com/StarRocks/starrocks/pull/37740)
- Hive metadata in Hive catalogs is not automatically refreshed when new fields are added to Hive tables. [#37549](https://github.com/StarRocks/starrocks/pull/37549)
- In some cases, `bitmap_to_string` may return incorrect results due to data type overflow. [#37405](https://github.com/StarRocks/starrocks/pull/37405)
- When `SELECT ... FROM ... INTO OUTFILE` is executed to export data into CSV files, the error "Unmatched number of columns" is reported if the FROM clause contains multiple constants. [#38045](https://github.com/StarRocks/starrocks/pull/38045)
- In some cases, querying semi-structured data in tables may cause BEs to crash. [#40208](https://github.com/StarRocks/starrocks/pull/40208)

## 3.2.2

Release date: December 30, 2023

### Bug Fixes

Fixed the following issue:

- When StarRocks is upgraded from v3.1.2 or earlier to v3.2, FEs may fail to restart. [#38172](https://github.com/StarRocks/starrocks/pull/38172)

## 3.2.1

Release date: December 21, 2023

### New Features

#### Data Lake Analytics

- Supports reading [Hive Catalog](https://docs.starrocks.io/docs/3.2/data_source/catalog/hive_catalog/) tables and file external tables in Avro, SequenceFile, and RCFile formats through Java Native Interface (JNI).

#### Materialized View

- Added a view `object_dependencies` to the database `sys`. It contains the lineage information of asynchronous materialized views. [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- Supports creating synchronous materialized views with the WHERE clause.
- Supports partition-level incremental refresh for asynchronous materialized views created upon Iceberg catalogs.
- [Preview] Supports creating asynchronous materialized views based on tables in a Paimon catalog with partition-level refresh.

#### Query and SQL functions

- Supports the prepared statement. It allows better performance for processing high-concurrency point lookup queries. It also prevents SQL injection effectively.
- Supports the following Bitmap functions: [subdivide_bitmap](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/), [bitmap_from_binary](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/), and [bitmap_to_binary](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/).
- Supports the Array function [array_unique_agg](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_unique_agg/).

#### Monitoring and alerts

- Added a new metric `max_tablet_rowset_num` for setting the maximum allowed number of rowsets. This metric helps detect possible compaction issues and thus reduces the occurrences of the error "too many versions". [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### Parameter change

- A new BE configuration item `enable_stream_load_verbose_log` is added. The default value is `false`. With this parameter set to `true`, StarRocks can record the HTTP requests and responses for Stream Load jobs, making troubleshooting easier. [#36113](https://github.com/StarRocks/starrocks/pull/36113)

### Improvements

- Upgraded the default GC algorithm in JDK8 to G1. [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- A new value option `GROUP_CONCAT_LEGACY` is added to the session variable [sql_mode](https://docs.starrocks.io/docs/3.2/reference/System_variable/#sql_mode) to provide compatibility with the implementation logic of the [group_concat](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/string-functions/group_concat/) function in versions earlier than v2.5. [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- The authentication information `aws.s3.access_key` and `aws.s3.access_secret` for [AWS S3 in Broker Load jobs](https://docs.starrocks.io/docs/3.2/loading/s3/) are hidden in audit logs. [#36571](https://github.com/StarRocks/starrocks/pull/36571)
- The `be_tablets` view in the `information_schema` database provides a new field `INDEX_DISK`, which records the disk usage (measured in bytes) of persistent indexes. [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement provides a new field `OtherMsg`, which shows information about the last failed task. [#35806](https://github.com/StarRocks/starrocks/pull/35806)

### Bug Fixes

Fixed the following issues:

- The BEs crash if users create persistent indexes in the event of data corruption.[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- The [array_distinct](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_distinct/) function occasionally causes the BEs to crash. [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- After the DISTINCT window operator pushdown feature is enabled, errors are reported if SELECT DISTINCT operations are performed on the complex expressions of the columns computed by window functions.  [#36357](https://github.com/StarRocks/starrocks/pull/36357)
- Some S3-compatible object storage returns duplicate files, causing the BEs to crash. [#36103](https://github.com/StarRocks/starrocks/pull/36103)

## 3.2.0

Release date: December 1, 2023

### New Features

#### Shared-data cluster

- Supports persisting indexes of [Primary Key tables](https://docs.starrocks.io/docs/3.2/table_design/table_types/primary_key_table/) to local disks.
- Supports even distribution of Data Cache among multiple local disks.

#### Materialized View

**Asynchronous materialized view**

- The Query Dump file can include information of asynchronous materialized views.
- The Spill to Disk feature is enabled by default for the refresh tasks of asynchronous materialized views, reducing memory consumption.

#### Data Lake Analytics

- Supports creating and dropping databases and managed tables in [Hive catalogs](https://docs.starrocks.io/docs/3.2/data_source/catalog/hive_catalog/), and supports exporting data to Hive's managed tables using INSERT or INSERT OVERWRITE.
- Supports [Unified Catalog](https://docs.starrocks.io/docs/3.2/data_source/catalog/unified_catalog/), with which users can access different table formats (Hive, Iceberg, Hudi, and Delta Lake) that share a common metastore like Hive metastore or AWS Glue.
- Supports collecting statistics of Hive and Iceberg tables using ANALYZE TABLE, and storing the statistics in StarRocks, thus facilitating optimization of query plans and accelerating subsequent queries.
- Supports Information Schema for external tables, providing additional convenience for interactions between external systems (such as BI tools) and StarRocks.

#### Storage engine, data ingestion, and export

- Added the following features of loading with the table function [FILES()](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/table-functions/files/):
  - Loading Parquet and ORC format data from Azure or GCP.
  - Extracting the value of a key/value pair from the file path as the value of a column using the parameter `columns_from_path`.
  - Loading complex data types including ARRAY, JSON, MAP, and STRUCT.
- Supports unloading data from StarRocks to Parquet-formatted files stored in AWS S3 or HDFS by using INSERT INTO FILES. For detailed instructions, see [Unload data using INSERT INTO FILES](https://docs.starrocks.io/docs/3.2/unloading/unload_using_insert_into_files/).
- Supports [manual optimization of table structure and data distribution strategy](https://docs.starrocks.io/docs/3.2/table_design/Data_distribution#optimize-data-distribution-after-table-creation-since-32) used in an existing table to optimize the query and loading performance. You can set a new bucket key, bucket number, or sort key for a table. You can also set a different bucket number for specific partitions.
- Supports continuous data loading from [AWS S3](https://docs.starrocks.io/docs/3.2/loading/s3/#use-pipe) or [HDFS](https://docs.starrocks.io/docs/3.2/loading/hdfs_load/#use-pipe) using the PIPE method.
  - When PIPE detects new  or modifications in a remote storage directory, it can automatically load the new or modified data into the destination table in StarRocks. While loading data, PIPE automatically splits a large loading task into smaller, serialized tasks, enhancing stability in large-scale data ingestion scenarios and reducing the cost of error retries.

#### Query

- Supports [HTTP SQL API](https://docs.starrocks.io/docs/3.2/reference/HTTP_API/SQL/), enabling users to access StarRocks data via HTTP and execute SELECT, SHOW, EXPLAIN, or KILL operations.
- Supports Runtime Profile and text-based Profile analysis commands (SHOW PROFILELIST, ANALYZE PROFILE, EXPLAIN ANALYZE) to allow users to directly analyze profiles via MySQL clients, facilitating bottleneck identification and discovery of optimization opportunities.

#### SQL reference

Added the following functions:

- String functions: substring_index, url_extract_parameter, url_encode, url_decode, and translate
- Date functions: dayofweek_iso, week_iso, quarters_add, quarters_sub, milliseconds_add, milliseconds_sub, date_diff, jodatime_format, str_to_jodatime, to_iso8601, to_tera_date, and to_tera_timestamp
- Pattern matching function: regexp_extract_all
- hash function: xx_hash3_64
- Aggregate functions: approx_top_k
- Window functions: cume_dist, percent_rank and session_number
- Utility functions: get_query_profile and is_role_in_session

#### Privileges and security

StarRocks supports access control through [Apache Ranger](https://docs.starrocks.io/docs/3.2/administration/ranger_plugin/), providing a higher level of data security and allowing the reuse of existing services of external data sources. After integrating with Apache Ranger, StarRocks enables the following access control methods:

- When accessing internal tables, external tables, or other objects in StarRocks, access control can be enforced based on the access policies configured for the StarRocks Service in Ranger.
- When accessing an external catalog, access control can also leverage the corresponding Ranger service of the original data source (such as Hive Service) to control access (currently, access control for exporting data to Hive is not yet supported).

For more information, see [Manage permissions with Apache Ranger](https://docs.starrocks.io/docs/3.2/administration/ranger_plugin/).

### Improvements

#### Data Lake Analytics

- Optimized ORC Reader:
  - Optimized the ORC Column Reader, resulting in nearly a two-fold performance improvement for VARCHAR and CHAR data reading.
  - Optimized the decompression performance of ORC files in Zlib compression format.
- Optimized Parquet Reader:
  - Supports adaptive I/O merging, allowing adaptive merging of columns with and without predicates based on filtering effects, thus reducing I/O.
  - Optimized Dict Filter for faster predicate rewriting. Supports STRUCT sub-columns, and on-demand dictionary column decoding.
  - Optimized Dict Decode performance.
  - Optimized late materialization performance.
  - Supports caching file footers to avoid repeated computation overhead.
  - Supports decompression of Parquet files in lzo compression format.
- Optimized CSV Reader:
  - Optimized the Reader performance.
  - Supports decompression of CSV files in Snappy and lzo compression formats.
- Optimized the performance of the count calculation.
- Optimized Iceberg Catalog capabilities:
  - Supports collecting column statistics from Manifest files to accelerate queries.
  - Supports collecting NDV (number of distinct values) from Puffin files to accelerate queries.
  - Supports partition pruning.
  - Reduced Iceberg metadata memory consumption to enhance stability in scenarios with large metadata volume or high query concurrency.

#### Materialized View

**Asynchronous materialized view**

- Supports automatic refresh for an asynchronous materialized view created upon views or materialized views when schema changes occur on the views, materialized views, or their base tables.
- Data consistency:
  - Added the property `query_rewrite_consistency` for asynchronous materialized view creation. This property defines the query rewrite rules based on the consistency check.
  - Add the property `force_external_table_query_rewrite` for external catalog-based asynchronous materialized view creation. This property defines whether to allow force query rewrite for asynchronous materialized views created upon external catalogs.
  - For detailed information, see [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/).
- Added a consistency check for materialized views' partitioning key.
  - When users create an asynchronous materialized view with window functions that include a PARTITION BY expression, the partitioning column of the window function must match that of the materialized view.

#### Storage engine, data ingestion, and export

- Optimized the persistent index for Primary Key tables by improving memory usage logic while reducing I/O read and write amplification. [#24875](https://github.com/StarRocks/starrocks/pull/24875)  [#27577](https://github.com/StarRocks/starrocks/pull/27577)  [#28769](https://github.com/StarRocks/starrocks/pull/28769)
- Supports data re-distribution across local disks for Primary Key tables.
- Partitioned tables support automatic cooldown based on the partition time range and cooldown time. Compared to the original cooldown logic, it is more convenient to perform hot and cold data management on the partition level. For more information, see [Specify initial storage medium, automatic storage cooldown time, replica number](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#specify-initial-storage-medium-automatic-storage-cooldown-time-replica-number).
- The Publish phase of a load job that writes data into a Primary Key table is changed from asynchronous mode to synchronous mode. As such, the data loaded can be queried immediately after the load job finishes. For more information, see [enable_sync_publish](https://docs.starrocks.io/docs/3.2/administration/FE_configuration#enable_sync_publish).
- Supports Fast Schema Evolution, which is controlled by the table property [`fast_schema_evolution`](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#set-fast-schema-evolution). After this feature is enabled, the execution efficiency of adding or dropping columns is significantly improved. This mode is disabled by default (Default value is `false`). You cannot modify this property for existing tables using ALTER TABLE.
- [Supports dynamically adjusting the number of tablets to create](https://docs.starrocks.io/docs/3.2/table_design/Data_distribution#set-the-number-of-buckets) according to cluster information and the size of the data for **Duplicate Key** tables created with the Radom Bucketing strategy.

#### Query

- Optimized StarRocks' compatibility with Metabase and Superset. Supports integrating them with external catalogs.

#### SQL Reference

- [array_agg](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_agg/) supports the keyword DISTINCT.
- INSERT, UPDATE, and DELETE operations now support `SET_VAR`. [#35283](https://github.com/StarRocks/starrocks/pull/35283)

#### Others

- Added the session variable `large_decimal_underlying_type = "panic"|"double"|"decimal"` to set the rules to deal with DECIMAL type overflow. `panic` indicates returning an error immediately, `double` indicates converting the data to DOUBLE type, and `decimal` indicates converting the data to DECIMAL(38,s).

### Developer tools

- Supports Trace Query Profile for asynchronous materialized views, which can be used to analyze its transparent rewrite.

### Behavior Change

To be updated.

### Parameter Change

#### FE Parameters

- Added the following FE configuration items:
  - `catalog_metadata_cache_size`
  - `enable_backup_materialized_view`
  - `enable_colocate_mv_index`
  - `enable_fast_schema_evolution`
  - `json_file_size_limit`
  - `lake_enable_ingest_slowdown`
  - `lake_ingest_slowdown_threshold`
  - `lake_ingest_slowdown_ratio`
  - `lake_compaction_score_upper_bound`
  - `mv_auto_analyze_async`
  - `primary_key_disk_schedule_time`
  - `statistic_auto_collect_small_table_rows`
  - `stream_load_task_keep_max_num`
  - `stream_load_task_keep_max_second`
- Removed FE configuration item `enable_pipeline_load`.
- Default value modifications:
  - The default value of `enable_sync_publish` is changed from `false` to `true`.
  - The default value of `enable_persistent_index_by_default` is changed from `false` to `true`.

#### BE Parameters

- Data Cache-related configuration changes.
  - Added `datacache_enable` to replace `block_cache_enable`.
  - Added `datacache_mem_size` to replace `block_cache_mem_size`.
  - Added `datacache_disk_size` to replace `block_cache_disk_size`.
  - Added `datacache_disk_path` to replace `block_cache_disk_path`.
  - Added `datacache_meta_path` to replace `block_cache_meta_path`.
  - Added `datacache_block_size` to replace `block_cache_block_size`.
  - Added `datacache_checksum_enable` to replace `block_cache_checksum_enable`.
  - Added `datacache_direct_io_enable` to replace `block_cache_direct_io_enable`.
  - Added `datacache_max_concurrent_inserts` to replace `block_cache_max_concurrent_inserts`.
  - Added `datacache_max_flying_memory_mb`.
  - Added `datacache_engine` to replace `block_cache_engine`.
  - Removed `block_cache_max_parcel_memory_mb`.
  - Removed `block_cache_report_stats`.
  - Removed `block_cache_lru_insertion_point`.

  After renaming Block Cache to Data Cache, StarRocks has introduced a new set of BE parameters prefixed with `datacache` to replace the original parameters prefixed with `block_cache`. After upgrade to v3.2, the original parameters will still be effective. Once enabled, the new parameters will override the original ones. The mixed usage of new and original parameters is not supported, as it may result in some configurations not taking effect. In the future, StarRocks plans to deprecate the original parameters with the `block_cache` prefix, so we recommend you use the new parameters with the `datacache` prefix.

- Added the following BE configuration items:
  - `spill_max_dir_bytes_ratio`
  - `streaming_agg_limited_memory_size`
  - `streaming_agg_chunk_buffer_size`
- Removed the following BE configuration items:
  - Dynamic parameter `tc_use_memory_min`
  - Dynamic parameter `tc_free_memory_rate`
  - Dynamic parameter `tc_gc_period`
  - Static parameter `tc_max_total_thread_cache_byte`
- Default value modifications:
  - The default value of `disable_column_pool` is changed from `false` to `true`.
  - The default value of `thrift_port` is changed from `9060` to `0`.
  - The default value of `enable_load_colocate_mv` is changed from `false` to `true`.
  - The default value of `enable_pindex_minor_compaction` is changed from `false` to `true`.

#### System Variables

- Added the following session variables:
  - `enable_per_bucket_optimize`
  - `enable_write_hive_external_table`
  - `hive_temp_staging_dir`
  - `spill_revocable_max_bytes`
  - `thrift_plan_protocol`
- Removed the following session variables:
  - `enable_pipeline_query_statistic`
  - `enable_deliver_batch_fragments`
- Renamed the following session variables:
  - `enable_scan_block_cache` is renamed as `enable_scan_datacache`.
  - `enable_populate_block_cache` is renamed as `enable_populate_datacache`.

#### Reserved Keywords

Added reserved keywords `OPTIMIZE` and `PREPARE`.

### Bug Fixes

Fixed the following issues:

- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Schema Change may fail if it takes an excessively long period of time, because the specified tablet version is handled by garbage collection. [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- Failed to access the Parquet files in MinIO via file external tables. [#29873](https://github.com/StarRocks/starrocks/pull/29873)
- The ARRAY, MAP, and STRUCT type columns are not correctly displayed in  `information_schema.columns`. [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- An error is reported if specific path formats are used during data loading via Broker Load: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`. [#32720](https://github.com/StarRocks/starrocks/pull/32720)
- `DATA_TYPE` and `COLUMN_TYPE` for BINARY or VARBINARY data types are displayed as `unknown` in the `information_schema.columns` view. [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- Complex queries that involve many unions, expressions, and SELECT columns can result in a sudden surge in the bandwidth or CPU usage within an FE node.
- The refresh of asynchronous materialized view may occasionally encounter deadlock. [#35736](https://github.com/StarRocks/starrocks/pull/35736)

### Upgrade Notes

- Optimization on **Random Bucketing** is disabled by default. To enable it, you need to add the property `bucket_size` when creating tables. This allows the system to dynamically adjust the number of tablets based on cluster information and the size of loaded data. Please note that once this optimization is enabled, if you need to roll back your cluster to v3.1 or earlier, you must delete tables with this optimization enabled and manually execute a metadata checkpoint (by executing `ALTER SYSTEM CREATE IMAGE`). Otherwise, the rollback will fail.
- Starting from v3.2.0, StarRocks has disabled non-Pipeline queries. Therefore, before upgrading your cluster to v3.2, you need to globally enable the Pipeline engine (by adding the configuration `enable_pipeline_engine=true` in the FE configuration file **fe.conf**). Failure to do so will result in errors for non-Pipeline queries.
