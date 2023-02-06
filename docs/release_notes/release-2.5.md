# StarRocks version 2.5

## 2.5.1

Release date: February 5, 2023

### Improvements

- Multi-table materialized views created based on external catalogs support query rewrite.  [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#15791](https://github.com/StarRocks/starrocks/issues/15791)
- Allows users to specify a collection period for automatic CBO statistics collection, which prevents cluster performance jitter caused by automatic full collection. [#14996](https://github.com/StarRocks/starrocks/pull/14996)
- Added Thrift server queue. Requests that cannot be processed immediately during INSERT INTO SELECT can be pending in the Thrift server queue, preventing requests from being rejected. [#14571](https://github.com/StarRocks/starrocks/pull/14571)
- Deprecated the FE parameter `default_storage_medium`. If `storage_medium` is not explicitly specified when users create a table, the system automatically infers the storage medium of the table based on BE disk type. For more information, see description of `storage_medium` in [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20VIEW.md). [#14394](https://github.com/StarRocks/starrocks/pull/14394)

### Bug Fixes

The following bugs are fixed:

- Null pointer exception (NPE) caused by SET PASSWORD. [#15247](https://github.com/StarRocks/starrocks/pull/15247)
- JSON data with empty keys cannot be parsed. [#16852](https://github.com/StarRocks/starrocks/pull/16852)
- Data of invalid types can be successfully converted into ARRAY data. [#16866](https://github.com/StarRocks/starrocks/pull/16866)
- Nested Loop Join cannot be interrupted when an exception occurs.  [#16875](https://github.com/StarRocks/starrocks/pull/16875)

### Behavior Change

- Deprecated the FE parameter `default_storage_medium`. The storage medium of a table is automatically inferred by the system. [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.5.0

Release date: January 22, 2023

### New Features

- Supports querying Merge On Read tables using [Hudi catalogs](../data_source/catalog/hudi_catalog.md) and [Hudi external tables](../data_source/External_table.md#hudi-external-table). [#6780](https://github.com/StarRocks/starrocks/pull/6780)
- Supports querying STRUCT and MAP data using [Hive catalogs](../data_source/catalog/hive_catalog.md), Hudi catalogs, and [Iceberg catalogs](../data_source/catalog/iceberg_catalog.md). [#10677](https://github.com/StarRocks/starrocks/issues/10677)
- Provides [Block Cache](../data_source/Block_cache.md) to improve access performance of hot data stored in external storage systems, such as HDFS. [#11597](https://github.com/StarRocks/starrocks/pull/11579)
- Supports creating [Delta Lake catalogs](../data_source/catalog/deltalake_catalog.md), which allow direct queries on data from Delta Lake. [#11972](https://github.com/StarRocks/starrocks/issues/11972)
- Hive, Hudi, and Iceberg catalogs are compatible with AWS Glue. [#12249](https://github.com/StarRocks/starrocks/issues/12249)
- Supports creating [file external tables](../data_source/file_external_table.md), which allow direct queries on Parquet and ORC files from HDFS and object stores. [#13064](https://github.com/StarRocks/starrocks/pull/13064)
- Supports creating materialized views based on Hive, Hudi, Iceberg catalogs, and materialized views. For more information, see [Materialized view](../using_starrocks/Materialized_view.md). [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#11873](https://github.com/StarRocks/starrocks/pull/11873)
- Supports conditional updates for tables that use the Primary Key model. For more information, see [Change data through loading](../loading/Load_to_Primary_Key_tables.md). [#12159](https://github.com/StarRocks/starrocks/pull/12159)
- Supports [Query Cache](../using_starrocks/query_cache.md), which stores intermediate computation results of queries, improving the QPS and reduces the average latency of highly-concurrent, simple queries. [#9194](https://github.com/StarRocks/starrocks/pull/9194)
- Supports specifying the priority of Broker Load jobs. For more information, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) [#11029](https://github.com/StarRocks/starrocks/pull/11029)
- Supports specifying the number of replicas for data loading for StarRocks native tables. For more information, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md). [#11253](https://github.com/StarRocks/starrocks/pull/11253)
- Supports [query queues](../administration/query_queues.md). [#12594](https://github.com/StarRocks/starrocks/pull/12594)
- Supports isolating compute resources occupied by data loading, thereby limiting the resource consumption of data loading tasks. For more information, see [Resource group](../administration/resource_group.md). [#12606](https://github.com/StarRocks/starrocks/pull/12606)
- Supports specifying the following data compression algorithms for StarRocks native tables: LZ4, Zstd, Snappy, and Zlib. For more information, see [Data compression](../table_design/data_compression.md). [#10097](https://github.com/StarRocks/starrocks/pull/10097) [#12020](https://github.com/StarRocks/starrocks/pull/12020)
- Supports [user-defined variables](../reference/user_defined_variables.md). [#10011](https://github.com/StarRocks/starrocks/pull/10011)
- Supports [lambda expression](../sql-reference/sql-functions/Lambda_expression.md) and the following higher-order functions: [array_map](../sql-reference/sql-functions/array-functions/array_map.md), [array_filter](../sql-reference/sql-functions/array-functions/array_filter.md), [array_sum](../sql-reference/sql-functions/array-functions/array_sum.md), and [array_sortby](../sql-reference/sql-functions/array-functions/array_sortby.md). [#9461](https://github.com/StarRocks/starrocks/pull/9461) [#9806](https://github.com/StarRocks/starrocks/pull/9806) [#10323](https://github.com/StarRocks/starrocks/pull/10323) [#14034](https://github.com/StarRocks/starrocks/pull/14034)
- Provides the QUALIFY clause that filters the results of [window functions](../sql-reference/sql-functions/Window_function.md). [#13239](https://github.com/StarRocks/starrocks/pull/13239)
- Supports using the result returned by the uuid() and uuid_numeric() functions as the default value of a column when you create a table. For more information, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md). [#11155](https://github.com/StarRocks/starrocks/pull/11155)
- Supports the following functions: [map_size](../sql-reference/sql-functions/map-functions/map_size.md), [map_keys](../sql-reference/sql-functions/map-functions/map_keys.md), [map_values](../sql-reference/sql-functions/map-functions/map_values.md), [max_by](../sql-reference/sql-functions/aggregate-functions/max_by.md), [sub_bitmap](../sql-reference/sql-functions/bitmap-functions/sub_bitmap.md), [bitmap_to_base64](../sql-reference/sql-functions/bitmap-functions/bitmap_to_base64.md), [host_name](../sql-reference/sql-functions/utility-functions/host_name.md), and [date_slice](../sql-reference/sql-functions/date-time-functions/date_slice.md). [#11299](https://github.com/StarRocks/starrocks/pull/11299) [#11323](https://github.com/StarRocks/starrocks/pull/11323) [#12243](https://github.com/StarRocks/starrocks/pull/12243) [#11776](https://github.com/StarRocks/starrocks/pull/11776) [#12634](https://github.com/StarRocks/starrocks/pull/12634) [#14225](https://github.com/StarRocks/starrocks/pull/14225)

### Improvements

- Optimized the metadata access performance when you query external data using [Hive catalogs](../data_source/catalog/hive_catalog.md), [Hudi catalogs](../data_source/catalog/hudi_catalog.md), and [Iceberg catalogs](../data_source/catalog/iceberg_catalog.md). [#11349](https://github.com/StarRocks/starrocks/issues/11349)
- Supports querying ARRAY data using [Elasticsearch external tables](../data_source/External_table.md#elasticsearch-external-table). [#9693](https://github.com/StarRocks/starrocks/pull/9693)
- Optimized the following aspects of materialized views:
  - Multi-table async refresh materialized views support automatic and transparent query rewrite based on the SPJG-type materialized views. For more information, see [Materialized view](../using_starrocks/Materialized_view.md#about-async-refresh-mechanisms-for-materialized-views). [#13193](https://github.com/StarRocks/starrocks/issues/13193)
  - Multi-table async refresh materialized views support multiple async refresh mechanisms. For more information, see [Materialized view](../using_starrocks/Materialized_view.md#enable-query-rewrite-based-on-async-materialized-views). [#12712](https://github.com/StarRocks/starrocks/pull/12712) [#13171](https://github.com/StarRocks/starrocks/pull/13171) [#13229](https://github.com/StarRocks/starrocks/pull/13229) [#12926](https://github.com/StarRocks/starrocks/pull/12926)
  - The efficiency of refreshing materialized views is improved. [#13167](https://github.com/StarRocks/starrocks/issues/13167)
- StarRocks automatically sets an appropriate number of tablets when you create a table, eliminating the need for manual operations. For more information, see [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md). [#10614](https://github.com/StarRocks/starrocks/pull/10614)
- Optimized the following aspects of data loading:
  - Optimized loading performance in multi-replica scenarios by supporting the "single leader replication" mode. Data loading gains a one-fold performance lift. For more information about "single leader replication", see `replicated_storage` in [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md). [#10138](https://github.com/StarRocks/starrocks/pull/10138)
  - Broker Load and Spark Load no longer need to depend on brokers for data loading when only one HDFS system or Kerberos is configured. For more information, see [Load data from HDFS or cloud storage](../loading/BrokerLoad.md) and [Bulk load using Apache Spark™](../loading/SparkLoad.md). [#9049](https://github.com/starrocks/starrocks/pull/9049) [#9228](https://github.com/StarRocks/starrocks/pull/9228)
  - Optimized the performance of Broker Load when a large number of small ORC files are loaded. [#11380](https://github.com/StarRocks/starrocks/pull/11380)
  - Reduced the memory usage when you load data into tables of the Primary Key Model.
- Optimized the `information_schema` database and the `tables` and `columns` tables within. Adds a new table `table_config`. For more information, see [Information Schema](../administration/information_schema.md). [#10033](https://github.com/StarRocks/starrocks/pull/10033)
- Optimized data backup and restore:
  - Supports backing up and restoring data from multiple tables in a database at a time. For more information, see [Backup and restore data](../administration/Backup_and_restore.md). [#11619](https://github.com/StarRocks/starrocks/issues/11619)
  - Supports backing up and restoring data from Primary Key tables. For more information, see Backup and restore. [#11885](https://github.com/StarRocks/starrocks/pull/11885)
- Optimized the following functions:
  - Added an optional parameter for the [time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md) function, which is used to determine whether the beginning or end of the time interval is returned. [#11216](https://github.com/StarRocks/starrocks/pull/11216)
  - Added a new mode `INCREASE` for the [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md) function to avoid computing duplicate timestamps. [#10134](https://github.com/StarRocks/starrocks/pull/10134)
  - Supports specifying multiple arguments in the [unnest](../sql-reference/sql-functions/array-functions/unnest.md) function. [#12484](https://github.com/StarRocks/starrocks/pull/12484)
  - lead() and lag() functions support querying HLL and BITMAP data. For more information, see [Window function](../sql-reference/sql-functions/Window_function.md). [#12108](https://github.com/StarRocks/starrocks/pull/12108)
  - The following ARRAY functions support querying JSON data: [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md), [array_sort](../sql-reference/sql-functions/array-functions/array_sort.md), [array_concat](../sql-reference/sql-functions/array-functions/array_concat.md), [array_slice](../sql-reference/sql-functions/array-functions/array_slice.md), and [reverse](../sql-reference/sql-functions/array-functions/reverse.md). [#13155](https://github.com/StarRocks/starrocks/pull/13155)
  - Optimized the use of some functions. The `current_date`, `current_timestamp`, `current_time`, `localtimestamp`, and `localtime` functions can be executed without using `()`, for example, you can directly run `select current_date;`. [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- Removed some redundant information from FE logs. [# 15374](https://github.com/StarRocks/starrocks/pull/15374)

### Bug Fixes

The following bugs are fixed:

- The append_trailing_char_if_absent() function may return an incorrect result when the first argument is empty. [#13762](https://github.com/StarRocks/starrocks/pull/13762)
- After a table is restored using the RECOVER statement, the table does not exist. [#13921](https://github.com/StarRocks/starrocks/pull/13921)
- The result returned by the SHOW CREATE MATERIALIZED VIEW statement does not contain the database and catalog specified in the query statement when the materialized view was created. [#12833](https://github.com/StarRocks/starrocks/pull/12833)
- Schema change jobs in the `waiting_stable` state cannot be canceled. [#12530](https://github.com/StarRocks/starrocks/pull/12530)
- Running the `SHOW PROC '/statistic';` command on a Leader FE and non-Leader FE returns different results. [#12491](https://github.com/StarRocks/starrocks/issues/12491)
- The position of the ORDER BY clause is incorrect in the result returned by SHOW CREATE TABLE. [# 13809](https://github.com/StarRocks/starrocks/pull/13809)
- When users use Hive Catalog to query Hive data, if the execution plan generated by FE does not contain partition IDs, BEs fail to query Hive partition data. [# 15486](https://github.com/StarRocks/starrocks/pull/15486).

### Behavior Change

- Changed the default value of the `AWS_EC2_METADATA_DISABLED` parameter to `False`, which means that the metadata of Amazon EC2 is obtained to access AWS resources.
- Renamed session variable `is_report_success` to `enable_profile`, which can be queried using the SHOW VARIABLES statement.
- Added four reserved keywords: `CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIME`, and `LOCALTIMESTAMP`. [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- The maximum length of table and database names can be up to 1023 characters. [# 14929](https://github.com/StarRocks/starrocks/pull/14929) [# 15020](https://github.com/StarRocks/starrocks/pull/15020)
- BE configuration items `enable_event_based_compaction_framework` and `enable_size_tiered_compaction_strategy` are set to `true` by default, which significantly reduces compaction overheads when there are a large number of tablets or a single tablet has large data volume.

### Upgrade Notes

- You can upgrade your cluster to 2.5.0 from 2.0.x, 2.1.x, 2.2.x, 2.3.x, or 2.4.x. However, if you need to perform a rollback, we recommend that you roll back only to 2.4.x.
- If you have a partitioned table that uses LIST partitioning, you must delete this table before the upgrade.
