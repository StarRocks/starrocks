# StarRocks version 2.5

## 2.5.8

Release date: June 30, 2023

### Improvements

- Optimized the error message reported when partitions are added to a non-partitioned table. [#25266](https://github.com/StarRocks/starrocks/pull/25266)
- Optimized the [auto tablet distribution policy](../table_design/Data_distribution.md#determine-the-number-of-buckets) for tables. [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- Optimized the default comments in the CREATE TABLE statement. [#24803](https://github.com/StarRocks/starrocks/pull/24803)
- You can initiate synchronous manual refresh tasks for asynchronous materialized views using REFRESH MATERIALIZED VIEW WITH SYNC MODE. [#25910](https://github.com/StarRocks/starrocks/pull/25910pr)

### Bug Fixes

Fixed the following issues:

- The COUNT result of an asynchronous materialized view may be inaccurate if the materialized view is built on Union results. [#24460](https://github.com/StarRocks/starrocks/issues/24460)
- "Unknown error" is reported when users attempt to forcibly reset the root password. [#25492](https://github.com/StarRocks/starrocks/pull/25492)
- Inaccurate error message is displayed when INSERT OVERWRITE is executed on a cluster with less than three alive BEs. [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 2.5.7

Release date: June 14, 2023

### New features

- Inactive materialized views can be manually activated using `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`. You can use this SQL command to activate materialized views whose base tables were dropped and then recreated. For more information, see [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md). [#24001](https://github.com/StarRocks/starrocks/pull/24001)
- StarRocks can automatically set an appropriate number of tablets when you create a table or add a partition, eliminating the need for manual operations. For more information, see [Determine the number of tablets](../table_design/Data_distribution.md#determine-the-number-of-tablets). [#10614](https://github.com/StarRocks/starrocks/pull/10614)

### Improvements

- Optimized the I/O concurrency of Scan nodes used in external table queries, which reduces memory usage and improves the stability of data loading from external tables. [#23617](https://github.com/StarRocks/starrocks/pull/23617) [#23624](https://github.com/StarRocks/starrocks/pull/23624) [#23626](https://github.com/StarRocks/starrocks/pull/23626)
- Optimized the error message for Broker Load jobs. The error message contains retry information and the name of erroneous files. [#18038](https://github.com/StarRocks/starrocks/pull/18038) [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- Optimized the error message returned when CREATE TABLE times out and added parameter tuning tips. [#24510](https://github.com/StarRocks/starrocks/pull/24510)
- Optimized the error message returned when ALTER TABLE fails because the table status is not Normal. [#24381](https://github.com/StarRocks/starrocks/pull/24381)
- Ignores full-width spaces in the CREATE TABLE statement. [#23885](https://github.com/StarRocks/starrocks/pull/23885)
- Optimized the Broker access timeout to increase the success rate of Broker Load jobs. [#22699](https://github.com/StarRocks/starrocks/pull/22699)
- For Primary Key tables, the `VersionCount` field returned by SHOW TABLET contains Rowsets that are in the Pending state. [#23847](https://github.com/StarRocks/starrocks/pull/23847)
- Optimized the Persistent Index policy. [#22140](https://github.com/StarRocks/starrocks/pull/22140)

### Bug Fixes

Fixed the following issues:

- When users load Parquet data into StarRocks, DATETIME values overflow during type conversion, causing data errors. [#22356](https://github.com/StarRocks/starrocks/pull/22356)
- Bucket information is lost after Dynamic Partitioning is disabled. [#22595](https://github.com/StarRocks/starrocks/pull/22595)
- Using unsupported properties in the CREATE TABLE statement causes null pointer exceptions (NPEs). [#23859](https://github.com/StarRocks/starrocks/pull/23859)
- Table permission filtering in `information_schema` becomes ineffective. As a result, users can view tables they do not have permission to. [#23804](https://github.com/StarRocks/starrocks/pull/23804)
- Information returned by SHOW TABLE STATUS is incomplete. [#24279](https://github.com/StarRocks/starrocks/issues/24279)
- A schema change sometimes may be hung if data loading occurs simultaneously with the schema change. [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- RocksDB WAL flush blocks the brpc worker from processing bthreads, which interrupts high-frequency data loading into Primary Key tables. [#22489](https://github.com/StarRocks/starrocks/pull/22489)
- TIME-type columns that are not supported in StarRocks can be successfully created. [#23474](https://github.com/StarRocks/starrocks/pull/23474)
- Materialized view Union rewrite fails. [#22922](https://github.com/StarRocks/starrocks/pull/22922)

## 2.5.6

Release date: May 19, 2023

### Improvements

- Optimized the error message reported when INSERT INTO ... SELECT expires due to a small `thrift_server_max_worker_thread` value. [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- Tables created using CTAS have three replicas by default, which is consistent with the default replica number for common tables. [#22854](https://github.com/StarRocks/starrocks/pull/22854)

### Bug Fixes

- Truncating partitions fails because the TRUNCATE operation is case-sensitive to partition names. [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- Decommissioning BE fails due to the failure in creating temporary partitions for materialized views. [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- Dynamic FE parameters that require an ARRAY value cannot be set to an empty array. [#22225](https://github.com/StarRocks/starrocks/pull/22225)
- Materialized views with the `partition_refresh_number` property specified may fail to completely refresh. [#21619](https://github.com/StarRocks/starrocks/pull/21619)
- SHOW CREATE TABLE masks cloud credential information, which causes incorrect credential information in memory. [#21311](https://github.com/StarRocks/starrocks/pull/21311)
- Predicates cannot take effect on some ORC files that are queried via external tables. [#21901](https://github.com/StarRocks/starrocks/pull/21901)
- The min-max filter cannot properly handle lower- and upper-case letters in column names. [#22626](https://github.com/StarRocks/starrocks/pull/22626)
- Late materialization causes errors in querying complex data types (STRUCT or MAP).  [#22862](https://github.com/StarRocks/starrocks/pull/22862)
- The issue that occurs when restoring a Primary Key table. [#23384](https://github.com/StarRocks/starrocks/pull/23384)

## 2.5.5

Release date: April 28, 2023

### New features

Added a metric to monitor the tablet status of Primary Key tables:

- Added the FE metric `err_state_metric`.
- Added the `ErrorStateTabletNum` column to the output of `SHOW PROC '/statistic/'` to display the number of **err_state** tablets.
- Added the `ErrorStateTablets` column to the output of `SHOW PROC '/statistic/<db_id>/'` to display the IDs of **err_state** tablets.

For more information, see [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW%20PROC.md).

### Improvements

- Optimized the disk balancing speed when multiple BEs are added. [# 19418](https://github.com/StarRocks/starrocks/pull/19418)
- Optimized the inference of `storage_medium`. When BEs use both SSD and HDD as storage devices, if the property `storage_cooldown_time` is specified, StarRocks sets `storage_medium` to `SSD`. Otherwise, StarRocks sets `storage_medium` to `HDD`. [#18649](https://github.com/StarRocks/starrocks/pull/18649)
- Optimized the performance of Unique Key tables by forbidding the collection of statistics from value columns. [#19563](https://github.com/StarRocks/starrocks/pull/19563)

### Bug Fixes

- For Colocation tables, the replica status can be manually specified as `bad` by using statements like `ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`. If the number of BEs is less than or equal to the number of replicas, the corrupted replica cannot be repaired. [# 17876](https://github.com/StarRocks/starrocks/issues/17876)
- After a BE is started, its process exists but the BE port cannot be enabled. [# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- Wrong results are returned for aggregate queries whose subquery is nested with a window function. [# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- `auto_refresh_partitions_limit` does not take effect when the materialized view (MV) is refreshed for the first time. As a result, all the partitions are refreshed. [# 19759](https://github.com/StarRocks/starrocks/issues/19759)
- An error occurs when querying a CSV Hive external table whose array data is nested with complex data such as MAP and STRUCT. [# 20233](https://github.com/StarRocks/starrocks/pull/20233)
- Queries that use Spark connector time out. [# 20264](https://github.com/StarRocks/starrocks/pull/20264)
- If one replica of a two-replica table is corrupted, the table cannot recover. [# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- Query failure caused by MV query rewrite failure. [# 19549](https://github.com/StarRocks/starrocks/issues/19549)
- The metric interface expires due to database lock. [# 20790](https://github.com/StarRocks/starrocks/pull/20790)
- Wrong results are returned for Broadcast Join. [# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- NPE is returned when an unsupported data type is used in CREATE TABLE. [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- The issue caused by using window_funnel() with the Query Cache feature. [# 21474](https://github.com/StarRocks/starrocks/issues/21474)
- Optimization plan selection takes an unexpectedly long time after the CTE is rewritten. [# 16515](https://github.com/StarRocks/starrocks/pull/16515)

## 2.5.4

Release date: April 4, 2023

### Improvements

- Optimized the performance of rewriting queries on materialized views during query planning. The amount of time taken for query planning is reduced by about 70%. [#19579](https://github.com/StarRocks/starrocks/pull/19579)
- Optimized the type inference logic. If a query like `SELECT sum(CASE WHEN XXX);` contains a constant `0`, such as `SELECT sum(CASE WHEN k1 = 1 THEN v1 ELSE 0 END) FROM test;`, pre-aggregation is automatically enabled to accelerate the query. [#19474](https://github.com/StarRocks/starrocks/pull/19474)
- Supports using `SHOW CREATE VIEW` to view the creation statement of a materialized view. [#19999](https://github.com/StarRocks/starrocks/pull/19999)
- Supports transmitting packets that are 2 GB or larger in size for a single bRPC request between BE nodes. [#20283](https://github.com/StarRocks/starrocks/pull/20283) [#20230](https://github.com/StarRocks/starrocks/pull/20230)
- Supports using [SHOW CREATE CATALOG](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) to query the creation statement of an external catalog.

### Bug Fixes

The following bugs are fixed:

- After queries on materialized views are rewritten, the global dictionary for low-cardinality optimization does not take effect. [#19615](https://github.com/StarRocks/starrocks/pull/19615)
- If a query on materialized views fails to be rewritten, the query fails. [#19774](https://github.com/StarRocks/starrocks/pull/19774)
- If a materialized view is created based on a Primary Key or Unique Key table, queries on that materialized view cannot be rewritten. [#19600](https://github.com/StarRocks/starrocks/pull/19600)
- The column names of materialized views are case-sensitive. However, when you create a table, the table is successfully created without an error message even if column names are incorrect in the `PROPERTIES` of the table creation statement, and moreover the rewriting of queries on materialized views created on that table fails. [#19780](https://github.com/StarRocks/starrocks/pull/19780)
- After a query on materialized views is rewritten, the query plan ma contain partition column-based, invalid predicates, which affect query performance. [#19784](https://github.com/StarRocks/starrocks/pull/19784)
- When data is loaded into a newly created partition, queries on materialized views may fail to be rewritten. [#20323](https://github.com/StarRocks/starrocks/pull/20323)
- Configuring `"storage_medium" = "SSD"` at the creation of materialized views causes the refresh of the materialized views to fail. [#19539](https://github.com/StarRocks/starrocks/pull/19539) [#19626](https://github.com/StarRocks/starrocks/pull/19626)
- Concurrent compaction may happen on Primary Key tables. [#19692](https://github.com/StarRocks/starrocks/pull/19692)
- Compaction does not occur promptly after a large number of DELETE operations. [#19623](https://github.com/StarRocks/starrocks/pull/19623)
- If the expression of a statement contains multiple low-cardinality columns, the expression may fail to be properly rewritten. As a result, the global dictionary for low-cardinality optimization does not take effect. [#20161](https://github.com/StarRocks/starrocks/pull/20161)

## 2.5.3

Release date: March 10, 2023

### Improvements

- Optimized query rewrite for materialized views (MVs).
  - Supports rewriting queries with Outer Join and Cross Join. [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Optimized the data scan logic for MVs, further accelerating the rewritten queries. [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Enhanced rewrite capabilities for single-table aggregate queries.  [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Enhanced rewrite capabilities in View Delta scenarios, which is when the queried tables are a subset of the MV's base tables. [#18800](https://github.com/StarRocks/starrocks/pull/18800)
- Optimized the performance and memory usage when the window function RANK() is used as a filter or a sort key. [#17553](https://github.com/StarRocks/starrocks/issues/17553)

### Bug Fixes

The following bugs are fixed:

- Errors caused by null literals `[]` in ARRAY data. [#18563](https://github.com/StarRocks/starrocks/pull/18563)
- Misuse of the low-cardinality optimization dictionary in some complex query scenarios. The dictionary mapping check is now added before applying the dictionary.  [#17318](https://github.com/StarRocks/starrocks/pull/17318)
- In a single BE environment, Local Shuffle causes GROUP BY to produce duplicate results. [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- Misuses of partition-related PROPERTIES for a non-partitioned MV may cause the MV refresh to fail. The partition PROPERTIES check is now performed when users create an MV. [#18741](https://github.com/StarRocks/starrocks/pull/18741)
- Errors in parsing Parquet Repetition columns. [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- The obtained column's nullable information is incorrect. Solution: When CTAS is used to create a Primary Key table, only the primary key columns are non-nullable; non-primary key columns are nullable. [#16431](https://github.com/StarRocks/starrocks/pull/16431)
- Some issues caused by deleting data from Primary Key tables.  [#18768](https://github.com/StarRocks/starrocks/pull/18768)

## 2.5.2

Release date: February 21, 2023

### New Features

- Supports using the Instance Profile and Assumed Role-based credential methods to access AWS S3 and AWS Glue. [#15958](https://github.com/StarRocks/starrocks/pull/15958)
- Supports the following bit functions: bit_shift_left, bit_shift_right, and bit_shift_right_logical. [#14151](https://github.com/StarRocks/starrocks/pull/14151)

### Improvements

- Optimized the memory release logic, which significantly reduces peak memory usage when a query contains a large number of aggregate queries. [#16913](https://github.com/StarRocks/starrocks/pull/16913)
- Reduced the memory usage of sorting. The memory consumption is halved when a query involves window functions or sorting. [#16937](https://github.com/StarRocks/starrocks/pull/16937) [#17362](https://github.com/StarRocks/starrocks/pull/17362) [#17408](https://github.com/StarRocks/starrocks/pull/17408)

### Bug Fixes

The following bugs are fixed:

- Apache Hive external tables that contain MAP and ARRAY data cannot be refreshed. [#17548](https://github.com/StarRocks/starrocks/pull/17548)
- Superset cannot identify column types of materialized views. [#17686](https://github.com/StarRocks/starrocks/pull/17686)
- BI connectivity fails because SET GLOBAL/SESSION TRANSACTION cannot be parsed. [#17295](https://github.com/StarRocks/starrocks/pull/17295)
- The bucket number of dynamic partitioned tables in a Colocate Group cannot be modified and an error message is returned. [#17418](https://github.com/StarRocks/starrocks/pull/17418/)
- Potential issues caused by a failure in the Prepare stage. [#17323](https://github.com/StarRocks/starrocks/pull/17323)

### Behavior Change

- Changed the default value of `enable_experimental_mv` from `false` to `true`, which means asynchronous materialized view is enabled by default.
- Added CHARACTER to the reserved keyword list. [#17488](https://github.com/StarRocks/starrocks/pull/17488)

## 2.5.1

Release date: February 5, 2023

### Improvements

- Asynchronous materialized views created based on external catalogs support query rewrite.  [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#15791](https://github.com/StarRocks/starrocks/issues/15791)
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
- Provides [Data Cache](../data_source/data_cache.md) to improve access performance of hot data stored in external storage systems, such as HDFS. [#11597](https://github.com/StarRocks/starrocks/pull/11579)
- Supports creating [Delta Lake catalogs](../data_source/catalog/deltalake_catalog.md), which allow direct queries on data from Delta Lake. [#11972](https://github.com/StarRocks/starrocks/issues/11972)
- Hive, Hudi, and Iceberg catalogs are compatible with AWS Glue. [#12249](https://github.com/StarRocks/starrocks/issues/12249)
- Supports creating [file external tables](../data_source/file_external_table.md), which allow direct queries on Parquet and ORC files from HDFS and object stores. [#13064](https://github.com/StarRocks/starrocks/pull/13064)
- Supports creating materialized views based on Hive, Hudi, Iceberg catalogs, and materialized views. For more information, see [Materialized view](../using_starrocks/Materialized_view.md). [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#11873](https://github.com/StarRocks/starrocks/pull/11873)
- Supports conditional updates for tables that use the Primary Key table. For more information, see [Change data through loading](../loading/Load_to_Primary_Key_tables.md). [#12159](https://github.com/StarRocks/starrocks/pull/12159)
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
  - Asynchronous materialized views support automatic and transparent query rewrite based on the SPJG-type materialized views. For more information, see [Materialized view](../using_starrocks/Materialized_view.md#about-async-refresh-mechanisms-for-materialized-views). [#13193](https://github.com/StarRocks/starrocks/issues/13193)
  - Asynchronous materialized views support multiple async refresh mechanisms. For more information, see [Materialized view](../using_starrocks/Materialized_view.md#enable-query-rewrite-based-on-async-materialized-views). [#12712](https://github.com/StarRocks/starrocks/pull/12712) [#13171](https://github.com/StarRocks/starrocks/pull/13171) [#13229](https://github.com/StarRocks/starrocks/pull/13229) [#12926](https://github.com/StarRocks/starrocks/pull/12926)
  - The efficiency of refreshing materialized views is improved. [#13167](https://github.com/StarRocks/starrocks/issues/13167)
- Optimized the following aspects of data loading:
  - Optimized loading performance in multi-replica scenarios by supporting the "single leader replication" mode. Data loading gains a one-fold performance lift. For more information about "single leader replication", see `replicated_storage` in [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md). [#10138](https://github.com/StarRocks/starrocks/pull/10138)
  - Broker Load and Spark Load no longer need to depend on brokers for data loading when only one HDFS cluster or one Kerberos user is configured. However, if you have multiple HDFS clusters or multiple Kerberos users, you still need to deploy a broker. For more information, see [Load data from HDFS or cloud storage](../loading/BrokerLoad.md) and [Bulk load using Apache Spark™](../loading/SparkLoad.md). [#9049](https://github.com/starrocks/starrocks/pull/9049) [#9228](https://github.com/StarRocks/starrocks/pull/9228)
  - Optimized the performance of Broker Load when a large number of small ORC files are loaded. [#11380](https://github.com/StarRocks/starrocks/pull/11380)
  - Reduced the memory usage when you load data into Primary Key tables.
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
