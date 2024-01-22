---
displayed_sidebar: "English"
---

# StarRocks version 2.3

## 2.3.18

Release date: October 11, 2023

### Bug Fixes

Fixed the following issues:

- The bug in the third-party library librdkafka causes the load tasks of Routine Load jobs to get stuck during data loading, and newly created load tasks also fail to execute. [#28301](https://github.com/StarRocks/starrocks/pull/28301)
- Spark or Flink connectors may fail to export data because of inaccurate memory statistics. [#31200](https://github.com/StarRocks/starrocks/pull/31200) [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- BEs crash when a Stream Load job uses the keyword `if`. [#31926](https://github.com/StarRocks/starrocks/pull/31926)
- An error `"get TableMeta failed from TNetworkAddress"` is reported when data is loaded into partitioned StarRocks external tables.  [#30466](https://github.com/StarRocks/starrocks/pull/30466)

## 2.3.17

Release date: September 4, 2023

### Bug Fixes

Fixed the following issue:

- A Routine Load job fails to consume data. [#29883](https://github.com/StarRocks/starrocks/issues/29883) [#18550](https://github.com/StarRocks/starrocks/pull/18550)

## 2.3.16

Release date: August 4, 2023

### Bug Fixes

Fixed the following issue:

FE memory leak caused by blocked LabelCleaner threads. [#28311](https://github.com/StarRocks/starrocks/pull/28311) [#28636](https://github.com/StarRocks/starrocks/pull/28636)

## 2.3.15

Release date: July 31, 2023

### Improvements

- Optimized tablet scheduling logic to prevent tablets remaining pending for a long period or an FE crashing under certain circumstances. [#21647](https://github.com/StarRocks/starrocks/pull/21647) [#23062](https://github.com/StarRocks/starrocks/pull/23062) [#25785](https://github.com/StarRocks/starrocks/pull/25785)
- Optimized the scheduling logic of TabletChecker to prevent the checker from repeatedly scheduling tablets that are not repaired. [#27648](https://github.com/StarRocks/starrocks/pull/27648)
- The partition metadata records visibleTxnId, which corresponds to the visible version of tablet replicas. When the version of a replica is inconsistent with others, it is easier to trace the transaction that created this version. [#27924](https://github.com/StarRocks/starrocks/pull/27924)

### Bug Fixes

Fixed the following issues:

- Incorrect table-level scan statistics in the FEs cause inaccurate metrics related to table queries and loading. [#28022](https://github.com/StarRocks/starrocks/pull/28022)
- BEs may crash if the Join key is a large BINARY column. [#25084](https://github.com/StarRocks/starrocks/pull/25084)
- An aggregate operator may trigger thread safety issues in certain scenarios, causing BEs to crash. [#26092](https://github.com/StarRocks/starrocks/pull/26092)
- The version number for a tablet is inconsistent between the BE and FE after data is restored by using [RESTORE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/RESTORE/). [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- Partitions cannot be automatically created after the table is recovered by using [RECOVER](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/RECOVER/). [#26813](https://github.com/StarRocks/starrocks/pull/26813)
- The loading transaction is stuck in the Pending state and DDL statements are hung if data to be loaded using INSERT INTO does not meet quality requirements and the strict mode is enabled for data loading. [#27140](https://github.com/StarRocks/starrocks/pull/27140)
- Some INSERT jobs return `[42000][1064] Dict Decode failed, Dict can't take cover all key :0` if low-cardinality optimization is enabled. [#27395](https://github.com/StarRocks/starrocks/pull/27395)
- In certain cases, the INSERT INTO SELECT operation times out when the Pipeline is not enabled. [#26594](https://github.com/StarRocks/starrocks/pull/26594)
- The query returns no data when the query condition is `WHERE partition_column < xxx` and the value in `xxx` is only accurate to the hour, not to minute and second, for example, `2023-7-21 22`. [#27780](https://github.com/StarRocks/starrocks/pull/27780)

## 2.3.14

Release date: June 28, 2023

### Improvements

- Optimized the error message returned when CREATE TABLE times out and added parameter tuning tips. [#24510](https://github.com/StarRocks/starrocks/pull/24510)
- Optimized the memory usage for the Primary Key tables with a large number of accumulated tablet versions. [#20760](https://github.com/StarRocks/starrocks/pull/20760)
- The synchronization of StarRocks external table metadata has been changed to occur during data loading. [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- Removed the dependency of NetworkTime on system clocks to fix incorrect NetworkTime caused by inconsistent system clocks across servers. [#24858](https://github.com/StarRocks/starrocks/pull/24858)

### Bug Fixes

Fixed the following issues:

- When low cardinality dictionary optimization is applied for small tables that undergo frequent TRUNCATE operations, queries may encounter errors. [#23185](https://github.com/StarRocks/starrocks/pull/23185)
- BEs may crash when a view that contains UNION whose first child is a constant NULL is queried. [#13792](https://github.com/StarRocks/starrocks/pull/13792)  
- In some cases, a query based on Bitmap Index may return an error. [#23484](https://github.com/StarRocks/starrocks/pull/23484)
- The result of rounding a DOUBLE or FLOAT value into a DECIMAL value in BEs is inconsistent with the result in FEs. [#23152](https://github.com/StarRocks/starrocks/pull/23152)
- A schema change sometimes may be hung if data loading occurs simultaneously with the schema change. [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- When you load a Parquet file into StarRocks by using Broker Load, Spark connector or Flink connector, BE OOM issues may occur. [#25254](https://github.com/StarRocks/starrocks/pull/25254)
- The error message `unknown error` is returned when a constant is specified in the ORDER BY clause and there is a LIMIT clause in the query. [#25538](https://github.com/StarRocks/starrocks/pull/25538)

## 2.3.13

Release date: June 1, 2023

### Improvements

- Optimized the error message reported when INSERT INTO ... SELECT expires due to a small `thrift_server_max_worker_thread` value. [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- Reduced memory consumption and optimized performance for multi-table joins that use the `bitmap_contains` function. [#20617](https://github.com/StarRocks/starrocks/pull/20617) [#20653](https://github.com/StarRocks/starrocks/pull/20653)

### Bug Fixes

The following bugs are fixed:

- Truncating partitions fails because the TRUNCATE operation is case-sensitive to partition names. [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- Loading int96 timestamp data from Parquet files causes data overflow. [#22355](https://github.com/StarRocks/starrocks/issues/22355)
- Decommissioning a BE fails after a materialized view is dropped. [#22743](https://github.com/StarRocks/starrocks/issues/22743)
- When the execution plan for a query includes a Broadcast Join followed by a Bucket Shuffle Join, such as `SELECT * FROM t1 JOIN [Broadcast] t2 ON t1.a = t2.b JOIN [Bucket] t3 ON t2.b = t3.c`, and the data of the equijoin key of the left table in the Broadcast Join is deleted before the data is sent to Bucket Shuffle Join, a BE may crash. [#23227](https://github.com/StarRocks/starrocks/pull/23227)
- When the execution plan for a query includes a Cross Join followed by a Hash Join, and the right table of the Hash Join within a fragment instance is empty, the returned result may be incorrect. [#23877](https://github.com/StarRocks/starrocks/pull/23877)
- Decommissioning a BE fails due to failures in creating temporary partitions for materialized views. [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- If a SQL statement contains a STRING value with multiple escape characters, the SQL statement cannot be parsed. [#23119](https://github.com/StarRocks/starrocks/issues/23119)
- Querying the data with the maximum value in the partition column fails. [#23153](https://github.com/StarRocks/starrocks/issues/23153)
- Load jobs fail after StarRocks rolls back from v2.4 to v2.3. [#23642](https://github.com/StarRocks/starrocks/pull/23642)
- Issues related to column pruning and reuse. [#16624](https://github.com/StarRocks/starrocks/issues/16624)

## 2.3.12

Release date: April 25, 2023

### Improvements

Supports implicit conversion if the returned value of an expression can be converted to a valid Boolean value. [# 21792](https://github.com/StarRocks/starrocks/pull/21792)

### Bug Fixes

The following bugs are fixed:

- If a user's LOAD_PRIV is granted at the table level, an error message `Access denied; you need (at least one of) the LOAD privilege(s) for this operation`  is returned at transaction rollback in the event of a load job failure. [# 21129](https://github.com/StarRocks/starrocks/issues/21129)
- After ALTER SYSTEM DROP BACKEND is executed to drop a BE, the replicas of tables whose replication number is set to 2 on that BE cannot be repaired. In this situation, data loads into these tables fail. [# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- NPE is returned when an unsupported data type is used in CREATE TABLE. [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- The shortcircut logic of the Broadcast Join is abnormal, leading to incorrect query results. [# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- Disk usage may increase significantly after materialized views are used. [# 20590](https://github.com/StarRocks/starrocks/pull/20590)
- The Audit Loader plugin cannot be completely uninstalled. [# 20468](https://github.com/StarRocks/starrocks/issues/20468)
- The number of rows displayed in the result of `INSERT INTO XXX SELECT` may not match the result of `SELECT COUNT(*) FROM XXX`. [# 20084](https://github.com/StarRocks/starrocks/issues/20084)
- If a subquery uses window functions and its parent query uses the GROUP BY clause, the query result cannot be aggregated. [# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- When a BE is started, the BE process exists but all the BE’s ports cannot be open. [# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- If the disk I/O is exceedingly high, transactions on Primary Key tables are slowly committed, and consequently queries on these tables may return an error "backend not found". [# 18835](https://github.com/StarRocks/starrocks/issues/18835)

## 2.3.11

Release date: March 28, 2023

### Improvements

- Executing complex queries that contain many expressions usually generates a large number of `ColumnRefOperators`. Originally, StarRocks uses `BitSet` to store `ColumnRefOperator::id`, which consumes a large amount of memory. In order to reduce memory usage, StarRocks now uses `RoaringBitMap` to store `ColumnRefOperator::id`. [#16499](https://github.com/StarRocks/starrocks/pull/16499)
- A new I/O scheduling strategy is introduced to reduce the performance impact of large queries on small queries. To enable the new I/O scheduling strategy, configure the BE static parameter `pipeline_scan_queue_mode=1` in **be.conf** and then restart BEs. [#19009](https://github.com/StarRocks/starrocks/pull/19009)

### Bug Fixes

The following bugs are fixed:

- A table whose expired data is not properly recycled occupies a relatively large portion of disk space. [#19796](https://github.com/StarRocks/starrocks/pull/19796)
- The error message displayed in the following scenario is not informative: A Broker Load job loads Parquet files into StarRocks and a `NULL` value is loaded into a NOT NULL column. [#19885](https://github.com/StarRocks/starrocks/pull/19885)
- Frequently creating a large number of temporary partitions to replace existing partitions leads to memory leaks and Full GC on the FE nodes. [#19283](https://github.com/StarRocks/starrocks/pull/19283)
- For Colocation tables, the replica status can be manually specified as `bad` by using statements like `ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`. If the number of BEs is less than or equal to the number of replicas, the corrupted replica cannot be repaired. [#19443](https://github.com/StarRocks/starrocks/pull/19443)
- When the request `INSERT INTO SELECT` is sent to a Follower FE, the parameter `parallel_fragment_exec_instance_num` does not take effect. [#18841](https://github.com/StarRocks/starrocks/pull/18841)
- When the operator `<=>` is used to compare a value with a `NULL` value, the comparison result is incorrect. [#19210](https://github.com/StarRocks/starrocks/pull/19210)
- The query concurrency metric decreases slowly when the concurrency limit of a resource group is continuously reached. [#19363](https://github.com/StarRocks/starrocks/pull/19363)
- Highly concurrent data load jobs may cause the error `"get database read lock timeout, database=xxx"`. [#16748](https://github.com/StarRocks/starrocks/pull/16748) [#18992](https://github.com/StarRocks/starrocks/pull/18992)

## 2.3.10

Release date: March 9, 2023

### Improvements

Optimized the inference of `storage_medium`. When BEs use both SSD and HDD as storage devices,  if the property `storage_cooldown_time` is specified, StarRocks sets `storage_medium` to `SSD`. Otherwise, StarRocks sets `storage_medium` to `HDD`. [#18649](https://github.com/StarRocks/starrocks/pull/18649)

### Bug Fixes

The following bugs are fixed:

- A query may fail if ARRAY data from Parquet files in data lakes is queried. [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- The Stream Load job initiated by a program is hung and the FE does not receive the HTTP request sent by the program. [#18559](https://github.com/StarRocks/starrocks/pull/18559)
- An error may occur when an Elasticsearch external table is queried. [#13727](https://github.com/StarRocks/starrocks/pull/13727)
- BEs may crash if an expression encounters an error during initialization. [#11396](https://github.com/StarRocks/starrocks/pull/11396)
- A query may fail if the SQL statement uses an empty array literal `[]`. [#18550](https://github.com/StarRocks/starrocks/pull/18550)
- After StarRocks is upgraded from version 2.2 and later to version 2.3.9 and later, an error `No match for <expr> with operand types xxx and xxx` may occur when a Routine Load job is created with a calculation expression specified in the `COLUMN` parameter. [#17856](https://github.com/StarRocks/starrocks/pull/17856)
- A load job is hung after a BE restarts. [#18488](https://github.com/StarRocks/starrocks/pull/18488)
- When a SELECT statement uses an OR operator in the WHERE clause, extra partitions are scanned. [#18610](https://github.com/StarRocks/starrocks/pull/18610)

## 2.3.9

Release date: February 20, 2023

### Bug Fixes

- During a schema change, if a tablet clone is triggered and the BE nodes on which the tablet replicas reside change, the schema change fails. [#16948](https://github.com/StarRocks/starrocks/pull/16948)
- The string returned by the group_concat() function is truncated. [#16948](https://github.com/StarRocks/starrocks/pull/16948)
- When you use Broker Load to load data from HDFS through Tencent Big Data Suite (TBDS), an error `invalid hadoop.security.authentication.tbds.securekey` occurs, indicating that StarrRocks cannot access HDFS by using the authentication information provided by TBDS. [#14125](https://github.com/StarRocks/starrocks/pull/14125) [#15693](https://github.com/StarRocks/starrocks/pull/15693)
- In some cases, CBO may use incorrect logic to compare whether two operators are equivalent. [#17227](https://github.com/StarRocks/starrocks/pull/17227) [#17199](https://github.com/StarRocks/starrocks/pull/17199)
- When you connect to a non-Leader FE node and send the SQL statement `USE <catalog_name>.<database_name>`, the non-Leader FE node forwards the SQL statement, with `<catalog_name>` excluded, to the Leader FE node. As a result, the Leader FE node chooses to use the `default_catalog` and eventually fails to find the specified database. [#17302](https://github.com/StarRocks/starrocks/pull/17302)

## 2.3.8

Release date: February 2, 2023

### Bug fixes

The following bugs are fixed:

- When resources are released after a large query finishes, there is a low probability that other queries are slowed down. This issue is more likely to occur if resource groups are enabled or the large query ends unexpectedly. [#16454](https://github.com/StarRocks/starrocks/pull/16454) [#16602](https://github.com/StarRocks/starrocks/pull/16602)
- For a Primary Key table, if a replica's metadata version falls behind, StarRocks incrementally clones the missing metadata from other replicas to this replica. In this process, StarRocks pulls a large number of versions of metadata, and if too many versions of metadata accumulate without timely GC, excessive memory may be consumed and consequently the BEs may encounter OOM exceptions. [#15935](https://github.com/StarRocks/starrocks/pull/15935)
- If an FE sends an occasional heartbeat to a BE, and the heartbeat connection times out, the FE considers the BE unavailable, leading to transaction failures on the BE. [# 16386](https://github.com/StarRocks/starrocks/pull/16386)
- When you use a StarRocks external table to load data between StarRocks clusters, if the source StarRocks cluster is in an earlier version and the target StarRocks cluster is in a later version (2.2.8 ~ 2.2.11, 2.3.4 ~ 2.3.7, 2.4.1 or 2.4.2), the data loading fails. [#16173](https://github.com/StarRocks/starrocks/pull/16173)
- BEs crash when multiple queries run concurrently and memory usage is relatively high. [#16047](https://github.com/StarRocks/starrocks/pull/16047)
- When dynamic partitioning is enabled for a table and some partitions are dynamically deleted, if you execute TRUNCATE TABLE, an error `NullPointerException` is returned. Meanwhile, if you load data into the table, the FEs crash and can not restart. [#16822](https://github.com/StarRocks/starrocks/pull/16822)

## 2.3.7

Release date: December 30, 2022

### Bug fixes

The following bugs are fixed:

- The column that is allowed to be NULL in a StarRocks table is incorrectly set to NOT NULL in a view created from that table. [#15749](https://github.com/StarRocks/starrocks/pull/15749)
- A new tablet version is generated when data is loaded into StarRocks. However, the FE may not yet detect the new tablet version and still requires BEs to read the historical version of the tablet. If the garbage collection mechanism removes the historical version, the query cannot find the historical version and an error "Not found: get_applied_rowsets(version xxxx) failed tablet:xxx #version:x [xxxxxxx]" is returned. [#15726](https://github.com/StarRocks/starrocks/pull/15726)
- FE takes up too much memory when data is frequently loaded. [#15377](https://github.com/StarRocks/starrocks/pull/15377)
- For aggregate queries and multi-table JOIN queries, the statistics are not collected accurately and CROSS JOIN occurs in the execution plans, resulting in long query latency. [#12067](https://github.com/StarRocks/starrocks/pull/12067)  [#14780](https://github.com/StarRocks/starrocks/pull/14780)

## 2.3.6

Release date: December 22, 2022

### Improvements

- The Pipeline execution engine supports INSERT INTO statements. To enable it, set the FE configuration item `enable_pipeline_load_for_insert` to `true`.  [#14723](https://github.com/StarRocks/starrocks/pull/14723)
- The memory used by Compaction for the Primary Key table is reduced. [#13861](https://github.com/StarRocks/starrocks/pull/13861)  [#13862](https://github.com/StarRocks/starrocks/pull/13862)

### Behavior Change

- Deprecated the FE parameter `default_storage_medium`. The storage medium of a table is automatically inferred by the system. [#14394](https://github.com/StarRocks/starrocks/pull/14394)

### Bug Fixes

The following bugs are fixed:

- BEs may hang up when the resource group feature is enabled and multiple resource groups run queries at the same time. [#14905](https://github.com/StarRocks/starrocks/pull/14905)
- When you create a materialized view by using CREATE MATERIALIZED VIEW AS SELECT, if the SELECT clause does not use aggregate functions, and uses GROUP BY, for example `CREATE MATERIALIZED VIEW test_view AS SELECT a,b from test group by b,a order by a;`, then the BE nodes all crash. [#13743](https://github.com/StarRocks/starrocks/pull/13743)
- If you restart the BE immediately after you use INSERT INTO to frequently load data into the Primary Key table to make data changes, the BE may restart very slowly. [#15128](https://github.com/StarRocks/starrocks/pull/15128)
- If only JRE is installed on the environment and JDK is not installed, queries fail after FE restarts. After the bug is fixed, FE cannot restart in that environment and it returns error `JAVA_HOME can not be jre`. To successfully restart FE, you need to install JDK on the environment. [#14332](https://github.com/StarRocks/starrocks/pull/14332)
- Queries cause BE crashes. [#14221](https://github.com/StarRocks/starrocks/pull/14221)
- `exec_mem_limit` cannot be set to an expression. [#13647](https://github.com/StarRocks/starrocks/pull/13647)
- You cannot create a sync refreshed materialized view based on subquery results. [#13507](https://github.com/StarRocks/starrocks/pull/13507)
- The comments for columns are deleted after you refresh the Hive external table. [#13742](https://github.com/StarRocks/starrocks/pull/13742)
- During a correlated JOIN, the right table is processed before the left table and the right table is very large. If compaction is performed on the left table while the right table is being processed, the BE node crashes. [#14070](https://github.com/StarRocks/starrocks/pull/14070)
- If the Parquet file column names are case-sensitive, and the query condition uses upper-case column names from the Parquet file, the query returns no result. [#13860](https://github.com/StarRocks/starrocks/pull/13860) [#14773](https://github.com/StarRocks/starrocks/pull/14773)
- During bulk loading, if the number of connections to Broker exceeds the default maximum number of connections, Broker is disconnected and the loading job fails with an error message `list path error`. [#13911](https://github.com/StarRocks/starrocks/pull/13911)
- When BEs are highly loaded, the metric for resource groups `starrocks_be_resource_group_running_queries` may be incorrect. [#14043](https://github.com/StarRocks/starrocks/pull/14043)
- If the query statement uses OUTER JOIN, it may cause the BE node to crash. [#14840](https://github.com/StarRocks/starrocks/pull/14840)
- After you create an asynchronous materialized view by using StarRocks 2.4, and you roll back it to 2.3, you may find FE fails to start. [#14400](https://github.com/StarRocks/starrocks/pull/14400)
- When the Primary Key table uses delete_range, and the performance is not good, it may slow down data reading from RocksDB and cause high CPU usage. [#15130](https://github.com/StarRocks/starrocks/pull/15130)

## 2.3.5

Release date: November 30, 2022

### Improvements

- Colocate Join supports Equi Join. [#13546](https://github.com/StarRocks/starrocks/pull/13546)
- Fix the problem that primary key index files are too large due to continuously appending WAL records when data is frequently loaded. [#12862](https://github.com/StarRocks/starrocks/pull/12862)
- FE scans all tablets in batches so that FE releases db.readLock at scanning intervals in case of holding db.readLock for too long. [#13070](https://github.com/StarRocks/starrocks/pull/13070)

### Bug Fixes

The following bugs are fixed:

- When a view is created based directly on the result of UNION ALL, and the UNION ALL operator's input columns include NULL values, the schema of the view is incorrect since the data type of columns is NULL_TYPE rather than UNION ALL's input columns. [#13917](https://github.com/StarRocks/starrocks/pull/13917)
- The query result of `SELECT * FROM ...` and `SELECT * FROM ... LIMIT ...`  is inconsistent. [#13585](https://github.com/StarRocks/starrocks/pull/13585)
- External tablet metadata synchronized to FE may overwrite local tablet metadata, which causes data loading from Flink to fail. [#12579](https://github.com/StarRocks/starrocks/pull/12579)
- BE nodes crash when null filter in Runtime Filter handles literal constants. [#13526](https://github.com/StarRocks/starrocks/pull/13526)
- An error is returned when you execute CTAS. [#12388](https://github.com/StarRocks/starrocks/pull/12388)
- The metrics `ScanRows` collected by pipeline engine in audit log may be wrong. [#12185](https://github.com/StarRocks/starrocks/pull/12185)
- The query result is incorrect when you query compressed  HIVE data. [#11546](https://github.com/StarRocks/starrocks/pull/11546)
- Queries are timeout and StarRocks responds slowly after a BE node crashes. [#12955](https://github.com/StarRocks/starrocks/pull/12955)
- The error of Kerberos authentication failure occurs when you use Broker Load to load data. [#13355](https://github.com/StarRocks/starrocks/pull/13355)
- Too many OR predicates cause statistics estimation to take too long. [#13086](https://github.com/StarRocks/starrocks/pull/13086)
- BE node crashes if Broker Load loads ORC files (Snappy compression) contain uppercase column names. [#12724](https://github.com/StarRocks/starrocks/pull/12724)
- An error is returned when unloading or querying Primary Key table takes more than 30 minutes. [#13403](https://github.com/StarRocks/starrocks/pull/13403)
- The backup task fails when you back up large data volumes to HDFS by using a broker. [#12836](https://github.com/StarRocks/starrocks/pull/12836)
- The data StarRocks read from Iceberg may be incorrect, which is caused by the `parquet_late_materialization_enable` parameter. [#13132](https://github.com/StarRocks/starrocks/pull/13132)
- An error `failed to init view stmt`  is returned when a view is created. [#13102](https://github.com/StarRocks/starrocks/pull/13102)
- An error is returned when you use JDBC to connect StarRock and execute SQL statements. [#13526](https://github.com/StarRocks/starrocks/pull/13526)
- The query is timeout because the query involves too many buckets and uses tablet hint. [#13272](https://github.com/StarRocks/starrocks/pull/13272)
- A BE node crashes and cannot be restarted, and in the meantime, the loading job  into a newly built table reports an error. [#13701](https://github.com/StarRocks/starrocks/pull/13701)
- All BE nodes crash when a materialized view is created. [#13184](https://github.com/StarRocks/starrocks/pull/13184)
- When you execute ALTER ROUTINE LOAD to update the offset of consumed partitions, an error `The specified partition 1 is not in the consumed partitions`may be returned, and followers eventually crash. [#12227](https://github.com/StarRocks/starrocks/pull/12227)

## 2.3.4

Release date: November 10, 2022

### Improvements

- The error message provides a solution when StarRocks fails to create a Routine Load job because the number of running Routine Load job exceeds the limit. [#12204]( https://github.com/StarRocks/starrocks/pull/12204)
- The query fails when StarRocks queries data from Hive and fails to parse CSV files. [#13013](https://github.com/StarRocks/starrocks/pull/13013)

### Bug Fixes

The following bugs are fixed:

- The query may fail if HDFS files paths contain `()`. [#12660](https://github.com/StarRocks/starrocks/pull/12660)
- The result of ORDER BY ... LIMIT ... OFFSET is incorrect when the subquery contains LIMIT. [#9698](https://github.com/StarRocks/starrocks/issues/9698)
- StarRocks is case-insensitive when querying ORC files. [#12724](https://github.com/StarRocks/starrocks/pull/12724)
- BE may crash when RuntimeFilter is closed without invoking the prepare method. [#12906](https://github.com/StarRocks/starrocks/issues/12906)
- BE may crash because of memory leak. [#12906](https://github.com/StarRocks/starrocks/issues/12906)
- The query result may be incorrect after you add a new column and immediately delete data. [#12907](https://github.com/StarRocks/starrocks/pull/12907)
- BE may crash because of sorting data. [#11185](https://github.com/StarRocks/starrocks/pull/11185)
- If StarRocks and MySQL client are not on the same LAN, the loading job created by using INSERT INTO SELECT can not be terminated successfully by executing KILL only once. [#11879](https://github.com/StarRocks/starrocks/pull/11897)
- The metrics `ScanRows` collected by pipeline engine in audit log may be wrong. [#12185](https://github.com/StarRocks/starrocks/pull/12185)

## 2.3.3

Release date: September 27, 2022

### Bug Fixes

The following bugs are fixed:

- Query result may be inaccurate when you query an Hive external table stored as a text file. [#11546](https://github.com/StarRocks/starrocks/pull/11546)
- Nested arrays are not supported when you query Parquet files. [#10983](https://github.com/StarRocks/starrocks/pull/10983)
- Queries or a query may time out if concurrent queries that read data from StarRocks and external data sources are routed to the same resource group, or a query reads data from StarRocks and external data sources. [#10983](https://github.com/StarRocks/starrocks/pull/10983)
- When the Pipeline execution engine is enabled by default, the parameter parallel_fragment_exec_instance_num is changed to 1. It will cause data loading by using INSERT INTO to be slow. [#11462](https://github.com/StarRocks/starrocks/pull/11462)
- BE may crash if there are mistakes when a expression is initialized. [#11396](https://github.com/StarRocks/starrocks/pull/11396)
- The error heap-buffer-overflow may occur if you execute ORDER BY LIMIT.  [#11185](https://github.com/StarRocks/starrocks/pull/11185)
- Schema change fails if you restart Leader FE in the meantime. [#11561](https://github.com/StarRocks/starrocks/pull/11561)

## 2.3.2

Release date: September 7, 2022

### New Features

- Late materialization is supported to accelerate range filter-based queries on external tables in Parquet format. [#9738](https://github.com/StarRocks/starrocks/pull/9738)
- The SHOW AUTHENTICATION statement is added to display user authentication-related information. [#9996](https://github.com/StarRocks/starrocks/pull/9996)

### Improvements

- A configuration item is provided to control whether StarRocks recursively traverses all data files for the bucketed Hive table from which StarRocks queries data. [#10239](https://github.com/StarRocks/starrocks/pull/10239)
- The resource group type `realtime` is renamed as `short_query`. [#10247](https://github.com/StarRocks/starrocks/pull/10247)
- StarRocks no longer distinguishes between uppercase letters and lowercase letters in Hive external tables by default. [#10187](https://github.com/StarRocks/starrocks/pull/10187)

### Bug Fixes

The following bugs are fixed:

- Queries on an Elasticsearch external table may unexpectedly exit when the table is divided into multiple shards. [#10369](https://github.com/StarRocks/starrocks/pull/10369)
- StarRocks throws errors when sub-queries are rewritten as common table expressions (CTEs). [#10397](https://github.com/StarRocks/starrocks/pull/10397)
- StarRocks throws errors when a large amount of data is loaded. [#10370](https://github.com/StarRocks/starrocks/issues/10370) [#10380](https://github.com/StarRocks/starrocks/issues/10380)
- When the same Thrift service IP address is configured for multiple catalogs, deleting one catalog invalidates the incremental metadata updates in the other catalogs. [#10511](https://github.com/StarRocks/starrocks/pull/10511)
- The statistics of memory consumption from BEs are inaccurate. [#9837](https://github.com/StarRocks/starrocks/pull/9837)
- StarRocks throws errors for queries on Primary Key tables. [#10811](https://github.com/StarRocks/starrocks/pull/10811)
- Queries on logical views are not allowed even when you have SELECT permissions on these views. [#10563](https://github.com/StarRocks/starrocks/pull/10563)
- StarRocks does not impose limits on the naming of logical views. Now logical views need to follow the same naming conventions as tables. [#10558](https://github.com/StarRocks/starrocks/pull/10558)

### Behavior Change

- Add BE configuration `max_length_for_bitmap_function` with a default value 1000000 for bitmap function, and add `max_length_for_to_base64` with a default value 200000 for base64 to prevent crash. [#10851](https://github.com/StarRocks/starrocks/pull/10851)

## 2.3.1

Release date: August 22, 2022

### Improvements

- Broker Load supports transforming the List type in Parquet files into non-nested ARRAY data type. [#9150](https://github.com/StarRocks/starrocks/pull/9150)
- Optimized the performance of JSON-related functions (json_query, get_json_string, and get_json_int). [#9623](https://github.com/StarRocks/starrocks/pull/9623)
- Optimized the error message: During a query on Hive, Iceberg, or Hudi, if the data type of the column to query is not supported by StarRocks, the system throws an exception on the column. [#10139](https://github.com/StarRocks/starrocks/pull/10139)
- Reduced the scheduling latency of resource groups to optimize resource isolation performance. [#10122](https://github.com/StarRocks/starrocks/pull/10122)

### Bug Fixes

The following bugs are fixed:

- Wrong result is returned from the query on Elasticsearch external tables due to incorrect pushdown of the `limit` operator. [#9952](https://github.com/StarRocks/starrocks/pull/9952)
- Query on Oracle external tables fails when the `limit` operator is used. [#9542](https://github.com/StarRocks/starrocks/pull/9542)
- BE is blocked when all Kafka Brokers are stopped during a Routine Load. [#9935](https://github.com/StarRocks/starrocks/pull/9935)
- BE crashes during a query on a Parquet file whose data type mismatches that of the corresponding external table. [#10107](https://github.com/StarRocks/starrocks/pull/10107)
- Query times out because the scan range of external tables is empty. [#10091](https://github.com/StarRocks/starrocks/pull/10091)
- The system throws an exception when the ORDER BY clause is included in a sub-query. [#10180](https://github.com/StarRocks/starrocks/pull/10180)
- Hive Metastore hangs when Hive metadata is reloaded asynchronously. [#10132](https://github.com/StarRocks/starrocks/pull/10132)

## 2.3.0

Release date: July 29, 2022

### New Features

- The Primary Key table supports complete DELETE WHERE syntax. For more information, see [DELETE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/DELETE#delete-data-by-primary-key).
- The Primary Key table supports persistent primary key indexes. You can choose to persist the primary key index on disk rather than in memory, significantly reducing memory usage. For more information, see [Primary Key table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/).
- Global dictionary can be updated during real-time data ingestion，optimizing query performance and delivering 2X query performance for string data.
- The CREATE TABLE AS SELECT statement can be executed asynchronously. For more information, see [CREATE TABLE AS SELECT](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT/).
- Support the following resource group-related features:
  - Monitor resource groups: You can view the resource group of the query in the audit log and obtain the metrics of the resource group by calling APIs. For more information, see [Monitor and Alerting](https://docs.starrocks.io/docs/administration/Monitor_and_Alert#monitor-and-alerting).
  - Limit the consumption of large queries on CPU, memory, and I/O resources: You can route queries to specific resource groups based on the classifiers or by configuring session variables. For more information, see [Resource group](https://docs.starrocks.io/docs/administration/resource_group/).
- JDBC external tables can be used to conveniently query data in Oracle, PostgreSQL, MySQL, SQLServer, ClickHouse, and other databases. StarRocks also supports predicate pushdown, improving query performance. For more information, see [External table for a JDBC-compatible database](https://docs.starrocks.io/docs/data_source/External_table#external-table-for-a-JDBC-compatible-database).
- [Preview] A new Data Source Connector framework is released to support external catalogs. You can use external catalogs to directly access and query Hive data without creating external tables. For more information, see [Use catalogs to manage internal and external data](https://docs.starrocks.io/docs/data_source/catalog/query_external_data/).
- Added the following functions:
  - [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/)
  - [ntile](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/)
  - [bitmap_union_count](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_union_count/), [base64_to_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/base64_to_bitmap/), [array_to_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_to_bitmap/)
  - [week](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/week/), [time_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/)

### Improvements

- The compaction mechanism can merge large volume of metadata more quickly. This prevents metadata squeezing and excessive disk usage that can occur shortly after frequent data updates.
- Optimized the performance of loading Parquet files and compressed files.
- Optimized the mechanism of creating materialized views. After the optimization, materialized views can be created at a speed up to 10 times faster than before.
- Optimized the performance of the following operators:
  - TopN and sort operators
  - Equivalence comparison operators that contain functions can use Zone Map indexes when these operators are pushed down to scan operators.
- Optimized Apache Hive™ external tables.
  - When Apache Hive™ tables are stored in Parquet, ORC, or CSV format, schema changes caused by ADD COLUMN or REPLACE COLUMN on Hive can be synchronized to StarRocks when you execute the REFRESH statement on the corresponding Hive external table. For more information, see [Hive external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#hive-external-table).
  - `hive.metastore.uris` can be modified for Hive resources. For more information, see [ALTER RESOURCE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_RESOURCE/).
- Optimized the performance of Apache Iceberg external tables. A custom catalog can be used to create an Iceberg resource. For more information, see [Apache Iceberg external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#deprecated-iceberg-external-table).
- Optimized the performance of Elasticsearch external tables. Sniffing the addresses of the data nodes in an Elasticsearch cluster can be disabled. For more information, see [Elasticsearch external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#elasticsearch-external-table).
- When the sum() function accepts a numeric string, it implicitly converts the numeric string.
- The year(), month(), and day() functions support the DATE data type.

### Bug Fixes

Fixed the following bugs:

- CPU utilization surges due to an excessive number of tablets.
- Issues that cause "fail to prepare tablet reader" to occur.
- The FEs fail to restart.[#5642](https://github.com/StarRocks/starrocks/issues/5642 )  [#4969](https://github.com/StarRocks/starrocks/issues/4969 )  [#5580](https://github.com/StarRocks/starrocks/issues/5580)
- The CTAS statement cannot be run successfully when the statement includes a JSON function. [#6498](https://github.com/StarRocks/starrocks/issues/6498)

### Others

- StarGo, a cluster management tool, can deploy, start, upgrade, and roll back clusters and manage multiple clusters. For more information, see [Deploy StarRocks with StarGo](https://docs.starrocks.io/docs/administration/stargo/).
- The pipeline engine is enabled by default when you upgrade StarRocks to version 2.3 or deploy StarRocks. The pipeline engine can improve the performance of simple queries in high concurrency scenarios and complex queries. If you detect significant performance regressions when using StarRocks 2.3, you can disable the pipeline engine by executing the `SET GLOBAL` statement to set `enable_pipeline_engine` to `false`.
- The [SHOW GRANTS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_GRANTS/) statement is compatible with the MySQL syntax and displays the privileges assigned to a user in the form of GRANT statements.
- It is recommended that the memory_limitation_per_thread_for_schema_change ( BE configuration item)  use the default value 2 GB, and data is written to disk when data volume exceeds this limit. Therefore, if you have previously set this parameter to a larger value, it is recommended that you set it to 2 GB, otherwise a schema change task may take up a large amount of memory.

### Upgrade notes

To roll back to the previous version that was used before the upgrade, add the `ignore_unknown_log_id` parameter to the **fe.conf** file of each FE and set the parameter to `true`. The parameter is required because new types of logs are added in StarRocks v2.2.0. If you do not add the parameter, you cannot roll back to the previous version. We recommend that you set the `ignore_unknown_log_id` parameter to `false` in the **fe.conf** file of each FE after checkpoints are created. Then, restart the FEs to restore the FEs to the previous configurations.
