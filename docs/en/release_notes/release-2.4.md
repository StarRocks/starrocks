---
displayed_sidebar: "English"
---

# StarRocks version 2.4

## 2.4.5

Release date: April 21, 2023

### Improvements

- Forbade the List Partition syntax because it may cause errors in upgrading metadata. [#15401](https://github.com/StarRocks/starrocks/pull/15401)
- Supports BITMAP, HLL, and PERCENTILE types for materialized views. [#15731](https://github.com/StarRocks/starrocks/pull/15731)
- Optimized the inference of `storage_medium`. When BEs use both SSD and HDD as storage devices, if the property `storage_cooldown_time` is specified, StarRocks sets `storage_medium` to `SSD`. Otherwise, StarRocks sets `storage_medium` to `HDD`. [#18649](https://github.com/StarRocks/starrocks/pull/18649)
- Optimized the accuracy of thread dump. [#16748](https://github.com/StarRocks/starrocks/pull/16748)
- Optimized load efficiency by triggering metadata compaction before loading. [#19347](https://github.com/StarRocks/starrocks/pull/19347)
- Optimized the Stream Load planner timeout. [#18992](https://github.com/StarRocks/starrocks/pull/18992/files)
- Optimized the Unique key table performance by forbidding the collection of statistics from value columns. [#19563](https://github.com/StarRocks/starrocks/pull/19563)

### Bug Fixes

The following bugs are fixed:

- NPE is returned when an unsupported data type is used in CREATE TABLE. [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- Wrong results are returned to queries using Broadcast Join with the short-circuit. [#20952](https://github.com/StarRocks/starrocks/issues/20952)
- The disk occupation problem caused by the wrong data truncate logic. [#20590](https://github.com/StarRocks/starrocks/pull/20590)
- The AuditLoader plugin can neither be installed nor deleted. [#20468](https://github.com/StarRocks/starrocks/issues/20468)
- If an exception is thrown when a tablet is being scheduled, other tablets in the same batch will never be scheduled. [#20681](https://github.com/StarRocks/starrocks/pull/20681)
- Unknown Error is returned when an unsupported SQL function is used in the creation of a synchronous materialized view. [#20348](https://github.com/StarRocks/starrocks/issues/20348)
- Multiple COUNT DISTINCT calculations are incorrectly rewritten. [#19714](https://github.com/StarRocks/starrocks/pull/19714)
- Wrong results are returned to queries on the tablet that is in compaction. [#20084](https://github.com/StarRocks/starrocks/issues/20084)
- Wrong results are returned to queries with aggregation. [#19725](https://github.com/StarRocks/starrocks/issues/19725)
- No error message is returned when loading NULL parquet data into NOT NULL columns. [#19885](https://github.com/StarRocks/starrocks/pull/19885)
- The query concurrency metric decreases slowly when the concurrency limit of a resource group is continuously reached. [#19363](https://github.com/StarRocks/starrocks/pull/19363)
- FE fails to start when replaying the `InsertOverwriteJob` state change log. [#19061](https://github.com/StarRocks/starrocks/issues/19061)
- The Primary Key table deadlock. [#18488](https://github.com/StarRocks/starrocks/pull/18488)
- For Colocation tables, the replica status can be manually specified as `bad` by using statements like `ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`. If the number of BEs is less than or equal to the number of replicas, the corrupted replica cannot be repaired. [#17876](https://github.com/StarRocks/starrocks/issues/17876)
- Issues caused by ARRAY-related functions. [#18556](https://github.com/StarRocks/starrocks/pull/18556)

## 2.4.4

Release date: February 22, 2023

### Improvements

- Supports fast cancellation of load. [#15514](https://github.com/StarRocks/starrocks/pull/15514) [#15398](https://github.com/StarRocks/starrocks/pull/15398) [#15969](https://github.com/StarRocks/starrocks/pull/15969)
- Optimized the CPU usage of the compaction framework. [#11747](https://github.com/StarRocks/starrocks/pull/11747)
- Supports cumulative compaction on tablets with missing versions. [#17030](https://github.com/StarRocks/starrocks/pull/17030)

### Bug Fixes

The following bugs are fixed:

- Excessive dynamic partitions are created when an invalid DATE value is specified. [#17966](https://github.com/StarRocks/starrocks/pull/17966)
- Failure to connect to Elasticsearch external tables with the default HTTPS port. [#13726](https://github.com/StarRocks/starrocks/pull/13726)
- BE failed to cancel a Stream Load transaction after the transaction times out. [#15738](https://github.com/StarRocks/starrocks/pull/15738)
- Wrong query results are returned from a local shuffle aggregation on a single BE. [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- Queries may fail with the error message "wait_for_version version: failed: apply stopped". [#17848](https://github.com/StarRocks/starrocks/pull/17850)
- Wrong query results are returned because OLAP scan bucket expressions are not correctly cleared. [#17666](https://github.com/StarRocks/starrocks/pull/17666)
- The bucket number of dynamic partitioned tables in a Colocate Group cannot be modified and an error message is returned. [#17418](https://github.com/StarRocks/starrocks/pull/17418)
- When you connect to a non-Leader FE node and send the SQL statement `USE <catalog_name>.<database_name>`, the non-Leader FE node forwards the SQL statement, with `<catalog_name>` excluded, to the Leader FE node. As a result, the Leader FE node chooses to use the `default_catalog` and eventually fails to find the specified database. [#17302](https://github.com/StarRocks/starrocks/pull/17302)
- Incorrect logic for the dictmapping check before rewriting. [#17405](https://github.com/StarRocks/starrocks/pull/17405)
- If an FE sends an occasional heartbeat to a BE, and the heartbeat connection times out, the FE considers the BE unavailable, leading to transaction failures on the BE. [#16386](https://github.com/StarRocks/starrocks/pull/16386)
- `get_applied_rowsets` failed for queries on newly cloned tablets on a follower FE. [#17192](https://github.com/StarRocks/starrocks/pull/17192)
- NPE caused by executing `SET variable = default` on a follower FE. [#17549](https://github.com/StarRocks/starrocks/pull/17549)
- Expressions in projection are not rewritten by the dictionary. [#17558](https://github.com/StarRocks/starrocks/pull/17558)
- Incorrect logic for creating dynamic partitions on weekly basis. [#17163](https://github.com/StarRocks/starrocks/pull/17163)
- Wrong query results are returned from a local shuffle. [#17130](https://github.com/StarRocks/starrocks/pull/17130)
- Incremental clones may fail. [#16930](https://github.com/StarRocks/starrocks/pull/16930)
- In some cases, CBO may use incorrect logic to compare whether two operators are equivalent. [#17199](https://github.com/StarRocks/starrocks/pull/17199) [#17227](https://github.com/StarRocks/starrocks/pull/17227)
- Failed to access JuiceFS because the JuiceFS schema is not checked and resolved properly. [#16940](https://github.com/StarRocks/starrocks/pull/16940)

## 2.4.3

Release date: January 19, 2023

### Improvements

- StarRocks checks whether the corresponding database and table exist during an Analyze to prevent NPE. [#14467](https://github.com/StarRocks/starrocks/pull/14467)
- Columns with data types that are not supported are not materialized for queries on external tables. [#13305](https://github.com/StarRocks/starrocks/pull/13305)
- Adds Java version check for the FE start script **start_fe.sh**. [#14333](https://github.com/StarRocks/starrocks/pull/14333)

### Bug Fixes

The following bugs are fixed:

- Stream Load may fail when timeout is not set. [#16241](https://github.com/StarRocks/starrocks/pull/16241)
- bRPC Send crashes when memory usage is high. [#16046](https://github.com/StarRocks/starrocks/issues/16046)
- StarRocks fails to load data in external tables from a StarRocks instance of an early version.  [#16130](https://github.com/StarRocks/starrocks/pull/16130)
- Materialized view Refresh failure may cause memory leak. [#16041](https://github.com/StarRocks/starrocks/pull/16041)
- Schema Change hangs at the Publish stage. [#14148](https://github.com/StarRocks/starrocks/issues/14148)
- Memory leak caused by a materialized view QeProcessorImpl issue. [#15699](https://github.com/StarRocks/starrocks/pull/15699)
- The results of queries with `limit`  are inconsistent. [#13574](https://github.com/StarRocks/starrocks/pull/13574)
- Memory leak caused by INSERT. [#14718](https://github.com/StarRocks/starrocks/pull/14718)
- Primary Key tables executes Tablet Migration.[#13720](https://github.com/StarRocks/starrocks/pull/13720)
- Broker Kerberos tickets timeout during Broker Load. [#16149](https://github.com/StarRocks/starrocks/pull/16149)
- The `nullable` information is inferred incorrectly in the view of a table. [#15744](https://github.com/StarRocks/starrocks/pull/15744)

### Behavior Change

- Modified the default backlog of Thrift Listen to `1024`. [#13911](https://github.com/StarRocks/starrocks/pull/13911)
- Added SQL mode `FORBID_INVALID_DATES`. This SQL mode is disabled by default. When it is enabled, StarRocks verifies the input of the DATE type, and returns an error when the input is invalid. [#14143](https://github.com/StarRocks/starrocks/pull/14143)

## 2.4.2

Release date: December 14, 2022

### Improvement

- Optimized the performance of Bucket Hint when a multitude of buckets exist. [#13142](https://github.com/StarRocks/starrocks/pull/13142)

### Bug Fixes

The following bugs are fixed:

- Flushing the Primary Key index may cause BE to crash. [#14857](https://github.com/StarRocks/starrocks/pull/14857) [#14819](https://github.com/StarRocks/starrocks/pull/14819)
- Materialized view types cannot be correctly identified by `SHOW FULL TABLES`. [#13954](https://github.com/StarRocks/starrocks/pull/13954)
- Upgrading StarRocks v2.2 to v2.4 may cause BE to crash. [#13795](https://github.com/StarRocks/starrocks/pull/13795)
- Broker Load may cause BE to crash. [#13973](https://github.com/StarRocks/starrocks/pull/13973)
- The session variable `statistic_collect_parallel` does not take effect. [#14352](https://github.com/StarRocks/starrocks/pull/14352)
- INSERT INTO may cause BE to crash. [#14818](https://github.com/StarRocks/starrocks/pull/14818)
- JAVA UDF may cause BE to crash. [#13947](https://github.com/StarRocks/starrocks/pull/13947)
- Cloning replicas during partial updates may cause BE to crash and fail to restart. [#13683](https://github.com/StarRocks/starrocks/pull/13683)
- Colocated Join may not take effect. [#13561](https://github.com/StarRocks/starrocks/pull/13561)

### Behavior Change

- Constrained the session variable `query_timeout` with an upper limit of `259200` and a lower limit of `1`.
- Deprecated the FE parameter `default_storage_medium`. The storage medium of a table is automatically inferred by the system. [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.4.1

Release date: November 14, 2022

### New Features

- Supports non-equi joins - LEFT SEMI JOIN and ANTI JOIN. Optimized the JOIN function. [#13019](https://github.com/StarRocks/starrocks/pull/13019)

### Improvements

- Supports property `aliveStatus` in `HeartbeatResponse`. `aliveStatus` indicates if a node is alive in the cluster. Mechanisms that judge the `aliveStatus` are further optimized. [#12713](https://github.com/StarRocks/starrocks/pull/12713)

- Optimized the error message of Routine Load. [#12155](https://github.com/StarRocks/starrocks/pull/12155)

### Bug Fixes

- BE crashes after being upgraded from v2.4.0RC to v2.4.0. [#13128](https://github.com/StarRocks/starrocks/pull/13128)

- Late materialization causes incorrect results to queries on data lakes. [#13133](https://github.com/StarRocks/starrocks/pull/13133)

- The get_json_int function throws exceptions. [#12997](https://github.com/StarRocks/starrocks/pull/12997)

- Data may be inconsistent after deletion from a PRIMARY KEY table with a persistent index.[#12719](https://github.com/StarRocks/starrocks/pull/12719)

- BE may crash during compaction on a PRIMARY KEY table. [#12914](https://github.com/StarRocks/starrocks/pull/12914)

- The json_object function returns incorrect results when its input contains an empty string. [#13030](https://github.com/StarRocks/starrocks/issues/13030)

- BE crashes due to `RuntimeFilter`. [#12807](https://github.com/StarRocks/starrocks/pull/12807)

- FE hangs due to excessive recursive computations in CBO. [#12788](https://github.com/StarRocks/starrocks/pull/12788)

- BE may crash or report an error when exiting gracefully. [#12852](https://github.com/StarRocks/starrocks/pull/12852)

- Compaction crashes after data is deleted from a table with new columns added to it. [#12907](https://github.com/StarRocks/starrocks/pull/12907)

- Data may be inconsistent due to incorrect mechanisms in OLAP external table metadata synchronization. [#12368](https://github.com/StarRocks/starrocks/pull/12368)

- When one BE crashes, the other BEs may execute relevant queries till timeout. [#12954](https://github.com/StarRocks/starrocks/pull/12954)

### Behavior Change

- When parsing Hive external table fails, StarRocks throws error messages instead of converting relevant columns into NULL columns. [#12382](https://github.com/StarRocks/starrocks/pull/12382)

## 2.4.0

Release date: October 20, 2022

### New Features

- Supports creating asynchronous materialized views based on multiple base tables to accelerate queries with JOIN operations. Asynchronous materialized views support all [table types](https://docs.starrocks.io/docs/table_design/table_types/table_types/). For more information, see [Materialized View](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/).

- Supports overwriting data via INSERT OVERWRITE. For more information, see [Load data using INSERT](https://docs.starrocks.io/docs/loading/InsertInto/).

- [Preview] Provides stateless Compute Nodes (CN) that can be horizontally scaled. You can use StarRocks Operator to deploy CN into your Kubernetes (K8s) cluster to achieve automatic horizontal scaling. For more information, see [Deploy and manage CN on Kubernetes with StarRocks Operator](https://docs.starrocks.io/docs/deployment/sr_operator/).

- Outer Join supports non-equi joins in which join items are related by comparison operators including `<`, `<=`, `>`, `>=`, and `<>`. For more information, see [SELECT](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SELECT/).

- Supports creating Iceberg catalogs and Hudi catalogs, which allow direct queries on data from Apache Iceberg and Apache Hudi. For more information, see [Iceberg catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/) and [Hudi catalog](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/).

- Supports querying ARRAY-type columns from Apache Hive™ tables in CSV format. For more information, see [External table](https://docs.starrocks.io/docs/data_source/External_table/).

- Supports viewing the schema of external data via DESC. For more information, see [DESC](https://docs.starrocks.io/docs/sql-reference/sql-statements/Utility/DESCRIBE/).

- Supports granting a specific role or IMPERSONATE permission to a user via GRANT and revoking them via REVOKE, and supports executing an SQL statement with IMPERSONATE permission via  EXECUTE AS. For more information, see [GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/), [REVOKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/REVOKE/), and [EXECUTE AS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/EXECUTE_AS/).

- Supports FDQN access: now you can use domain name or the combination of hostname and port as the unique identification of a BE or an FE node. This prevents access failures caused by changing IP addresses. For more information, see [Enable FQDN Access](https://docs.starrocks.io/docs/administration/enable_fqdn/).

- flink-connector-starrocks supports Primary Key table partial update. For more information, see [Load data by using flink-connector-starrocks](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/).

- Provides the following new functions:

  - array_contains_all: checks whether a specific array is a subset of another. For more information, see [array_contains_all](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_contains_all/).
  - percentile_cont: calculates the percentile value with linear interpolation. For more information, see [percentile_cont](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_cont/).

### Improvements

- The Primary Key table supports flushing VARCHAR-type primary key indexes to disks. From version 2.4.0, the Primary Key table supports the same data types for primary key indexes regardless of whether persistent primary key index is turned on or not.

- Optimized the query performance on external tables.

  - Supports late materialization during queries on external tables in Parquet format to optimize the query performance on data lakes with small-scale filtering involved.
  - Small I/O operations can be merged to reduce the delay for querying data lakes, thereby improving the query performance on external tables.

- Optimized the performance of window functions.

- Optimized the performance of Cross Join by supporting predicate pushdown.

- Histograms are added to CBO statistics. Full statistics collection is further optimized. For more information, see [Gather CBO statistics](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/).

- Adaptive multi-threading is enabled for tablet scanning to reduce the dependency of scanning performance on the tablet number. As a result, you can set the number of buckets more easily. For more information, see [set the number of buckets](https://docs.starrocks.io/docs/2.4/table_design/Data_distribution/#determine-the-number-of-tablets).

- Supports querying compressed TXT files in Apache Hive.

- Adjusted the mechanisms of default PageCache size calculation and memory consistency check to avoid OOM issues during multi-instance deployments.

- Improved the performance of large-size batch load on Primary Key tables up to two times by removing final_merge operations.

- Supports a Stream Load transaction interface to implement two-phase commit (2PC) for transactions that are run to load data from external systems such as Apache Flink® and Apache Kafka®, improving the performance of highly concurrent stream loads.

- Functions:

  - You can use multiple COUNT(DISTINCT) in one statement. For more information, see [count](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/count/).
  - Window functions min() and max() support sliding windows. For more information, see [Window functions](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/).
  - Optimized the performance of the window_funnel function. For more information, see [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/).

### Bug Fixes

The following bugs are fixed:

- DECIMAL data types returned by DESC are different from those specified in the CREATE TABLE statement. [#7309](https://github.com/StarRocks/starrocks/pull/7309)

- FE metadata management issues that affect the stability of FEs. [#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- Data load-related issues:

  - Broke Load fails when ARRAY columns are specified. [#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - Replicas are inconsistent after data is loaded to a non-Duplicate Key table via Broker Load. [#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - Executing ALTER ROUTINE LOAD raises NPE. [#7804](https://github.com/StarRocks/starrocks/pull/7804)

- Data Lake analytics-related issues:

  - Queries on Parquet data in Hive external tables fail. [#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Incorrect results are returned for queries with `limit` clause on Elasticsearch external table. [#9226](https://github.com/StarRocks/starrocks/pull/9226)
  - An unknown error is raised during queries on an Apache Iceberg table with a complex data type. [#11298](https://github.com/StarRocks/starrocks/pull/11298)

- Metadata is inconsistent between the Leader FE and Follower FE nodes. [#11215](https://github.com/StarRocks/starrocks/pull/11215)

- BE crashes when the size of BITMAP data exceeds 2 GB. [#11178](https://github.com/StarRocks/starrocks/pull/11178)

### Behavior Change

- Page Cache is enabled by default ("disable_storage_page_cache" = "false"). The default cache size (`storage_page_cache_limit`) is 20% of the system memory.
- CBO is enabled by default. Deprecated the session variable `enable_cbo`.
- Vectorized engine is enabled by default. Deprecated the session variable `vectorized_engine_enable`.

### Others

- Announcing stable release of Resource Group.
- Announcing stable release of JSON data type and its related functions.
