# StarRocks version 2.0

## 2.0.9

Release date: August 6, 2022

### Bug Fixes

The following bugs are fixed:

- For a Broker Load job, if the broker is heavily loaded, internal heartbeats may time out, causing data loss. [#8282](https://github.com/StarRocks/starrocks/issues/8282)
- For a Broker Load job, if the destination StarRocks table does not have the column specified by the `COLUMNS FROM PATH AS` parameter, the BEs stop running. [#5346](https://github.com/StarRocks/starrocks/issues/5346)
- Some queries are forwarded to the Leader FE, causing the `/api/query_detail` action to return incorrect execution information about SQL statements such as SHOW FRONTENDS. [#9185](https://github.com/StarRocks/starrocks/issues/9185)
- When multiple Broker Load jobs are created to load the same HDFS data file, if one job encounters exceptions, the other jobs may not be able to properly read data either and consequently fail. [#9506](https://github.com/StarRocks/starrocks/issues/9506)

## 2.0.8

Release date: July 15, 2022

### Bug Fixes

The following bugs are fixed:

- Switching Leader FE node repetitively may cause all load jobs hang and fail. [#7350](https://github.com/StarRocks/starrocks/issues/7350)
- BE crashes when the memory usage estimation of MemTable exceeds 4GB, because, during a data skew in load, some fields may occupy a large amount of memory resources. [#7161](https://github.com/StarRocks/starrocks/issues/7161)
- After restarting FEs, the schemas of materialized views changed due to incorrect parsing of uppercase and lowercase letters. [#7362](https://github.com/StarRocks/starrocks/issues/7362)
- When you load JSON data from Kafka into StarRocks by using Routine Load, if there are blank rows in the JSON data, the data after the blank rows will be lost. [#8534](https://github.com/StarRocks/starrocks/issues/8534)

## 2.0.7

Release date: June 13, 2022

### Bug Fixes

The following bugs are fixed:

- If the number of duplicate values in a column of a table that is being compacted exceeds 0x40000000, the compaction is suspended. [#6513](https://github.com/StarRocks/starrocks/issues/6513)
- After an FE restarts, it encounters high I/O and abnormally increasing disk usage due to a few issues in BDB JE v7.3.8 and shows no sign of restoring to normal. The FE is restored to normal after it rolls back to BDB JE v7.3.7. [#6634](https://github.com/StarRocks/starrocks/issues/6634)

## 2.0.6

Release date: May 25, 2022

### Bug Fixes

The following bugs are fixed:

- Some graphical user interface (GUI) tools automatically configure the `set_sql_limit` variable. As a result, the SQL statement ORDER BY LIMIT is ignored, and consequently an incorrect number of rows are returned for queries. [#5966](https://github.com/StarRocks/starrocks/issues/5966)
- If a colocation group (CG) contains a large number of tables and data is frequently loaded into the tables, the CG may not be able to stay in the `stable` state. In this case, the JOIN statement does not support Colocate Join operations. StarRocks has been optimized to wait for a little longer during data loading. This way, the integrity of the tablet replicas to which data is loaded can be maximized.
- If a few replicas fail to be loaded due to reasons such as heavy loads or high network latencies, cloning on these replicas is triggered. In this case, deadlocks may occur, which may cause a situation in which the loads on processes are low but a large number of requests time out. [#5646](https://github.com/StarRocks/starrocks/issues/5646) [#6290](https://github.com/StarRocks/starrocks/issues/6290)
- After the schema of a table that uses the Primary Key table is changed, a "duplicate key xxx" error may occur when data is loaded into that table. [#5878](https://github.com/StarRocks/starrocks/issues/5878)
- If the DROP SCHEMA statement is executed on a database, the database is forcibly deleted and cannot be restored. [#6201](https://github.com/StarRocks/starrocks/issues/6201)

## 2.0.5

Release date: May 13, 2022

Upgrade recommendation: Some critical bugs related to the correctness of stored data or data queries have been fixed in this version. We recommend that you upgrade your StarRocks cluster at your earliest opportunity.

### Bug Fixes

The following bugs are fixed:

- [Critical Bug] Data may be lost as a result of BE failures. This bug is fixed by introducing a mechanism that is used to publish a specific version to multiple BEs at a time. [#3140](https://github.com/StarRocks/starrocks/issues/3140)

- [Critical Bug] If tablets are migrated in specific data ingestion phases, data continues to be written to the original disk on which the tablets are stored. As a result, data is lost, and queries cannot be run properly. [#5160](https://github.com/StarRocks/starrocks/issues/5160)

- [Critical Bug] When you run queries after you perform multiple DELETE operations, you may obtain incorrect query results if optimization on low-cardinality columns is performed for the queries. [#5712](https://github.com/StarRocks/starrocks/issues/5712)

- [Critical Bug] If a query contains a JOIN clause that is used to combine a column with DOUBLE values and a column with VARCHAR values, the query result may be incorrect. [#5809](https://github.com/StarRocks/starrocks/pull/5809)

- In certain circumstances, when you load data into your StarRocks cluster, some replicas of specific versions are marked as valid by the FEs before the replicas take effect. At this time, if you query data of the specific versions, StarRocks cannot find the data and reports errors. [#5153](https://github.com/StarRocks/starrocks/issues/5153)

- If a parameter in the `SPLIT` function is set to `NULL`, the BEs of your StarRocks cluster may stop running. [#4092](https://github.com/StarRocks/starrocks/issues/4092)  

- After your cluster is upgraded from Apache Doris 0.13 to StarRocks 1.19.x and keeps running for a period of time, a further upgrade to StarRocks 2.0.1 may fail. [#5309](https://github.com/StarRocks/starrocks/issues/5309)

## 2.0.4

Release date: April 18, 2022

### Bug Fixes

The following bugs are fixed:

- After deleting columns, adding new partitions, and cloning tablets,  the columns' unique ids in old and new tablets may not be the same, which may cause BE to stop working because the system uses a shared tablet schema. [#4514](https://github.com/StarRocks/starrocks/issues/4514)
- When data is loading to a StarRocks external table, if the configured FE of the target StarRocks cluster is not a Leader, it will cause the FE to stop working. [#4573](https://github.com/StarRocks/starrocks/issues/4573)
- Query results may be incorrect, when a Duplicate Key table performs schema change and creates materialized view at the same time. [#4839](https://github.com/StarRocks/starrocks/issues/4839)
- The problem of possible data loss due to BE failure (solved by using Batch publish version). [#3140](https://github.com/StarRocks/starrocks/issues/3140)

## 2.0.3

Release date: March 14, 2022

### Bug Fixes

The following bugs are fixed:

- Query fails when BE nodes are in suspended animation.
- Query fails when there is no appropriate execution plan for single-tablet table joins.  [#3854](https://github.com/StarRocks/starrocks/issues/3854)
- A deadlock problem may occur when an FE node collects information to build a global dictionary for low-cardinality optimization. [#3839](https://github.com/StarRocks/starrocks/issues/3839)

## 2.0.2

Release date: March 2, 2022

### Improvements

- Memory usage is optimized. Users can specify the label_keep_max_num parameter to control the maximum number of loading jobs to retain within a period of time. This prevents full GC caused by high memory usage of FE during frequent data loading.

### Bug Fixes

The following bugs are fixed:

- BE nodes fail when the column decoder encounters an exception.
- Auto __op mapping does not take effect when jsonpaths is specified in the command used for loading JSON data.
- BE nodes fail because the source data changes during data loading using Broker Load.
- Some SQL statements report errors after materialized views are created.
- Query may fail if an SQL clause contains a predicate that supports global dictionary for low-cardinality optimization and a predicate that does not.

## 2.0.1

Release date: January 21, 2022

### Improvements

- Hive's implicit_cast operations can be read when StarRocks uses external tables to query Hive data. [#2829](https://github.com/StarRocks/starrocks/pull/2829)
- The read/write lock is used to fix high CPU usage when StarRocks CBO collects statistics to support high-concurrency queries. [#2901](https://github.com/StarRocks/starrocks/pull/2901)
- CBO statistics gathering and UNION operator are optimized.

### Bug Fixes

- The query error that is caused by inconsistent global dictionaries of replicas is fixed. [#2700](https://github.com/StarRocks/starrocks/pull/2700) [#2765](https://github.com/StarRocks/starrocks/pull/2765)
- The error that the parameter `exec_mem_limit` during data loading does not take effect is fixed. [#2693](https://github.com/StarRocks/starrocks/pull/2693)
  > The parameter `exec_mem_limit` specifies each BE node's memory limit during data loading.
- The OOM error that occurs when data is imported to the Primary Key table is fixed. [#2743](https://github.com/StarRocks/starrocks/pull/2743) [#2777](https://github.com/StarRocks/starrocks/pull/2777)
- The error that the BE node stops responding when StarRocks uses external tables to query large MySQL tables is fixed. [#2881](https://github.com/StarRocks/starrocks/pull/2881)

### Behavior Change

StarRocks can use external tables to access Hive and its AWS S3-based external tables. However, the jar file that is used to access S3 data is too large and the binary package of StarRocks does not contain this jar file. If you want to use this jar file, you can download it from [Hive_s3_lib](https://releases.starrocks.io/resources/hive_s3_jar.tar.gz).

## 2.0.0

Release date: January 5, 2022

### New Features

- External Table
  - [Experimental Function]Support for Hive external table on S3
  - DecimalV3 support for external table [#425](https://github.com/StarRocks/starrocks/pull/425)
- Implement complex expressions to be pushed down to the storage layer for computation, thus gaining performance gains
- Primary Key is officially released, which supports Stream Load, Broker Load, Routine Load, and also provides a second-level synchronization tool for MySQL data based on Flink-cdc

### Improvements

- Arithmetic operators optimization
  - Optimize the performance of dictionary with low cardinality [#791](https://github.com/StarRocks/starrocks/pull/791)
  - Optimize the scan performance of int for single table [#273](https://github.com/StarRocks/starrocks/issues/273)
  - Optimize the performance of `count(distinct int)` with high cardinality  [#139](https://github.com/StarRocks/starrocks/pull/139) [#250](https://github.com/StarRocks/starrocks/pull/250)  [#544](https://github.com/StarRocks/starrocks/pull/544)[#570](https://github.com/StarRocks/starrocks/pull/570)
  - Optimize `Group by int` / `limit` / `case when` / `not equa`l in implementation-level
- Memory management optimization
  - Refactor the memory statistics and control framework to accurately count memory usage and completely solve OOM
  - Optimize metadata memory usage
  - Solve the problem of large memory release stuck in execution threads for a long time
  - Add process graceful exit mechanism and support memory leak check [#1093](https://github.com/StarRocks/starrocks/pull/1093)

### Bug Fixs

- Fix the problem that the Hive external table is timeout to get metadata in a large amount.
- Fix the problem of unclear error message of materialized view creation.
- Fix the implementation of like in vectorization engine [#722](https://github.com/StarRocks/starrocks/pull/722)
- Repair the error of parsing the predicate is in `alter table`[#725](https://github.com/StarRocks/starrocks/pull/725)
- Fix the problem that the `curdate` function can not format the date
