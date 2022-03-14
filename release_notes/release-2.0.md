# StarRocks version 2.0

## 2.0.0

Release date: January 5, 2022

### New Feature

- External Table
  - [Experimental Function]Support for Hive external table on S3
  - DecimalV3 support for external table [#425](https://github.com/StarRocks/starrocks/pull/425)
- Implement complex expressions to be pushed down to the storage layer for computation, thus gaining performance gains
- Primary Key is officially released, which supports Stream Load, Broker Load, Routine Load, and also provides a second-level synchronization tool for MySQL data based on Flink-cdc

### Improvement

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

### Bugfix

- Fix the problem that the Hive external table is timeout to get metadata in a large amount.
- Fix the problem of unclear error message of materialized view creation.
- Fix the implementation of like in vectorization engine [#722](https://github.com/StarRocks/starrocks/pull/722)
- Repair the error of parsing the predicate is in `alter table`[#725](https://github.com/StarRocks/starrocks/pull/725)
- Fix the problem that the `curdate` function can not format the date

## 2.0.1

Release date: January 21, 2022

### Improvement

- Hive's implicit_cast operations can be read when StarRocks uses external tables to query Hive data. [#2829](https://github.com/StarRocks/starrocks/pull/2829)
- The read/write lock is used to fix high CPU usage when StarRocks CBO collects statistics to support high-concurrency queries. [#2901](https://github.com/StarRocks/starrocks/pull/2901)
- CBO statistics gathering and UNION operator are optimized.

### Bugfix

- The query error that is caused by inconsistent global dictionaries of replicas is fixed. [#2700](https://github.com/StarRocks/starrocks/pull/2700) [#2765](https://github.com/StarRocks/starrocks/pull/2765)
- The error that the parameter `exec_mem_limit` during data loading does not take effect is fixed. [#2693](https://github.com/StarRocks/starrocks/pull/2693)
  > The parameter `exec_mem_limit` specifies each BE node's memory limit during data loading.
- The OOM error that occurs when data is imported to the Primary Key Model is fixed. [#2743](https://github.com/StarRocks/starrocks/pull/2743) [#2777](https://github.com/StarRocks/starrocks/pull/2777)
- The error that the BE node stops responding when StarRocks uses external tables to query large MySQL tables is fixed. [#2881](https://github.com/StarRocks/starrocks/pull/2881)

### Behavior Change

StarRocks can use external tables to access Hive and its AWS S3-based external tables. However, the jar file that is used to access S3 data is too large and the binary package of StarRocks does not contain this jar file. If you want to use this jar file, you can download it from [Hive_s3_lib](https://cdn-thirdparty.starrocks.com/hive_s3_jar.tar.gz).

## 2.0.2

Release date: March 2, 2022

### Improvement

- Memory usage is optimized. Users can specify the label_keep_max_num parameter to control the maximum number of loading jobs to retain within a period of time. This prevents full GC caused by high memory usage of FE during frequent data loading.

### BugFix

The following bugs are fixed:

- BE nodes fail when the column decoder encounters an exception.
- Auto __op mapping does not take effect when jsonpaths is specified in the command used for loading JSON data.
- BE nodes fail because the source data changes during data loading using Broker Load.
- Some SQL statements report errors after materialized views are created.
- Query may fail if an SQL clause contains a predicate that supports global dictionary for low-cardinality optimization and a predicate that does not.

## 2.0.3

Release date: March 14, 2022

### BugFix

The following bugs are fixed:

- Query fails when BE nodes are in suspended animation.
- Query fails when there is no appropriate execution plan for single-tablet table joins.  [#3854](https://github.com/StarRocks/starrocks/issues/3854)
- A deadlock problem may occur when an FE node collects information to build a global dictionary for low-cardinality optimization. [#3839](https://github.com/StarRocks/starrocks/issues/3839)
