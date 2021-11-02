# StarRocks version 1.19

## v1.19.0

Release date: Octorber 22, 2021

### New Feature

* Implement Global Runtime Filter, which can enable runtime filter for shuffle join.
* CBO Planner is enabled by default, improved colocated join, bucket shuffle, statistical information estimation, etc.
* [Experimental Function] Primary Key model release: To better support real-time/frequent update feature, StarRocks has added a new table type: primary key model. The model supports Stream Load, Broker Load, Routine Load and also provides a second-level synchronization tool for MySQL data based on Flink-cdc.
* [Experimental Function] Support write function for external tables. Support writing data to another StarRocks cluster table by external tables to solve the read/write separation requirement and provide better resource isolation.

### Improvement

#### StarRocks

* Performance optimization.
  * count distinct int statement
  * group by int statement
  * or statement
* Optimize disk balance algorithm. Data can be automatically balanced after adding disks to a single machine.
* Support partial column export.
* Optimize show processlist to show specific SQL.
* Support multiple variable settings in SET_VAR .
* Improve the error reporting information, including table_sink, routine load, creation of materialized view, etc.

#### StarRocks-DataX Connector

* Support setting interval flush StarRocks-DataX Writer.

### Bugfix

* Fix the issue that the dynamic partition table cannot be created automatically after the data recovery operation is completed. [# 337](https://github.com/StarRocks/starrocks/issues/337)
* Fix the problem of error reported by row_number function after CBO is opened.
* Fix the problem of FE stuck due to statistical information collection
* Fix the problem that set_var takes effect for session but not for statements.
* Fix the problem that select count(*) returns abnormality on the Hive partition external table.

## v1.19.1

Release date: November 2, 2021

### Improvement

* Optimize the performance of `show frontends`. [# 507](https://github.com/StarRocks/starrocks/pull/507) [# 984](https://github.com/StarRocks/starrocks/pull/984)
* Add monitoring of slow queries. [# 502](https://github.com/StarRocks/starrocks/pull/502) [# 891](https://github.com/StarRocks/starrocks/pull/891)
* Optimize the fetching of Hive external metadata to achieve parallel fetching.[# 425](https://github.com/StarRocks/starrocks/pull/425) [# 451](https://github.com/StarRocks/starrocks/pull/451)

### BugFix

* Fix the problem of Thrift protocol compatibility, so that the Hive external table can be connected with Kerberos. [# 184](https://github.com/StarRocks/starrocks/pull/184) [# 947](https://github.com/StarRocks/starrocks/pull/947) [# 995](https://github.com/StarRocks/starrocks/pull/995) [# 999](https://github.com/StarRocks/starrocks/pull/999)
* Fix several bugs in view creation. [# 972](https://github.com/StarRocks/starrocks/pull/972) [# 987](https://github.com/StarRocks/starrocks/pull/987)[# 1001](https://github.com/StarRocks/starrocks/pull/1001)
* Fix the problem that FE cannot be upgraded in grayscale. [# 485](https://github.com/StarRocks/starrocks/pull/485) [# 890](https://github.com/StarRocks/starrocks/pull/890)
