# StarRocks version 1.19

## 1.19.0

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

### Bug Fixes

* Fix the issue that the dynamic partition table cannot be created automatically after the data recovery operation is completed. [# 337](https://github.com/StarRocks/starrocks/issues/337)
* Fix the problem of error reported by row_number function after CBO is opened.
* Fix the problem of FE stuck due to statistical information collection
* Fix the problem that set_var takes effect for session but not for statements.
* Fix the problem that select count(*) returns abnormality on the Hive partition external table.

## 1.19.1

Release date: November 2, 2021

### Improvement

* Optimize the performance of `show frontends`. [# 507](https://github.com/StarRocks/starrocks/pull/507) [# 984](https://github.com/StarRocks/starrocks/pull/984)
* Add monitoring of slow queries. [# 502](https://github.com/StarRocks/starrocks/pull/502) [# 891](https://github.com/StarRocks/starrocks/pull/891)
* Optimize the fetching of Hive external metadata to achieve parallel fetching.[# 425](https://github.com/StarRocks/starrocks/pull/425) [# 451](https://github.com/StarRocks/starrocks/pull/451)

### Bug Fixes

* Fix the problem of Thrift protocol compatibility, so that the Hive external table can be connected with Kerberos. [# 184](https://github.com/StarRocks/starrocks/pull/184) [# 947](https://github.com/StarRocks/starrocks/pull/947) [# 995](https://github.com/StarRocks/starrocks/pull/995) [# 999](https://github.com/StarRocks/starrocks/pull/999)
* Fix several bugs in view creation. [# 972](https://github.com/StarRocks/starrocks/pull/972) [# 987](https://github.com/StarRocks/starrocks/pull/987)[# 1001](https://github.com/StarRocks/starrocks/pull/1001)
* Fix the problem that FE cannot be upgraded in grayscale. [# 485](https://github.com/StarRocks/starrocks/pull/485) [# 890](https://github.com/StarRocks/starrocks/pull/890)

## 1.19.2

Release date: November 20, 2021

### Improvement

* bucket shuffle join support right join and full outer join [# 1209](https://github.com/StarRocks/starrocks/pull/1209)  [# 31234](https://github.com/StarRocks/starrocks/pull/1234)

### Bug Fixes

* Fix the problem that repeat node cannot do predicate push-down[# 1410](https://github.com/StarRocks/starrocks/pull/1410) [# 1417](https://github.com/StarRocks/starrocks/pull/1417)
* Repair the problem that routine load may lost data when the cluster alter leader node during import.[# 1074](https://github.com/StarRocks/starrocks/pull/1074) [# 1272](https://github.com/StarRocks/starrocks/pull/1272)
* Fix the problem that creation view cannot support union [# 1083](https://github.com/StarRocks/starrocks/pull/1083)
* Fix some stability issues of Hive external table[# 1408](https://github.com/StarRocks/starrocks/pull/1408)
* Fix an issue with group by view[# 1231](https://github.com/StarRocks/starrocks/pull/1231)

## 1.19.3

Release date: November 30, 2021

### Improvement

* Upgrade jprotobuf version to improve security [# 1506](https://github.com/StarRocks/starrocks/issues/1506)

### Bug Fixes

* Fix some problems with group by result correctness
* Fix some problems with grouping sets[# 1395](https://github.com/StarRocks/starrocks/issues/1395) [# 1119](https://github.com/StarRocks/starrocks/pull/1119)
* Fix the problem of some indicators of date_format
* Fix a boundary condition issue with aggregated streamming[# 1584](https://github.com/StarRocks/starrocks/pull/1584)
* For details, please refer to[link](https://github.com/StarRocks/starrocks/compare/1.19.2...1.19.3)

## 1.19.4

Release date: December 9, 2021

### Improvement

* Support cast(varchar as bitmap) [# 1941](https://github.com/StarRocks/starrocks/pull/1941)
* Update access policy of hive external table [# 1394](https://github.com/StarRocks/starrocks/pull/1394) [# 1807](https://github.com/StarRocks/starrocks/pull/1807)

### Bug Fixes

* Fix the bug of wrong query result with predicate Cross Join [# 1918](https://github.com/StarRocks/starrocks/pull/1918)
* Fix the bug of decimal type and time type conversion [# 1709](https://github.com/StarRocks/starrocks/pull/1709) [# 1738](https://github.com/StarRocks/starrocks/pull/1738)
* Fix the bug of colocate join/replicate join selection error [# 1727](https://github.com/StarRocks/starrocks/pull/1727)
* Fix several plan cost calculation problems

## 1.19.5

Release date: December 20, 2021

### Improvement

* A plan to optimize shuffle join [# 2184](https://github.com/StarRocks/starrocks/pull/2184)
* Optimize multiple large file imports [# 2067](https://github.com/StarRocks/starrocks/pull/2067)

### Bug Fixes

* Upgrade Log4j2 to 2.17.0, fix security vulnerabilities[# 2284](https://github.com/StarRocks/starrocks/pull/2284)[# 2290](https://github.com/StarRocks/starrocks/pull/2290)
* Fix the problem of empty partition in Hive external table[# 707](https://github.com/StarRocks/starrocks/pull/707)[# 2082](https://github.com/StarRocks/starrocks/pull/2082)

## 1.19.7

Release date: March 18, 2022

### Bug Fixes

The following bugs are fixed:

* dataformat will produce different results in different versions. [#4165](https://github.com/StarRocks/starrocks/pull/4165)
* BE nodes may fail because Parquet files are deleted by mistake during data loading. [#3521](https://github.com/StarRocks/starrocks/pull/3521)
