# StarRocks version 2.0

## 2.0

Release date: january 5, 2022

### New Feature

- External Table
  - [Experimental Function]Support for Hive external table on S3
  - DecimalV3 support for external table [#425](https://github.com/StarRocks/starrocks/pull/425)
- Implement complex expressions to be pushed down to the storage layer for computation, thus gaining performance gains
- Primary Key is officially released, which supports Stream Load, Broker Load, Routine Load, and also provides a second-level synchronization tool for MySQL data based on Flink-cdc

## Improvement

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

## Bugfix

- Fix the problem that the Hive external table is timeout to get metadata in a large amount.
- Fix the problem of unclear error message of materialized view creation.
- Fix the implementation of like in vectorization engine [#722](https://github.com/StarRocks/starrocks/pull/722)
- Repair the error of parsing the predicate is in `alter table`[#725](https://github.com/StarRocks/starrocks/pull/725)
- Fix the problem that the `curdate` function can not format the date
