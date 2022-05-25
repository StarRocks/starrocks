# StarRocks version 2.2

## 2.2.0

Release date: May 22, 2022

### New Features

- [Preview] The resource group management feature is released. This feature allows StarRocks to isolate and efficiently use CPU and memory resources when StarRocks processes both complex queries and simple queries from different tenants in the same cluster.

- [Preview] A Java-based user-defined function (UDF) framework is implemented. This framework supports UDFs that are compiled in compliance with the syntax of Java to extend the capabilities of StarRocks.

- [Preview] The primary key model supports updates only to specific columns when data is loaded to the primary key model in real-time data update scenarios such as order updates and multi-stream joins.

- [Preview] JSON data types and JSON functions are supported.

- External tables can be used to query data from Apache Hudi. This further improves users' data lake analytics experience with StarRocks. For more information, see [External tables](../using_starrocks/External_table.md).

- The following functions are added:
  - ARRAY functions: [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md), array_sort, array_distinct, array_join, reverse, array_slice, array_concat, array_difference, arrays_overlap, and array_intersect
  - BITMAP functions: bitmap_max and bitmap_min
  - Other functions: retention and square

### Improvements

- The parser and analyzer of the cost-based optimizer (CBO) are restructured, the code structure is optimized, and syntaxes such as INSERT with Common Table Expression (CTE) are supported. These improvements are made to increase the performance of complex queries, such as queries that involve the reuse of CTEs.

- The performance of queries on Apache Hive™ external tables that are stored in cloud object storage services such as AWS Simple Storage Service (S3), Alibaba Cloud Object Storage Service (OSS), and Tencent Cloud Object Storage (COS) is optimized. After the optimization, the performance of object storage-based queries is comparable to that of HDFS-based queries. Additionally, late materialization of ORC files is supported, and queries on small files are accelerated. For more information, see [Apache Hive™ external table](../using_starrocks/External_table.md).

- When queries from Apache Hive™ are run by using external tables, StarRocks automatically performs incremental updates to the cached metadata by consuming Hive metastore events such as data changes and partition changes. StarRocks also supports queries on data of the DECIMAL and ARRAY types from Apache Hive™. For more information, see [Apache Hive™ external table](../using_starrocks/External_table.md).

- The UNION ALL operator is optimized to run 2 to 25 times faster than before.

- A pipeline engine that supports adaptive parallelism and provides optimized profiles is released to improve the performance of simple queries in high concurrency scenarios.

- Multiple characters can be combined and used as a single row delimiter for CSV files that are to be imported.

### Bug Fixes

- Deadlocks occur if data is loaded or changes are committed into tables that are based on the primary key model. [#4998](https://github.com/StarRocks/starrocks/pull/4998)

- Frontends (FEs), including FEs that run Oracle Berkeley DB Java Edition (BDB JE), are unstable. [#4428](https://github.com/StarRocks/starrocks/pull/4428), [#4666](https://github.com/StarRocks/starrocks/pull/4666), [#2](https://github.com/StarRocks/bdb-je/pull/2)

- The result that is returned by the SUM function encounters an arithmetic overflow if the function is invoked on a large amount of data. [#3944](https://github.com/StarRocks/starrocks/pull/3944)

- The precision of the results that are returned by the ROUND and TRUNCATE functions is unsatisfactory. [#4256](https://github.com/StarRocks/starrocks/pull/4256)

- A few bugs are detected by Synthesized Query Lancer (SQLancer). For more information, see [SQLancer-related issues](https://github.com/StarRocks/starrocks/issues?q=is:issue++label:sqlancer++milestone:2.2).

### Others

Flink-connector-starrocks supports Apache Flink® v1.14.
