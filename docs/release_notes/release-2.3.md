# StarRocks version 2.3

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

- The Primary Key model supports complete DELETE WHERE syntax. For more information, see [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#delete-and-primary-key-model).

- The Primary Key model supports persistent primary key indexes. You can choose to persist the primary key index on disk rather than in memory, significantly reducing memory usage. For more information, see [Primary Key model](../table_design/Data_model.md#how-to-use-it-3).

- Global dictionary can be updated during real-time data ingestion，optimizing query performance and delivering 2X query performance for string data.

- The CREATE TABLE AS SELECT statement can be executed asynchronously. For more information, see [CREATE TABLE AS SELECT](../sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT.md).

- Support the following resource group-related features:
  - Monitor resource groups: You can view the resource group of the query in the audit log and obtain the metrics of the resource group by calling APIs. For more information, see [Monitor and Alerting](../administration/Monitor_and_Alert.md#monitor-and-alerting).
  - Limit the consumption of large queries on CPU, memory, and I/O resources: You can route queries to specific resource groups based on the classifiers or by configuring session variables. For more information, see [Resource group](../administration/resource_group.md).

- JDBC external tables can be used to conveniently query data in Oracle, PostgreSQL, MySQL, SQLServer, ClickHouse, and other databases. StarRocks also supports predicate pushdown, improving query performance. For more information, see [External table for a JDBC-compatible database](../using_starrocks/External_table.md#external-table-for-a-JDBC-compatible-database).

- [Preview] A new Data Source Connector framework is released to support external catalogs. You can use external catalogs to directly access and query Hive data without creating external tables. For more information, see [Use catalogs to manage internal and external data](../using_starrocks/Manage_data.md).

- Added the following functions:
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)
  - [ntile](../using_starrocks/Window_function.md)
  - [bitmap_union_count](../sql-reference/sql-functions/bitmap-functions/bitmap_union_count.md), [base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md), [array_to_bitmap](../sql-reference/sql-functions/array-functions/array_to_bitmap.md)
  - [week](../sql-reference/sql-functions/date-time-functions/week.md), [time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md)

### Improvements

- The compaction mechanism can merge large volume of metadata more quickly. This prevents metadata squeezing and excessive disk usage that can occur shortly after frequent data updates.

- Optimized the performance of loading Parquet files and compressed files.

- Optimized the mechanism of creating materialized views. After the optimization, materialized views can be created at a speed up to 10 times faster than before.

- Optimized the performance of the following operators:
  - TopN and sort operators
  - Equivalence comparison operators that contain functions can use Zone Map indexes when these operators are pushed down to scan operators.

- Optimized Apache Hive™ external tables.
  - When Apache Hive™ tables are stored in Parquet, ORC, or CSV format, schema changes caused by ADD COLUMN or REPLACE COLUMN on Hive can be synchronized to StarRocks when you execute the REFRESH statement on the corresponding Hive external table. For more information, see [Hive external table](../using_starrocks/External_table.md#hive-external-table).
  - `hive.metastore.uris` can be modified for Hive resources. For more information, see [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER%20RESOURCE.md).

- Optimized the performance of Apache Iceberg external tables. A custom catalog can be used to create an Iceberg resource. For more information, see [Apache Iceberg external table](../using_starrocks/External_table.md#apache-iceberg-external-table).

- Optimized the performance of Elasticsearch external tables. Sniffing the addresses of the data nodes in an Elasticsearch cluster can be disabled. For more information, see [Elasticsearch external table](../using_starrocks/External_table.md#elasticsearch-external-table).

- When the sum() function accepts a numeric string, it implicitly converts the numeric string.

- The year(), month(), and day() functions support the DATE data type.

### Bug Fixes

Fixed the following bugs:

- CPU utilization surges due to an excessive number of tablets.

- Issues that cause "fail to prepare tablet reader" to occur.

- The FEs fail to restart.[#5642](https://github.com/StarRocks/starrocks/issues/5642 )  [#4969](https://github.com/StarRocks/starrocks/issues/4969 )  [#5580](https://github.com/StarRocks/starrocks/issues/5580)

- The CTAS statement cannot be run successfully when the statement includes a JSON function. [#6498](https://github.com/StarRocks/starrocks/issues/6498)

### Others

- StarGo, a cluster management tool, can deploy, start, upgrade, and roll back clusters and manage multiple clusters. For more information, see [Deploy StarRocks with StarGo](../administration/stargo.md).

- The pipeline engine is enabled by default when you upgrade StarRocks to version 2.3 or deploy StarRocks. The pipeline engine can improve the performance of simple queries in high concurrency scenarios and complex queries. If you detect significant performance regressions when using StarRocks 2.3, you can disable the pipeline engine by executing the `SET GLOBAL` statement to set `enable_pipeline_engine` to `false`.
- The [SHOW GRANTS](../sql-reference/sql-statements/account-management/SHOW%20GRANTS.md) statement is compatible with the MySQL syntax and displays the privileges assigned to a user in the form of GRANT statements.
