# StarRocks version 2.3

## 2.3.0

Release date: July 29, 2022

### New Features

- The Primary Key model supports the full DELETE WHERE syntax. For more information, see [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#delete-and-primary-key-model).

- The Primary Key model supports the persistent primary key index. You can choose to persist and use the primary key index on disk rather than in memory, significantly reducing memory usage. For more information, see [Primary Key model](../table_design/Data_model.md#how-to-use-it-3).

- A global dictionary supports updates during real-time data ingestion，thus optimizing query performance and doubling query performance of string data.

- The CREATE TABLE AS SELECT statement can be executed asynchronously and write results to a new table. For more information, see [CREATE TABLE AS SELECT](sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT.md#create-table-as-select).

- Support the following resource group-related features:
  - Monitor resource groups: You can use the audit log to view a query in which resource group  and call API operations to obtain the monitoring metrics about specific resource groups. For more information, see [Monitor and Alerting](../administration/Monitor_and_Alert.md#monitor-and-alerting).
  - Limit the consumption of large queries on CPU, memory, or I/O resources: You can route queries to resource groups by matching classifiers or configure session variables to directly specify resource groups for queries. For more information, see [Resource group](../administration/resource_group.md).

- StarRocks provides JDBC external tables to conveniently query Oracle, PostgreSQL, MySQL, SQLServer, Clickhouse, and other databases. StarRocks also supports predicate pushdown, thus improving query performance. For more information, see [External table for a JDBC-compatible database](../using_starrocks/External_table.md#external-table-for-a-JDBC-compatible-database).

- [Preview] A new Data Source Connector framework is released to support external catalogs. You can use the external catalogs to directly access and query Hive without creating external tables. For more information, see [Use catalogs to manage internal and external data](../using_starrocks/Manage_data.md).

- Add the following functions:
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)
  - [ntile](../using_starrocks/Window_function.md)
  - [bitmap_union_count](../sql-reference/sql-functions/bitmap-functions/bitmap_union_count.md)、[base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md)、[array_to_bitmap](../sql-reference/sql-functions/array-functions/array_to_bitmap.md)
  - [week](../sql-reference/sql-functions/date-time-functions/week.md)、[time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md)

### Improvements

- The compaction mechanism can merge large metadata more quickly. This prevents metadata squeezing and excessive disk usage that can occur shortly after frequent data updates.

- The performance of loading Parquet files and compressed files is optimized.

- The mechanism of creating materialized views is optimized. After the optimization, materialized views can be created at a speed up to 10 times higher than before.

- The performance of the following operators is optimized:
  - TopN and sort operators
  - Equivalence comparison operators that contain functions can use Zone Map indexes when these operators are pushed down to scan operators.

- Optimize the Apache Hive™ external tables.
  - When Apache Hive™ tables are stored as Parquet, ORC, or CSV formats, StarRocks can synchronize the schema changes like ADD COLUMN and REPLACE COLUMN from hive tables when you perform REFRESH statement on external tables. For more information, see [Hive external table](../using_starrocks/External_table.md#hive-external-table).
  - Hive resources' `hive.metastore.uris` can be modified. For more information, see  [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER%20RESOURCE.md).

- Optimize the Apache Iceberg external tables. A custom catalog can be used to create an Iceberg resource. For more information, see [Apache Iceberg external table](../using_starrocks/External_table.md#apache-iceberg-external-table).

- Optimize the Elasticsearch external tables. Sniffing the addresses of the data nodes in an Elasticsearch cluster can be disabled. For more information, see [Elasticsearch external table](../using_starrocks/External_table.md#elasticsearch-external-table).

- When the sum() function calculates numeric strings, implicit conversion is  performed.

- The year, month, and day functions support the DATE data type.

### Bug Fixes

Fix the following bugs:

- CPU utilization increases abnormally high due to an excessive number of tablets.

- Problems cause the "fail to prepare tablet reader" error message to occur.

- The FEs fail to restart.[#5642](https://github.com/StarRocks/starrocks/issues/5642 )、[#4969](https://github.com/StarRocks/starrocks/issues/4969 )、[#5580](https://github.com/StarRocks/starrocks/issues/5580)

- The CTAS statement cannot be run successfully when the statement includes a JSON function. [#6498](https://github.com/StarRocks/starrocks/issues/6498)

### Others

- StarGo, a cluster management tool, can deploy, start, upgrade, and roll back clusters and manage multiple clusters. For more information, see [Deploy StarRocks with StarGo](../administration/stargo.md).

- The pipeline engine is enabled by default when you upgrade StarRocks to version 2.3 or deploy StarRocks. The pipeline engine can improve the performance of simple queries in high concurrency scenarios and complex queries. If you detect significant performance regressions when using StarRocks 2.3, you can disable the pipeline engine by executing the `SET GLOBAL` statement to set `enable_pipeline_engine` to `false`.
- The [SHOW GRANTS](../sql-reference/sql-statements/account-management/SHOW%20GRANTS.md) statement is compatible with the MySQL syntax and displays the privileges assigned to a user in the form of GRANT statements.
