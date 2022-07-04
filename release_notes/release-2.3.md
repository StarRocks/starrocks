# StarRocks version 2.3

## 2.3.0

### New Features

- The Primary Key model supports the full DELETE WHERE syntax. For more information, see [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#delete-and-primary-key-model).

- The primary key model supports persistent primary key indexes to prevent excessive memory consumption. For more information, see [Primary Key model](../table_design/Data_model.md#how-to-use-it-3).

- When a Routine Load job is executed, and a global dictionary is built to optimize queries on low-cardinality columns, the global dictionary can be updated, thus improving query performance.

- Stream Load provides a transaction interface that supports splitting the execution of "sending data" and "submitting transaction", enabling two-phase commit of transactions that stream data by using external systems such as Apache Flink® or Apache Kafka® and improve loading performance in highly concurrent scenarios. For example, when data stream into StarRocks by using Flink, the transaction interface allows for simultaneous data reception and data sending, and your transaction can be submitted at a proper time to complete a batch load. This way, the client side does not need to cache each batch of data. This reduces the memory usage on the client side and provides guarantees for the exactly-once commits of transactions. In addition, the transaction interface also supports the loading of multiple small files as a single batch. For more information, see [Load data by using Stream Load transaction interface](../loading/Stream_Load_transaction_interface.md).

- The CREATE TABLE AS SELECT statement can be executed asynchronously and write results to a new table. For more information, see [CREATE TABLE AS SELECT](sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT.md#create-table-as-select).

- Support the following resource group-related features:
  - Monitor resource groups: You can use the audit log to view a query in which resource group  and call API operations to obtain the monitoring metrics about specific resource groups. For more information, see [Monitor and Alerting](../administration/Monitor_and_Alert.md#monitor-and-alerting).
  - Limit the consumption of large queries on CPU, memory, or I/O resources: You can route queries to resource groups by matching classifiers or configure session variables to directly specify resource groups for queries. For more information, see [Resource group](../administration/resource_group.md).

- StarRocks provides JDBC external tables to query Oracle, PostgreSQL, MySQL, SQLServer, Clickhouse, and other databases. StarRocks also supports predicate pushdown when performing queries. For more information, see [External table for databases that support JDBC drivers](../using_starrocks/External_table.md#external-table-for-databases-that-support-jdbc-drivers).

- [Preview] A new Data Source Connector framework is released to support user-defined external catalogs. You can use the external catalogs to directly access and analyze Hive without creating external tables. For more information, see [Use catalogs to manage internal and external data](../using_starrocks/Manage_data.md).

- Add the following functions:
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)
  - [ntile](../using_starrocks/Window_function.md)
  - [bitmap_union_count](../sql-reference/sql-functions/bitmap-functions/bitmap_union_count.md)、[base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md)、[array_to_bitmap](../sql-reference/sql-functions/array-functions/array_to_bitmap.md)
  - [week](../sql-reference/sql-functions/date-time-functions/week.md)、[time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md)

- Add the [EXECUTE AS](../sql-reference/sql-statements/account-management/EXECUTE%20AS.md) statement. After you use the GRANT statement to impersonate a specific user identity to perform operations, you can use the EXECUTE AS statement to switch the execution context of the current session to this user.

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

- Optimize the [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 、[REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) statements.
  - Use the GRANT statement to grant a role to a user, or to grant user `a` the privilege to perform operations as user `b`.
  - Use the REVOKE statement to revoke a role from a user, or to revoke the privilege that allows user `a` to perform operations as user `b`.

- The year, month, and day functions support the DATE data type.

### Bug Fixes

Fix the following bugs:

- CPU utilization increases abnormally high due to an excessive number of tablets. [#5875](https://starrocks.atlassian.net/browse/SR-5875)

- Problems cause the "fail to prepare tablet reader" error message to occur.[#7248](https://starrocks.atlassian.net/browse/SR-7248)、 [#7854](https://starrocks.atlassian.net/browse/SR-7854)、 [#8257](https://starrocks.atlassian.net/browse/SR-8257)

- The FEs fail to restart.[#5642](https://github.com/StarRocks/starrocks/issues/5642 )、[#4969](https://github.com/StarRocks/starrocks/issues/4969 )、[#5580](https://github.com/StarRocks/starrocks/issues/5580)

- The CTAS statement cannot be run successfully when the statement includes a JSON function. [#6498](https://github.com/StarRocks/starrocks/issues/6498)

### Others

- StarGo, a cluster management tool, can deploy, start, upgrade, and roll back clusters and manage multiple clusters. For more information, see [Deploy StarRocks with StarGo](../administration/stargo.md).

- StarRocks clusters can be quickly deployed on AWS by using CloudFormation. For more information, see [Deploy a StarRocks Cluster on AWS by Using AWS CloudFormation](../administration/AWS_Cloudformation.md).
