# StarRocks

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis. StarRocks has an MPP architecture and is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view and more. StarRocks supports real-time and batch data ingestion from a variety of data sources. It also allows you to directly analyze data stored in data lakes with zero data migration.

StarRocks is also compatible with MySQL protocols and can be easily connected using MySQL clients and popular BI tools. StarRocks is highly scalable, available, and easy to maintain. It is widely adopted in the industry, powering a variety of OLAP scenarios, such as real-time analytics, ad-hoc queries, data lake analytics and more.

Join our [Slack channel](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg) for live support, discussion, or latest community news. You can also follow us on [LinkedIn](https://www.linkedin.com/company/starrocks) to get first-hand updates on new features, events, and sharing.

<NavBox>
<NavBoxPart title="About StarRocks">
<NavBoxPartItem>

- [Introduction](../introduction/what_is_starrocks.md)
- [Architecture](../introduction/Architecture.md)
- [Features](../introduction/Features.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Get started​">
<NavBoxPartItem>

- [Deploy StarRocks](../quick_start/Deploy.md)
- [Create a table](../quick_start/create_table.md)
- [Ingest and query data](../quick_start/Import_and_query.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Table design ​">
<NavBoxPartItem>

- [Understand table design](../table_design/StarRocks_table_design.md)
- [Data models](../table_design/Data_model.md)
- [Data distribution](../table_design/Data_distribution.md)
- [Sort keys and prefix indexes](../table_design/Sort_key.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Data loading">
<NavBoxPartItem>

- [Overview of data loading](../loading/Loading_intro.md)
- [Load data from a local file system or a streaming data source using HTTP PUT](../loading/StreamLoad.md)
- [Load data from HDFS or cloud storage](../loading/BrokerLoad.md)
- [Continuously load data from Apache Kafka®](../loading/RoutineLoad.md)
- [Bulk load using Apache Spark™](../loading/SparkLoad.md)
- [Load data using INSERT](../loading/InsertInto.md)
- [Synchronize data from MySQL in real time](../loading/Flink_cdc_load.md)
- [Continuously load data from Apache Flink®](../loading/Flink-connector-starrocks.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Querying​">
<NavBoxPartItem title="Query acceleration">

- [Cost-based optimizer](../using_starrocks/Cost_based_optimizer.md)
- [Materialized view](../using_starrocks/Materialized_view.md)
- [Colocate Join](../using_starrocks/Colocate_join.md)

</NavBoxPartItem>
<NavBoxPartItem title="Query semi-structured data">

- [JSON](../sql-reference/sql-statements/data-types/JSON.md)
- [ARRAY](../sql-reference/sql-statements/data-types/Array.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="Query external data sources​">

- [Apache Hive™](../data_source/Manage_data.md)
- [Apache Hudi](../data_source/External_table.md#hudi-external-table)
- [Apache Iceberg](../data_source/External_table.md#apache-iceberg-external-table)
- [MySQL](../data_source/External_table.md#mysql-external-table)
- [Elasticsearch](../data_source/External_table.md#elasticsearch-external-table)
- [JDBC-compatible database](../data_source/External_table.md#external-table-for-a-jdbc-compatible-database)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Administration">
<NavBoxPartItem>

- [Manage a cluster](../administration/Cluster_administration.md)
- [Scale in and out a cluster](../administration/Scale_up_down.md)
- [Resource group](../administration/resource_group.md)
- [Data recovery](../administration/Data_recovery.md)
- [Tune query performance](../administration/Query_planning.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="References​">
<NavBoxPartItem>

- [SQL reference](../sql-reference/sql-statements/account-management/ALTER_USER.md)
- [Function reference](../sql-reference/sql-functions/date-time-functions/convert_tz.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="FAQ​">
<NavBoxPartItem>

- [Ingestion and export](../faq/loading/Loading_faq.md)
- [Deployment](../faq/Deploy_faq.md)
- [SQL](../faq/Sql_faq.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Benchmarks​">
<NavBoxPartItem>

- [SSB benchmark](../benchmarking/SSB_Benchmarking.md)
- [TPC-H benchmark](../benchmarking/TPC-H_Benchmarking.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
