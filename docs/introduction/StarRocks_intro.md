# StarRocks

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis. StarRocks has an MPP architecture and is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view and more. StarRocks supports real-time and batch data ingestion from a variety of data sources. It also allows you to directly analyze data stored in data lakes with zero data migration.

StarRocks is also compatible with MySQL protocols and can be easily connected using MySQL clients and popular BI tools. StarRocks is highly scalable, available, and easy to maintain. It is widely adopted in the industry, powering a variety of OLAP scenarios, such as real-time analytics, ad-hoc queries, data lake analytics and more.

Join our [Slack channel](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg) for live support, discussion, or latest community news. You can also follow us on [LinkedIn](https://www.linkedin.com/company/starrocks) to get first-hand updates on new features, events, and sharing.

<NavBox>
<NavBoxPart title="About StarRocks">
<NavBoxPartItem>

- [Introduction](../introduction/what_is_starrocks.md)
- [Architecture](../introduction/Architecture.md)
- [Shared-data StarRocks cluster](../deployment/deploy_shared_data.md)
- [Features](../introduction/Features.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Get started​">
<NavBoxPartItem>

- [Deploy with Docker](../quick_start/deploy_with_docker.md)
- [Create a table](../quick_start/Create_table.md)
- [Ingest and query data](../quick_start/Import_and_query.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Table design ​">
<NavBoxPartItem>

- [Understand table design](../table_design/StarRocks_table_design.md)
- [Table types](../table_design/table_types/table_types.md)
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
- [Load data using Apache Spark™](../loading/Spark-connector-starrocks.md)
- [Load data using INSERT](../loading/InsertInto.md)
- [Realtime synchronization from MySQL](../loading/Flink_cdc_load.md)
- [Continuously load data from Apache Flink®](../loading/Flink-connector-starrocks.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Querying​">
<NavBoxPartItem title="Query acceleration">

- [Cost-based optimizer](../using_starrocks/Cost_based_optimizer.md)
- [Synchronous materialized view](../using_starrocks/Materialized_view-single_table.md)
- [Asynchronous materialized views](../using_starrocks/Materialized_view.md)
- [Colocate Join](../using_starrocks/Colocate_join.md)
- [Query cache](../using_starrocks/query_cache.md)

</NavBoxPartItem>
<NavBoxPartItem title="Query semi-structured data">

- [JSON](../sql-reference/sql-statements/data-types/JSON.md)
- [ARRAY](../sql-reference/sql-statements/data-types/Array.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="Query external data sources​">

- [Apache Hive™](../data_source/catalog/hive_catalog.md)
- [Apache Hudi](../data_source/catalog/hudi_catalog.md)
- [Apache Iceberg](../data_source/catalog/iceberg_catalog.md)
- [Delta Lake](../data_source/catalog/deltalake_catalog.md)
- [MySQL](../data_source/External_table.md#mysql-external-table)
- [Elasticsearch](../data_source/External_table.md#elasticsearch-external-table)
- [JDBC-compatible database](../data_source/catalog/jdbc_catalog.md)

</NavBoxPartItem>
<NavBoxPartItem title="Integration​">

- [AWS](../integrations/authenticate_to_aws_resources.md)
- [Microsoft Azure Storage](../integrations/authenticate_to_azure_storage.md)
- [Google Cloud Storage](../integrations/authenticate_to_gcs.md)
- [BI tools](../integrations/BI_integrations/Hex.md)
- [IDE tools](../integrations/IDE_integrations/DataGrip.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Administration">
<NavBoxPartItem>

- [Manage a cluster](../administration/Cluster_administration.md)
- [Scale in and out a cluster](../administration/Scale_up_down.md)
- [Resource group](../administration/monitor_manage_big_queries.md)
- [Privileges](../administration/privilege_overview.md)
- [Data recovery](../administration/Data_recovery.md)
- [Tune query performance](../administration/Query_planning.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="References​">
<NavBoxPartItem>

- [SQL reference](../sql-reference/sql-statements/account-management/ALTER%20USER.md)
- [Function reference](../sql-reference/sql-functions/date-time-functions/convert_tz.md)
- [Data type](../sql-reference/sql-statements/data-types/TINYINT.md)

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
