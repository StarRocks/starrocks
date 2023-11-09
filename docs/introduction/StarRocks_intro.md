---
displayed_sidebar: "English"
---

# StarRocks

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis. StarRocks has an MPP architecture and is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view and more. StarRocks supports real-time and batch data ingestion from a variety of data sources. It also allows you to directly analyze data stored in data lakes with zero data migration.

StarRocks is also compatible with MySQL protocols and can be easily connected using MySQL clients and popular BI tools. StarRocks is highly scalable, available, and easy to maintain. It is widely adopted in the industry, powering a variety of OLAP scenarios, such as real-time analytics, ad-hoc queries, data lake analytics and more.

Join our [Slack channel](https://join.slack.com/t/starrocks/shared_invite/zt-z5zxqr0k-U5lrTVlgypRIV8RbnCIAzg) for live support, discussion, or latest community news. You can also follow us on [LinkedIn](https://www.linkedin.com/company/starrocks) to get first-hand updates on new features, events, and sharing.

<NavBox>
<NavBoxPart title="About StarRocks">
<NavBoxPartItem>

- [Introduction](../introduction/what_is_starrocks)
- [Architecture](../introduction/Architecture)
- [Shared-data cluster](../deployment/shared_data/s3)
- [Features](../introduction/Features)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Get started​">
<NavBoxPartItem>

- [Deploy with Docker](../quick_start/deploy_with_docker)
- [Create a table](../quick_start/Create_table)
- [Ingest and query data](../quick_start/Import_and_query)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Table design ​">
<NavBoxPartItem>

- [Understand table design](../table_design/StarRocks_table_design)
- [Table types](../table_design/table_types/table_types)
- [Data distribution](../table_design/Data_distribution)
- [Sort keys and prefix indexes](../table_design/Sort_key)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Data loading">
<NavBoxPartItem>

- [Overview of data loading](../loading/Loading_intro)
- [Load data from a local file system or a streaming data source using HTTP PUT](../loading/StreamLoad)
- [Load data from HDFS](../loading/hdfs_load)
- [Load data from cloud storage](../loading/cloud_storage_load)
- [Continuously load data from Apache Kafka®](../loading/RoutineLoad)
- [Continuously load data from Apache Flink®](../loading/Flink-connector-starrocks)
- [Load data using Apache Spark™](../loading/Spark-connector-starrocks)
- [Load data using INSERT](../loading/InsertInto)
- [Realtime synchronization from MySQL](../loading/Flink_cdc_load)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Querying​">
<NavBoxPartItem title="Query acceleration">

- [Cost-based optimizer](../using_starrocks/Cost_based_optimizer)
- [Synchronous materialized view](../using_starrocks/Materialized_view-single_table)
- [Asynchronous materialized view](../using_starrocks/Materialized_view)
- [Colocate Join](../using_starrocks/Colocate_join)
- [Query cache](../using_starrocks/query_cache)

</NavBoxPartItem>
<NavBoxPartItem title="Query semi-structured data">

- [JSON](../sql-reference/sql-statements/data-types/JSON)
- [ARRAY](../sql-reference/sql-statements/data-types/Array)
- [MAP](../sql-reference/sql-statements/data-types/Map)
- [STRUCT](../sql-reference/sql-statements/data-types/STRUCT)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="Query external data sources​">

- [Apache Hive™](../data_source/catalog/hive_catalog)
- [Apache Hudi](../data_source/catalog/hudi_catalog)
- [Apache Iceberg](../data_source/catalog/iceberg_catalog)
- [Delta Lake](../data_source/catalog/deltalake_catalog)
- [MySQL and PostgreSQL](../data_source/catalog/jdbc_catalog)
- [Elasticsearch](../data_source/catalog/elasticsearch_catalog)

</NavBoxPartItem>
<NavBoxPartItem title="Integration​">

- [AWS](../integrations/authenticate_to_aws_resources)
- [Microsoft Azure Storage](../integrations/authenticate_to_azure_storage)
- [Google Cloud Storage](../integrations/authenticate_to_gcs)
- [BI tools](../integrations/BI_integrations/Hex)
- [IDE tools](../integrations/IDE_integrations/DataGrip)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Administration">
<NavBoxPartItem>

- [Manage a cluster](../administration/Cluster_administration)
- [Scale in and out a cluster](../administration/Scale_up_down)
- [Resource group](../administration/monitor_manage_big_queries)
- [Privileges](../administration/privilege_overview)
- [Data recovery](../administration/Data_recovery)
- [Tune query performance](../administration/Query_planning)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="References​">
<NavBoxPartItem>

- [SQL reference](../sql-reference/sql-statements/account-management/ALTER_USER)
- [Function reference](../sql-reference/sql-functions/date-time-functions/convert_tz)
- [Data type](../sql-reference/sql-statements/data-types/TINYINT)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="FAQ​">
<NavBoxPartItem>

- [Ingestion and export](../faq/loading/Loading_faq)
- [Deployment](../faq/Deploy_faq)
- [SQL](../faq/Sql_faq)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Benchmarks​">
<NavBoxPartItem>

- [SSB benchmark](../benchmarking/SSB_Benchmarking)
- [TPC-H benchmark](../benchmarking/TPC-H_Benchmarking)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
