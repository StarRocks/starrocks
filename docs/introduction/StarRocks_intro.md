# StarRocks

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis.  StarRocks has an MPP architecture and it is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view and more. StarRocks is also compatible with MySQL protocols and can be easily connected using MySQL clients and popular BI tools. StarRocks is highly scalable, available, and easy to maintain. It is widely adopted in the industry, powering a variety of OLAP scenarios, such as real-time analytics, ad-hoc queries, data lake analytics and more.

<NavBox>
<NavBoxPart title="About StarRocks">
<NavBoxPartItem>

- [Introduction](../introduction/what_is_starrocks.md)
- [Concepts](../quick_start/Concepts.md)
- [Architecture](../quick_start/Architecture.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Get started​">
<NavBoxPartItem>

- [Deploy StarRocks](../quick_start/Deploy.md)
- [Ingest and query data](../quick_start/Import_and_query.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Table design ​">
<NavBoxPartItem>

- [Overview of table design](../table_design/StarRocks_table_design)
- [Data models](../table_design/Data_model)
- [Data distribution](../table_design/Data_distribution)
- [Sort key and prefix index](../table_design/Sort_key)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Ingestion​">
<NavBoxPartItem>

- [Overview of ingestion](../loading/Loading_intro)
- [Ingestion via HTTP](../loading/StreamLoad)
- [Batch ingestion from HDFS or cloud object storage](../loading/BrokerLoad)
- [Continuous ingestion from Apache Kafka®](../loading/RoutineLoad)
- [Bulk ingestion and data transformation using Apache Spark™](../loading/SparkLoad)
- [Real-time synchronization from MySQL](../loading/Flink_cdc_load)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Querying​">
<NavBoxPartItem title="Query acceleration">

- [Cost-based optimizer](../using_starrocks/Cost_based_optimizer)
- [Materialized view](../using_starrocks/Materialized_view)
- [Colocate Join](../using_starrocks/Colocate_join)

</NavBoxPartItem>
<NavBoxPartItem title="Query semi-structured data">

- [JSON](../sql-reference/sql-statements/data-types/JSON)
- [ARRAY](../using_starrocks/Array)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="Query external data sources​">

- [Apache Hive™](../using_starrocks/External_table#hive-external-table)
- [Apache Hudi](../using_starrocks/External_table#hudi-external-table)
- [Apache Iceberg](../using_starrocks/External_table#apache-iceberg-external-table)
- [MySQL](../using_starrocks/External_table#mysql-external-table)
- [Elasticsearch](../using_starrocks/External_table#elasticsearch-external-table)
- [JDBC-compatible database](../using_starrocks/External_table#external-table-for-a-jdbc-compatible-database)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Administration">
<NavBoxPartItem>

- [Manage a cluster](../administration/Cluster_administration)
- [Scale in and out a cluster](../administration/Scale_up_down)
- [Tune query performance](../administration/Query_planning)
- [Manage workloads](../administration//resource_group)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="References​">
<NavBoxPartItem>

- [SQL reference](../sql-reference/sql-statements/account-management/ALTER%20USER)
- [Function reference](../sql-reference/sql-functions/date-time-functions/convert_tz)

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

- [SSB benchmark](../benchmarking/SSB_Benchmarking.md)
- [TPC-H benchmark](../benchmarking/TPC-H_Benchmarking.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
