# StarRocks

StarRocks is a next-gen, high-performance analytical data warehouse that enables real-time, multi-dimensional, and highly concurrent data analysis.  StarRocks has an MPP architecture and it is equipped with a fully vectorized execution engine, a columnar storage engine that supports real-time updates, and is powered by a rich set of features including a fully-customized cost-based optimizer (CBO), intelligent materialized view and more. StarRocks is also compatible with MySQL protocols and can be easily connected using MySQL clients and popular BI tools. StarRocks is highly scalable, available, and easy to maintain. It is widely adopted in the industry, powering a variety of OLAP scenarios, such as real-time analytics, ad-hoc queries, data lake analytics and more.

<NavBox>
<NavBoxPart title="About StarRocks">
<NavBoxPartItem>

- [Introduction](/docs/introduction/what_is_starrocks.md)
- [Concepts](/docs/quick_start/Concepts.md)
- [Architecture](/docs/quick_start/Architecture.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Get started​">
<NavBoxPartItem>

- [Deploy StarRocks](/docs/quick_start/Deploy.md)
- [Ingest and query data](/docs/quick_start/Import_and_query.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Table design ​">
<NavBoxPartItem>

- [Overview of table design](/docs/table_design/StarRocks_table_design)
- [Data models](/docs/table_design/Data_model)
- [Data distribution](/docs/table_design/Data_distribution)
- [Sort key and prefix index](/docs/table_design/Sort_key)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Ingestion​">
<NavBoxPartItem>

- [Overview of ingestion](/docs/loading/Loading_intro)
- [Ingestion via HTTP](/docs/loading/StreamLoad)
- [Batch ingestion from HDFS or cloud object storage](/docs/loading/BrokerLoad)
- [Continuous ingestion from Apache Kafka®](/docs/loading/RoutineLoad)
- [Bulk ingestion and data transformation using Apache Spark™](/docs/loading/SparkLoad)
- [Real-time synchronization from MySQL](/docs/loading/Flink_cdc_load)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Querying​">
<NavBoxPartItem title="Query acceleration">

- [Cost-based optimizer](/docs/using_starrocks/Cost_based_optimizer)
- [Materialized view](/docs/using_starrocks/Materialized_view)
- [Colocate Join](/docs/using_starrocks/Colocate_join)

</NavBoxPartItem>
<NavBoxPartItem title="Query semi-structured data">

- [JSON](/docs/sql-reference/sql-statements/data-types/JSON)
- [ARRAY](/docs/using_starrocks/Array)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="Query external data sources​">

- [Apache Hive™](/docs/using_starrocks/External_table#hive-external-table)
- [Apache Hudi](/docs/using_starrocks/External_table#hudi-external-table)
- [Apache Iceberg](/docs/using_starrocks/External_table#apache-iceberg-external-table)
- [MySQL](/docs/using_starrocks/External_table#mysql-external-table)
- [Elasticsearch](/docs/using_starrocks/External_table#elasticsearch-external-table)
- [JDBC-compatible database](/docs/using_starrocks/External_table#external-table-for-a-jdbc-compatible-database)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="Administration">
<NavBoxPartItem>

- [Manage a cluster](/docs/administration/Cluster_administration)
- [Scale in and out a cluster](/docs/administration/Scale_up_down)
- [Tune query performance](/docs/administration/Query_planning)
- [Manage workloads](/docs/administration//resource_group)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="References​">
<NavBoxPartItem>

- [SQL reference](/docs/sql-reference/sql-statements/account-management/ALTER%20USER)
- [Function reference](/docs/sql-reference/sql-functions/date-time-functions/convert_tz)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="FAQ​">
<NavBoxPartItem>

- [Ingestion and export](/docs/faq/loading/Loading_faq)
- [Deployment](/docs/faq/Deploy_faq)
- [SQL](/docs/faq/Sql_faq)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="Benchmarks​">
<NavBoxPartItem>

- [SSB benchmark](/docs/benchmarking/SSB_Benchmarking.md)
- [TPC-H benchmark](/docs/benchmarking/TPC-H_Benchmarking.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
