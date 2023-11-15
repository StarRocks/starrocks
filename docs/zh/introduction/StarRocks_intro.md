# StarRocks

  StarRocks 是一款高性能分析型数据仓库，使用向量化、MPP 架构、CBO、智能物化视图、可实时更新的列式存储引擎等技术实现多维、实时、高并发的数据分析。StarRocks 既支持从各类实时和离线的数据源高效导入数据，也支持直接分析数据湖上各种格式的数据。StarRocks 兼容 MySQL 协议，可使用 MySQL 客户端和常用 BI 工具对接。同时 StarRocks 具备水平扩展，高可用、高可靠、易运维等特性。广泛应用于实时数仓、OLAP 报表、数据湖分析等场景。

<NavBox>
<NavBoxPart title="StarRocks 入门">
<NavBoxPartItem title="产品介绍​">

- [产品优势和应用场景](/introduction/what_is_starrocks)
- [系统架构](/introduction/system_architecture)
- [产品特性](/introduction/features)
- [视频资源](/faq/Video)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="快速开始​">

- [手动部署 StarRocks](/quick_start/Deploy)
- [创建表](/quick_start/Create_table)
- [导入和查询数据](/quick_start/Import_and_query)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="表设计​">
<NavBoxPartItem>

- [理解表设计](/table_design/StarRocks_table_design)
- [数据模型](/table_design/table_types/table_types)
- [数据分布](/table_design/Data_distribution)
- [排序键和前缀索引](/table_design/Sort_key)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="数据导入​">
<NavBoxPartItem>

- [导入总览](/loading/Loading_intro)
- [通过 HTTP PUT 从本地文件系统或流式数据源导入](/loading/StreamLoad)
- [从 HDFS 或外部云存储系统导入](/loading/BrokerLoad)
- [从 Apache Kafka® 持续导入](/loading/RoutineLoad)
- [使用 Apache Spark™ 导入](/loading/SparkLoad)
- [使用 INSERT 语句导入](/loading/InsertInto)
- [从 MySQL 实时同步](/loading/Flink_cdc_load)
- [从 Apache Flink® 持续导入](/loading/Flink-connector-starrocks)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="数据查询​">
<NavBoxPartItem title="提高查询性能">

- [CBO 优化器](/using_starrocks/Cost_based_optimizer)
- [物化视图](/using_starrocks/Materialized_view)
- [Colocate Join](/using_starrocks/Colocate_join)

</NavBoxPartItem>
<NavBoxPartItem title="查询半结构化数据">

- [JSON](/sql-reference/sql-statements/data-types/JSON)
- [ARRAY](/sql-reference/sql-statements/data-types/Array)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="查询外部数据源​">

- [Apache Hive™](/data_source/catalog/hive_catalog)
- [Apache Iceberg](/data_source/catalog/iceberg_catalog)
- [Apache Hudi](/data_source/catalog/hudi_catalog)
- [MySQL](/data_source/External_table#mysql-%E5%A4%96%E9%83%A8%E8%A1%A8)
- [Elasticsearch](/data_source/External_table#elasticsearch-%E5%A4%96%E9%83%A8%E8%A1%A8)
- [支持 JDBC 的数据库](/data_source/External_table#更多数据库jdbc的外部表)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="管理 StarRocks">
<NavBoxPartItem>

- [部署使用](/administration/Build_in_docker)
- [运维操作](/administration/Scale_up_down)
- [资源隔离](/administration/resource_group)
- [数据恢复](/administration/Data_recovery)
- [性能调优](/administration/Query_planning)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="参考​">
<NavBoxPartItem>

- [SQL参考](/sql-reference/sql-statements/account-management/ALTER_USER)
- [函数参考](/sql-reference/sql-functions/date-time-functions/convert_tz)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="常见问题​">
<NavBoxPartItem>

- [数据导入和导出](/faq/loading/Loading_faq)
- [部署运维](/faq/Deploy_faq)
- [SQL](/faq/Sql_faq)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="性能测试​">
<NavBoxPartItem>

- [SSB 基准测试](/benchmarking/SSB_Benchmarking)
- [TPC-H 基准测试](/benchmarking/TPC-H_Benchmark)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
