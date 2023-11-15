# StarRocks

  StarRocks 是一款高性能分析型数据仓库，使用向量化、MPP 架构、CBO、智能物化视图、可实时更新的列式存储引擎等技术实现多维、实时、高并发的数据分析。StarRocks 既支持从各类实时和离线的数据源高效导入数据，也支持直接分析数据湖上各种格式的数据。StarRocks 兼容 MySQL 协议，可使用 MySQL 客户端和常用 BI 工具对接。同时 StarRocks 具备水平扩展，高可用、高可靠、易运维等特性。广泛应用于实时数仓、OLAP 报表、数据湖分析等场景。

<NavBox>
<NavBoxPart title="StarRocks 入门">
<NavBoxPartItem title="产品介绍​">

- [产品优势和应用场景](../introduction/what_is_starrocks)
- [系统架构](../introduction/Architecture)
- [存算分离](../deployment/deploy_shared_data)
- [产品特性](../introduction/Features)
- [视频资源](../faq/Video)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="快速开始​">

- [使用 Docker 部署](../quick_start/deploy_with_docker)
- [创建表](../quick_start/Create_table)
- [导入和查询数据](../quick_start/Import_and_query)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="表设计​">
<NavBoxPartItem>

- [理解表设计](../table_design/StarRocks_table_design)
- [数据模型](../table_design/table_types/table_types)
- [数据分布](../table_design/Data_distribution)
- [排序键和前缀索引](../table_design/Sort_key)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="数据导入​">
<NavBoxPartItem>

- [导入总览](../loading/Loading_intro)
- [通过 HTTP PUT 从本地文件系统或流式数据源导入](../loading/StreamLoad)
- [从 HDFS 导入](../loading/hdfs_load)
- [从云存储导入](../loading/cloud_storage_load)
- [从 Apache Kafka® 持续导入](../loading/RoutineLoad)
- [从 Apache Flink® 持续导入](../loading/Flink-connector-starrocks)
- [从 Apache Spark™ 导入](../loading/Spark-connector-starrocks)
- [使用 INSERT 语句导入](../loading/InsertInto)
- [从 MySQL 实时同步](../loading/Flink_cdc_load)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="数据查询​">
<NavBoxPartItem title="提高查询性能">

- [CBO 优化器](../using_starrocks/Cost_based_optimizer)
- [同步物化视图](../using_starrocks/Materialized_view-single_table)
- [异步物化视图](../using_starrocks/Materialized_view)
- [Colocate Join](../using_starrocks/Colocate_join)
- [Query Cache](../using_starrocks/query_cache)

</NavBoxPartItem>
<NavBoxPartItem title="查询半结构化数据">

- [JSON](../sql-reference/sql-statements/data-types/JSON)
- [ARRAY](../sql-reference/sql-statements/data-types/Array)
- [MAP](../sql-reference/sql-statements/data-types/Map)
- [STRUCT](../sql-reference/sql-statements/data-types/STRUCT)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="查询外部数据源​">

- [Apache Hive™](../data_source/catalog/hive_catalog)
- [Apache Iceberg](../data_source/catalog/iceberg_catalog)
- [Apache Hudi](../data_source/catalog/hudi_catalog)
- [Delta Lake](../data_source/catalog/deltalake_catalog)
- [MySQL 和 PostgreSQL](../data_source/catalog/jdbc_catalog)
- [Elasticsearch](../data_source/catalog/elasticsearch_catalog)

</NavBoxPartItem>
<NavBoxPartItem title="外部系统集成​">

- [AWS](../integrations/authenticate_to_aws_resources)
- [Microsoft Azure Storage](../integrations/authenticate_to_azure_storage)
- [Google Cloud Storage](../integrations/authenticate_to_gcs)
- [BI 集成](../integrations/BI_integrations/Hex)
- [IDE 集成](../integrations/IDE_integrations/DataGrip)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="管理 StarRocks">
<NavBoxPartItem>

- [部署使用](../administration/Build_in_docker)
- [运维操作](../administration/Scale_up_down)
- [资源隔离](../administration/monitor_manage_big_queries)
- [用户权限](../administration/privilege_overview)
- [数据恢复](../administration/Data_recovery)
- [性能调优](../administration/Query_planning)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="参考​">
<NavBoxPartItem>

- [SQL参考](../sql-reference/sql-statements/account-management/ALTER_USER)
- [函数参考](../sql-reference/sql-functions/date-time-functions/convert_tz)
- [数据类型](../sql-reference/sql-statements/data-types/TINYINT)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="常见问题​">
<NavBoxPartItem>

- [数据导入和导出](../faq/loading/Loading_faq)
- [部署运维](../faq/Deploy_faq)
- [SQL](../faq/Sql_faq)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="性能测试​">
<NavBoxPartItem>

- [SSB 基准测试](../benchmarking/SSB_Benchmarking)
- [TPC-H 基准测试](../benchmarking/TPC-H_Benchmarking)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
