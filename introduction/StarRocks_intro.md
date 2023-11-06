# StarRocks

  StarRocks 是一款高性能分析型数据仓库，使用向量化、MPP 架构、CBO、智能物化视图、可实时更新的列式存储引擎等技术实现多维、实时、高并发的数据分析。StarRocks 既支持从各类实时和离线的数据源高效导入数据，也支持直接分析数据湖上各种格式的数据。StarRocks 兼容 MySQL 协议，可使用 MySQL 客户端和常用 BI 工具对接。同时 StarRocks 具备水平扩展，高可用、高可靠、易运维等特性。广泛应用于实时数仓、OLAP 报表、数据湖分析等场景。

<NavBox>
<NavBoxPart title="StarRocks 入门">
<NavBoxPartItem title="产品介绍​">

- [产品优势和应用场景](../introduction/what_is_starrocks.md)
- [系统架构](../introduction/Architecture.md)
- [存算分离](../deployment/deploy_shared_data.md)
- [产品特性](../introduction/Features.md)
- [视频资源](../faq/Video.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="快速开始​">

- [使用 Docker 部署](../quick_start/deploy_with_docker.md)
- [创建表](../quick_start/Create_table.md)
- [导入和查询数据](../quick_start/Import_and_query.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="表设计​">
<NavBoxPartItem>

- [理解表设计](../table_design/StarRocks_table_design.md)
- [数据模型](../table_design/table_types/table_types.md)
- [数据分布](../table_design/Data_distribution.md)
- [排序键和前缀索引](../table_design/Sort_key.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="数据导入​">
<NavBoxPartItem>

- [导入总览](../loading/Loading_intro.md)
- [通过 HTTP PUT 从本地文件系统或流式数据源导入](../loading/StreamLoad.md)
- [从 HDFS 或外部云存储系统导入](../loading/BrokerLoad.md)
- [从 Apache Kafka® 持续导入](../loading/RoutineLoad.md)
- [使用 Apache Spark™ 导入](../loading/SparkLoad.md)
- [使用 INSERT 语句导入](../loading/InsertInto.md)
- [从 MySQL 实时同步](../loading/Flink_cdc_load.md)
- [从 Apache Flink® 持续导入](../loading/Flink-connector-starrocks.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="数据查询​">
<NavBoxPartItem title="提高查询性能">

- [CBO 优化器](../using_starrocks/Cost_based_optimizer.md)
- [同步物化视图](../using_starrocks/Materialized_view-single_table.md)
- [异步物化视图](../using_starrocks/Materialized_view.md)
- [Colocate Join](../using_starrocks/Colocate_join.md)
- [Query Cache](../using_starrocks/query_cache.md)

</NavBoxPartItem>
<NavBoxPartItem title="查询半结构化数据">

- [JSON](../sql-reference/sql-statements/data-types/JSON.md)
- [ARRAY](../sql-reference/sql-statements/data-types/Array.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart>
<NavBoxPartItem title="查询外部数据源​">

- [Apache Hive™](../data_source/catalog/hive_catalog.md)
- [Apache Iceberg](../data_source/catalog/iceberg_catalog.md)
- [Apache Hudi](../data_source/catalog/hudi_catalog.md)
- [Delta Lake](../data_source/catalog/deltalake_catalog.md)
- [MySQL](../data_source/External_table.md#deprecated-mysql-外部表)
- [Elasticsearch](../data_source/External_table.md#deprecated-elasticsearch-外部表)
- [支持 JDBC 的数据库](../data_source/catalog/jdbc_catalog.md)

</NavBoxPartItem>
<NavBoxPartItem title="外部系统集成​">

- [AWS](../integrations/authenticate_to_aws_resources.md)
- [Microsoft Azure Storage](../integrations/authenticate_to_azure_storage.md)
- [Google Cloud Storage](../integrations/authenticate_to_gcs.md)
- [BI 集成](../integrations/BI_integrations/Hex.md)
- [IDE 集成](../integrations/IDE_integrations/DataGrip.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="管理 StarRocks">
<NavBoxPartItem>

- [部署使用](../administration/Build_in_docker.md)
- [运维操作](../administration/Scale_up_down.md)
- [资源隔离](../administration/monitor_manage_big_queries.md)
- [用户权限](../administration/privilege_overview.md)
- [数据恢复](../administration/Data_recovery.md)
- [性能调优](../administration/Query_planning.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="参考​">
<NavBoxPartItem>

- [SQL参考](../sql-reference/sql-statements/account-management/ALTER_USER.md)
- [函数参考](../sql-reference/sql-functions/date-time-functions/convert_tz.md)
- [数据类型](../sql-reference/sql-statements/data-types/TINYINT.md)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>

<NavBox>
<NavBoxPart title="常见问题​">
<NavBoxPartItem>

- [数据导入和导出](../faq/loading/Loading_faq.md)
- [部署运维](../faq/Deploy_faq.md)
- [SQL](../faq/Sql_faq.md)

</NavBoxPartItem>
</NavBoxPart>

<NavBoxPart title="性能测试​">
<NavBoxPartItem>

- [SSB 基准测试](../benchmarking/SSB_Benchmarking)
- [TPC-H 基准测试](../benchmarking/TPC-H_Benchmarking)

</NavBoxPartItem>
</NavBoxPart>
</NavBox>
