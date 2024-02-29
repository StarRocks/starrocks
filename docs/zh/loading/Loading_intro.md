---
displayed_sidebar: "Chinese"
toc_max_heading_level: 4
---

# 导入总览

数据导入是指将原始数据按照业务需求进行清洗、转换、并加载到 StarRocks 中的过程，从而可以在 StarRocks  系统中进行极速统一的数据分析。

StarRocks 通过导入作业实现数据导入。每个导入作业都有一个标签 (Label)，由用户指定或系统自动生成，用于标识该导入作业。每个标签在一个数据库内都是唯一的，仅可用于一个成功的导入作业。一个导入作业成功后，其标签不可再用于提交其他导入作业。只有失败的导入作业的标签，才可再用于提交其他导入作业。这一机制可以保证任一标签对应的数据最多被导入一次，即实现“至多一次 (At-Most-Once) ”语义。

StarRocks 中所有导入方式都提供原子性保证，即同一个导入作业内的所有有效数据要么全部生效，要么全部不生效，不会出现仅导入部分数据的情况。这里的有效数据不包括由于类型转换错误等数据质量问题而被过滤掉的数据。

StarRocks 提供两种访问协议用于提交导入作业：MySQL 协议和 HTTP 协议。不同的导入方式支持的访问协议有所不同，具体请参见本文“[导入方式](../loading/Loading_intro.md#导入方式)”章节。

> **注意**
>
> 导入操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

## 支持的数据类型

StarRocks 支持导入所有数据类型。个别数据类型的导入可能会存在一些限制，具体请参见[数据类型](../sql-reference/sql-statements/data-types/BIGINT.md)。



## 导入方式

StarRocks 提供 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)、 [Routine Load](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)、[Spark Load](../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) 和 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) 多种导入方式，满足您在不同业务场景下的数据导入需求。

| 导入方式            | 数据源                                                                                          | 业务场景                                                                                                     | 数据量（单作业）      | 数据格式                                            | 同步模式    | 协议   |
| ------------------ | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------ | ------------------------------------------------- | ---------- | ------ |
| Stream Load        |<ul><li>本地文件</li><li>流式数据</li></ul>                                                        | 通过 HTTP 协议导入本地文件、或通过程序导入数据流。                                                                 | 10 GB 以内          |<ul><li>CSV</li><li>JSON</li></ul>                 | 同步       | HTTP  |
| Broker Load        |<ul><li>HDFS</li><li>Amazon S3</li><li>Google GCS</li><li>Microsoft Azure Storage</li><li>阿里云 OSS</li><li>腾讯云 COS</li><li>华为云 OBS</li><li>其他兼容 S3 协议的对象存储（如 MinIO）</li></ul> | 从 HDFS 或外部云存储系统导入数据。                                                                               | 数十到数百 GB        |<ul><li>CSV</li><li>Parquet</li><li>ORC</li></ul> | 异步        | MySQL |
| Routine Load       | Apache Kafka®                                                                                 | 从 Kafka 实时地导入数据流。                                                                             | 微批导入 MB 到 GB 级 |<ul><li>CSV</li><li>JSON</li><li>Avro（3.0.1 版本之后支持）</li></ul>          | 异步     | MySQL |
| Spark Load         | <ul><li>HDFS</li><li>Hive</li></ul>                                                            | <ul><li>通过 Apache Spark™ 集群初次从 HDFS 或 Hive 迁移导入大量数据。</li><li>需要做全局数据字典来精确去重。</li></ul> | 数十 GB 到 TB级别    |<ul><li>CSV</li><li>ORC（2.0 版本之后支持）</li><li>Parquet（2.0 版本之后支持）</li></ul>       | 异步     | MySQL |
| INSERT INTO SELECT |<ul><li>StarRocks 表</li><li>外部表</li><li>AWS S3</li><li>HDFS</li></ul>**注意**<br />从 AWS S3 或 HDFS 导入数据时，只支持导入 Parquet 和 ORC 格式的数据。                                                    |<ul><li>外表导入。</li><li>StarRocks 数据表之间的数据导入。</li></ul>                                              | 跟内存相关           | StarRocks 表                                     | 同步        | MySQL |
| INSERT INTO VALUES |<ul><li>程序</li><li>ETL 工具</li></ul>                                                          |<ul><li>单条批量小数据量插入。</li><li>通过 JDBC 等接口导入。</li></ul>                                             | 简单测试用           | SQL                                              | 同步        | MySQL |

您可以根据业务场景、数据量、数据源、数据格式和导入频次等来选择合适的导入方式。另外，在选择导入方式时，可以注意以下几点：

- 从 Kafka 导入数据时，推荐使用 [Routine Load](../loading/RoutineLoad.md) 实现导入。但是，如果导入过程中有复杂的多表关联和 ETL 预处理，建议先使用 Apache Flink® 从 Kafka 读取数据并对数据进行处理，然后再通过 StarRocks 提供的标准插件 [flink-connector-starrocks](../loading/Flink-connector-starrocks.md) 把处理后的数据导入到 StarRocks 中。

- 从 Hive、Iceberg、Hudi、Delta Lake 导入数据时，推荐创建 [Hive catalog](../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../data_source/catalog/iceberg_catalog.md)、[Hudi Catalog](../data_source/catalog/hudi_catalog.md)、[Delta Lake Catalog](../data_source/catalog/deltalake_catalog.md)，然后使用 [INSERT](../loading/InsertInto.md) 实现导入。

- 从另外一个 StarRocks 集群或从 Elasticsearch 导入数据时，推荐创建 [StarRocks 外部表](../data_source/External_table.md#starrocks-外部表)或 [Elasticsearch 外部表](../data_source/External_table.md#deprecated-elasticsearch-外部表)，然后使用 [INSERT](../loading/InsertInto.md) 实现导入。或者，您也可以通过 [DataX](../integrations/loading_tools/DataX-starrocks-writer.md) 实现导入。

  > **注意**
  >
  > StarRocks 外表只支持数据写入，不支持数据读取。

- 从 MySQL 导入数据时，推荐创建 [MySQL 外部表](../data_source/External_table.md#deprecated-mysql-外部表)、然后使用 [INSERT](../loading/InsertInto.md) 实现导入。或者，您也可以通过 [DataX](../integrations/loading_tools/DataX-starrocks-writer.md) 实现导入。如果要导入实时数据，建议您参考 [从 MySQL 实时同步](../loading/Flink_cdc_load.md) 实现导入。

- 从 Oracle、PostgreSQL 或 SQL Server 等数据源导入数据时，推荐创建 [JDBC 外部表](../data_source/External_table.md#更多数据库jdbc的外部表)、然后使用 [INSERT](../loading/InsertInto.md) 实现导入。或者，您也可以通过 [DataX](../integrations/loading_tools/DataX-starrocks-writer.md) 实现导入。

下图详细展示了在各种数据源场景下，应该选择哪一种导入方式。

![数据源与导入方式关系图](../assets/4.1-3.png)


