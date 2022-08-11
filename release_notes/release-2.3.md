# StarRocks version 2.3

## 2.3.0

发布日期： 2022 年 7 月 29 日

### 新增特性

- 主键模型支持完整的 DELETE WHERE 语法。相关文档，请参见 [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#delete-与主键模型)。

- 主键模型支持持久化主键索引，基于磁盘而不是内存维护索引，大幅降低内存使用。相关文档，请参见[主键模型](../table_design/Data_model.md#使用说明-3)。

- 全局低基数字典优化支持实时数据导入，实时场景下字符串数据的查询性能提升一倍。

- 支持以异步的方式执行 CTAS，并将结果写入新表。相关文档，请参见 [CREATE TABLE AS SELECT](../sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT.md)。

- 资源组相关功能：

  - 支持监控资源组：可在审计日志中查看查询所属的资源组，并通过相关 API 获取资源组的监控信息。相关文档，请参见[监控指标](../administration/Monitor_and_Alert.md#监控指标)。

  - 支持限制大查询的 CPU、内存、或 I/O 资源；可通过匹配分类器将查询路由至资源组，或者设置会话变量直接为查询指定资源组。相关文档，请参见[资源隔离](../administration/Resource_Group.md)。

- 支持 JDBC 外表，可以轻松访问Oracle、PostgreSQL、MySQL、SQLServer、ClickHouse 等数据库，并且查询时支持谓词下推，提高查询性能。相关文档，请参见 [更多数据库（JDBC）的外部表](../using_starrocks/External_table.md#更多数据库jdbc的外部表)。

- 【Preview】发布全新数据源 Connector 框架，支持创建外部数据目录（External Catalog），从而无需创建外部表，即可直接查询 Apache Hive™。相关文档，请参见[使用 Catalog 管理内部和外部数据](../using_starrocks/Manage_data.md)。

- 新增如下函数：
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)
  - [ntile](../using_starrocks/Window_function.md)
  - [bitmap_union_count](../sql-reference/sql-functions/bitmap-functions/bitmap_union_count.md)、[base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md)、[array_to_bitmap](../sql-reference/sql-functions/array-functions/array_to_bitmap.md)
  - [week](../sql-reference/sql-functions/date-time-functions/week.md)、[time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md)

### 功能优化

- 优化合并机制（Compaction），对较大的元数据进行合并操作，避免因数据高频更新而导致短时间内元数据挤压，占用较多磁盘空间。

- 优化导入 Parquet 文件和压缩文件格式的性能。

- 优化创建物化视图的性能，在部分场景下创建速度提升近 10 倍。

- 优化算子性能：
  - TopN，sort 算子。
  - 包含函数的等值比较运算符下推至 scan 算子时，支持使用 Zone Map 索引。

- 优化 Apache Hive™ 外表功能。
  - 当 Apache Hive™ 的数据存储采用 Parquet、ORC、CSV 格式时，支持 Hive 表执行 ADD COLUMN、REPLACE COLUMN 等表结构变更（Schema Change）。相关文档，请参见 [Hive 外部表](../using_starrocks/External_table.md#hive-外表)。
  - 支持 Hive 资源修改 `hive.metastore.uris`。相关文档，请参见 [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER%20RESOURCE.md)。

- 优化 Apache Iceberg 外表功能，创建 Iceberg 资源时支持使用自定义目录（Catalog）。相关文档，请参见 [Apache Iceberg 外表](../using_starrocks/External_table.md#apache-iceberg-外表)。

- 优化 Elasticsearch 外表功能，支持取消探测 Elasticsearch 集群数据节点的地址。相关文档，请参见 [Elasticsearch 外部表](../using_starrocks/External_table.md#elasticsearch-外部表)。

- 当 sum() 中输入的值为 STRING 类型且为数字时，则自动进行隐式转换。

- year、month、day 函数支持 DATE 数据类型。

### Bug 修复

修复了如下 Bug：

- Tablet 过多导致 CPU 占用率过高的问题。
- 导致出现"fail to prepare tablet reader"报错提示的问题。
- FE 重启失败的问题。[#5642](https://github.com/StarRocks/starrocks/issues/5642 )、[#4969](https://github.com/StarRocks/starrocks/issues/4969 )、[#5580](https://github.com/StarRocks/starrocks/issues/5580)
- CTAS 语句中调用 JSON 函数时报错的问题。[#6498](https://github.com/StarRocks/starrocks/issues/6498)

### 其他

- 【Preview】提供集群管理工具 StarGo，提供集群部署、启停、升级、回滚、多集群管理等多种能力。相关文档，请参见[通过 StarGo 部署 StarRocks 集群](../administration/stargo.md)。
- 支持在 AWS 上使用 CloudFormation 快速创建 StarRocks 集群。相关文档，请参见[在 AWS 上使用 AWS CloudFormation 部署 StarRocks 集群](../administration/AWS_cloudformation.md)。
- 部署或者升级至 2.3 版本，默认开启 Pipeline 执行引擎，预期在高并发小查询、复杂大查询场景下获得明显的性能优势。如果使用 2.3 版本时遇到明显的性能回退，则可以通过设置 `SET GLOBAL enable_pipeline_engine = false;`，关闭 Pipeline 执行引擎。
- [SHOW GRANTS](../sql-reference/sql-statements/account-management/SHOW%20GRANTS.md) 语句兼容 MySQL语法，显示授权 GRANT 语句。
