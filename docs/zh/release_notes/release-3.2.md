---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.2

## v3.2.0-RC01

发布日期：2023 年 11 月 15 日

### 新增特性

#### 存算分离架构

- 支持[主键模型表](../table_design/table_types/primary_key_table.md)的索引在本地磁盘持久化。
- 支持 Data Cache 在多磁盘间均匀分布。

#### 数据湖分析 

- 支持在 [Hive Catalog](../data_source/catalog/hive_catalog.md) 中创建、删除数据库以及 Managed Table，支持使用 INSERT 或 INSERT OVERWRITE 导出数据到 Hive 的 Managed Table。
- 支持 [Unified Catalog](../data_source/catalog/unified_catalog.md)。如果同一个 Hive Metastore 或 AWS Glue 元数据服务包含多种表格式（Hive、Iceberg、Hudi、Delta Lake 等），则可以通过 Unified Catalog 进行统一访问。

#### 导入、导出和存储

- 使用表函数 [FILES()](../sql-reference/sql-functions/table-functions/files.md) 进行数据导入新增以下功能：
  - 支持导入 Azure 和 GCP 中的 Parquet 或 ORC 格式文件的数据。
  - 支持 `columns_from_path` 参数，能够从文件路径中提取字段信息。
  - 支持导入复杂类型（JSON、ARRAY、MAP 及 STRUCT）的数据。
- 支持 dict_mapping 列属性，能够极大地方便构建全局字典中的数据导入过程，用以加速计算精确去重等。
- 支持使用 INSERT INTO FILES() 语句将数据导出至 AWS S3 或 HDFS 中的 Parquet 格式的文件。有关详细说明，请参见[使用 INSERT INTO FILES 导出数据](../unloading/unload_using_insert_into_files.md)。

#### SQL 语句和函数

新增如下函数：

- 字符串函数：substring_index、url_extract_parameter、url_encode、url_decode、translate
- 日期函数：dayofweek_iso、week_iso、quarters_add、quarters_sub、milliseconds_add、milliseconds_sub、date_diff、jodatime_format、str_to_jodatime、to_iso8601、to_tera_date、to_tera_timestamp
- 模糊/正则匹配函数：regexp_extract_all
- hash 函数：xx_hash3_64
- 聚合函数：approx_top_k
- 窗口函数：cume_dist、percent_rank、session_number
- 工具函数：dict_mapping、get_query_profile

#### 权限

支持通过 Apache Ranger 实现访问控制，提供更高层次的数据安全保障，并且允许复用外部数据源对应的 Ranger Service。StarRocks 集成 Apache Ranger 后可以实现以下权限控制方式：

- 访问 StarRocks 内表、外表或其他对象时，可根据在 Ranger 中创建的 StarRocks Service 配置的访问策略来进行访问控制。
- 访问 External Catalog 时，也可以复用对应数据源原有的 Ranger service（如 Hive Service）来进行访问控制（对于导出数据到 Hive，当前暂未提供相应的权限控制策略。）。

更多内容，请参阅 [使用 Apache Ranger 管理权限](../administration/ranger_plugin.md)。

### 功能优化

#### 物化视图 

异步物化视图

- 刷新物化视图：

  自动刷新：当创建物化视图涉及的表、视图及视图内涉及的表、物化视图发生 Schema Change 或 Swap 操作后，物化视图可以进行自动刷新。

- 物化视图的可观测性：

  物化视图支持 Query Dump。

- 物化视图的刷新默认开启中间结果落盘，降低刷新的内存消耗。
- 数据一致性：

  - 创建物化视图时，添加了 `query_rewrite_consistency` 属性。该属性允许用户基于一致性检查结果定义查询改写规则。
  - 创建物化视图时，添加了 `force_external_table_query_rewrite` 属性。该属性用于定义是否为外表物化视图强制开启查询重写。

  有关详细信息，请参见[CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md)。

- 增加分区列一致性检查：当创建分区物化视图时，如物化视图的查询中涉及带分区的窗口函数，则窗口函数的分区列需要与物化视图的分区列一致。

#### 导入、导出和存储  

- 优化主键模型表持久化索引功能，优化内存使用逻辑，同时降低 I/O 的读写放大。
- 主键模型表支持本地多块磁盘间数据均衡。
- 分区中数据可以随着时间推移自动进行降冷操作（List 分区方式暂不支持）。相对原来的设置，更方便进行分区冷热管理。有关详细信息，请参见[设置数据的初始存储介质、自动降冷时间](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#设置数据的初始存储介质自动降冷时间和副本数)。
- 主键模型表数据写入时的 Publish 过程由异步改为同步，导入作业成功返回后数据立即可见。有关详细信息，请参见 [enable_sync_publish](../administration/FE_configuration.md#enable_sync_publish)。

#### 查询

- Metabase 和 Superset 兼容性提升，支持集成 External Catalog。

#### SQL 语句和函数

- array_agg 支持使用 DISTINCT 关键词。

### 开发者工具

- 异步物化视图支持 Trace Query Profile，用于分析物化视图透明改写的场景。

### 兼容性变更

#### 行为变更

待补充。

#### 配置参数

- 新增 Data Cache 相关参数。

#### 系统变量

待补充。

### 问题修复

修复了如下问题：

- 调用 libcurl 会引起 BE Crash。[#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 如果 Schema Change 执行时间过长，会因为 Tablet 版本被垃圾回收而失败。[#31376](https://github.com/StarRocks/starrocks/pull/31376)
- 通过文件外部表无法读取存储在 MinIO 上的 Parquet 文件。[#29873] (https://github.com/StarRocks/starrocks/pull/29873)
- `information_schema.columns` 视图中无法正确显示 ARRAY、MAP、STRUCT 类型的字段。[#33431](https://github.com/StarRocks/starrocks/pull/33431)
- BINARY 或 VARBINARY 类型在 `information_schema.columns` 视图里面的 `DATA_TYPE` 和 `COLUMN_TYPE` 显示为 `unknown`。[#32678](https://github.com/StarRocks/starrocks/pull/32678)

### 升级注意事项

待补充。
