# Hudi catalog

本文介绍如何创建 Hudi catalog，以及需要做哪些相应的配置。

Hudi catalog 是一个外部数据目录 (external catalog)。在 StarRocks 中，您可以通过该目录直接查询 Apache Hudi 集群中的数据，无需数据导入或创建外部表。在查询数据时，StarRocks 会用到以下两个 Hudi 组件：

- **元数据服务**：用于将 Hudi 元数据暴露出来供 StarRocks 的 FE 进行查询规划。
- **存储系统**：用于存储 Hudi 表数据。数据文件以不同的格式存储在分布式文件系统或对象存储系统中。当 FE 将生成的查询计划分发给各个 BE 后，各个 BE 会并行扫描 Hudi 存储系统中的目标数据，并执行计算返回查询结果。

## 使用限制

- StarRocks 支持查询如下格式的 Hudi 数据文件：Parquet 和 ORC。
- StarRocks 支持查询如下压缩格式的 Hudi 数据文件：gzip、Zstd、LZ4 和 Snappy。
- StarRocks 支持查询如下类型的 Hudi 数据：BOOLEAN、INT、DATE、TIME、BIGINT、FLOAT、DOUBLE、DECIMAL、CHAR 和 VARCHAR。注意查询命中不支持的数据类型（ARRAY、MAP 和 STRUCT）会报错。
- StarRocks 支持查询 Copy on write 表。暂不支持查询 Merge on read 表。有关这两种表的详细信息，请参见 [Table & Query Types](https://hudi.apache.org/docs/table_types)。
- StarRocks 2.4 及以上版本支持创建 Hudi catalog，以及使用 [DESC](/sql-reference/sql-statements/Utility/DESCRIBE.md) 语句查看 Hudi 表结构。查看时，不支持的数据类型会显示成`unknown`。

## 前提条件

在创建 Hudi catalog 前，您需要StarRocks 中进行相应的配置，以便能够访问 Hudi 的存储系统和元数据服务。StarRocks 当前支持的 Hudi 存储系统包括：HDFS、Amazon S3、阿里云对象存储 OSS 和腾讯云对象存储 COS；支持的 Hudi 元数据服务为 Hive metastore。具体配置步骤和 Hive catalog 相同，详细信息请参见 [Hive catalog](../catalog/hive_catalog.md#前提条件)。

## 创建 Hudi catalog

以上相关配置完成后，即可创建 Hudi catalog，语法如下。

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

参数说明：

- `catalog_name`：Hudi catalog 的名称，必选参数。<br>命名要求如下：
  - 必须由字母(a-z或A-Z)、数字(0-9)或下划线(_)组成，且只能以字母开头。
  - 总长度不能超过 64 个字符。

- `PROPERTIES`：Hudi catalog 的属性，必选参数。<br>支持配置如下：

    | **参数**            | **必选** | **说明**                                                     |
    | ------------------- | -------- | ------------------------------------------------------------ |
    | type                | 是       | 数据源类型，取值为`hudi`。                                   |
    | hive.metastore.uris | 是       | Hive metastore 的 URI。格式为 `thrift://<Hive metastore的IP地址>:<端口号>`，端口号默认为 9083。 |

> 注意
>
> - 使用该创建语句无权限限制。
> - 查询前，需要将 Hive metastore 节点域名和其 IP 的映射关系配置到 **/etc/hosts** 路径中，否则查询时可能会因为域名无法识别而访问失败。

## 元数据异步更新

StarRocks 需要利用 Hudi 表的元数据来进行查询规划，因此请求访问 Hudi 元数据服务的时间直接影响了查询所消耗的时间。为了降低这种影响，StarRocks 提供了元数据同步功能，即将 Hudi 表元数据（包括分区统计信息和分区的数据文件信息）缓存在 StarRocks 中并维护更新。当前支持的同步方式为异步更新。

### 原理

如查询命中 Hudi 表的某个分区，StarRocks 会自动异步缓存该分区的元数据。缓存的元数据采用的是“懒更新策略”，即如果查询命中该分区，且距离上一次更新已经超过默认间隔时间，那么 StarRock 会异步更新缓存分区元数据，否则不会更新。更新的默认间隔时间由`hive_meta_cache_refresh_interval_s`参数控制，默认值为 `7200`，单位：秒。您可在每个 FE 的 **fe.conf** 文件中设置该参数，设置后重启各个 FE 生效。

如超过默认间隔时间，该分区元数据依旧没有更新，则默认缓存的分区元数据失效。在下次查询时，会重新缓存该分区元数据。元数据缓存失效的时间由`hive_meta_cache_ttl_s`参数控制，默认值为`86400`，单位：秒。您可在每个 FE 的 **fe.conf** 文件中设置该参数，设置后重启各个 FE 生效。

### 示例

有一张 Hudi 表`table1`，其包含 4 个分区：`p1`、`p2`、`p3`和`p4`。如查询命中分区 `p1`，那么 StarRocks 会自动异步缓存 `p1` 的元数据。如维护更新的间隔时间为 1 小时，则后续更新有以下几种情况：

- 如查询命中`p1`，且当前时间距离上一次更新超过 1 小时，StarRock 会异步更新缓存的`p1`元数据。
- 如查询命中`p1`，且当前时间距离上一次更新没有超过 1 小时，StarRock 不会异步更新缓存的`p1`元数据。

### 手动更新

要查询最新的 Hudi 数据，需保证 StarRocks 缓存的 Hudi 元数据也更至最新。如当前时间距离上一次更新还没有超过默认间隔时间，则可手动更新元数据后再进行查询，具体如下：

- 若 Hudi 表结构发生变更（例如增减分区或增减列），可执行如下语句将该变更同步到 StarRocks 中。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- 若 Hudi 表中的某些分区发生数据更新（例如数据导入），可执行如下语句将该变更同步到 StarRocks 中。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

有关 REFRESH EXTERNAL TABEL 语句的参数说明和示例，请参见 [REFRESH EXTERNAL TABEL](/sql-reference/sql-statements/data-definition/REFRESH%20EXTERNAL%20TABLE.md)。注意只有拥有`ALTER_PRIV`权限的用户才可以手动更新元数据。

## 下一步

在创建完 Hudi catalog 并做完相关的配置后即可查询 Hudi 集群中的数据。详细信息，请参见[查询外部数据](../catalog/query_external_data.md)。

## 相关操作

- 如要查看有关创建 external catalog 的示例， 请参见 [CREATE EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md)。
- 如要看查看当前集群中的所有 catalog， 请参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md)。
- 如要删除指定 external catalog， 请参见 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP%20CATALOG.md)。
