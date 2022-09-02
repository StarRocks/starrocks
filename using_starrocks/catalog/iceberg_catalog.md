# Iceberg catalog

本文介绍如何创建 Iceberg catalog 以及需要做哪些相应的配置。

Iceberg catalog 是一个外部数据目录 (external catalog)。在 StarRocks 中，您可以通过该目录直接查询 Apache Iceberg 集群中的数据，无需数据导入或创建外部表。在查询数据时，Iceberg catalog 会用到以下两个 Iceberg 组件：

- **元数据服务**：用于将 Iceberg 元数据暴露出来供 StarRocks 的 leader FE 进行查询规划。
- **存储系统**：用于存储 Iceberg 数据。数据文件以不同的格式存储在分布式文件系统或对象存储系统中。当 leader FE 将生成的查询计划分发给各个 BE 后，各个 BE 会并行扫描存储系统中的目标数据，并执行计算返回查询结果。

## 使用限制

- StarRocks 当前支持查询如下格式的 Iceberg 数据：Parquet 、ORC、gzip、Zstd、LZ4 和 Snappy。
- StarRocks 当前支持查询如下类型的 Iceberg 数据：BOOLEAN、INT、LONG、FLOAT、DOUBLE、DECIMAL(P, S)、DATE、TIME、TIMESTAMP、STRING、UUID、FIXED(L) 和 BINARY。注意查询命中不支持的数据类型（TIMESTAMPTZ、STRUCT、LIST 和 MAP）会报错。
- StarRocks 当前支持查询 Versions 1 表 (Analytic Data Tables) 。暂不支持查询 Versions 2 表 (Row-level Deletes) 。有关两种表的详细信息，请参见 [Iceberg Table Spec](https://iceberg.apache.org/spec/)。
- StarRocks 2.4 及以上版本支持创建 Iceberg catalog，以及使用 [DESCRIBE](/sql-reference/sql-statements/Utility/DESCRIBE.md) 语句查看 Iceberg 表结构。查看时，不支持的数据类型会显示成`unknown`。

## 前提条件

在创建 Iceberg catalog 前，您需要根据 Iceberg 使用的存储系统、元数据服务和认证方式在 StarRocks 中进行相应的配置。StarRocks 当前支持的 Iceberg 存储系统包括：HDFS、Amazon S3、阿里云对象存储 OSS 和腾讯云对象存储 COS；支持的 Iceberg 元数据服务为 Hive metastore。具体配置步骤和 Hive catalog 相同，详细信息请参见 [Hive catalog](../catalog/hive_catalog.md#前提条件)。

## 创建 Iceberg catalog

### 语法

以上相关配置完成后，即可创建 Iceberg catalog，语法如下。

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

> 注意：创建完成后，需要将 Hive metastore 节点域名和其 IP 的映射关系配置到 **/etc/hosts** 路径中，否则查询时可能会因为域名无法识别而访问失败。

### 参数说明

- `catalog_name`：Iceberg catalog 的名称，必选参数。命名要求如下：
  - 必须由字母(a-z或A-Z)、数字(0-9)或下划线(_)组成，且只能以字母开头。
  - 总长度不能超过 64 个字符。

- `PROPERTIES`：Iceberg catalog 的属性，必选参数。在 Apache Iceberg 中，也存在 [catalog](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)，其作用是保存 Iceberg 表和其存储路径的映射关系。如 Iceberg 中使用的 catalog 类型不同，那么`PROPERTIES`的配置也会有差异。StarRocks 当前支持 Iceberg 中的两种 catalog：HiveCatalog 和 custom catalog。

#### HiveCatalog

如在 Iceberg 中使用 HiveCatalog，则需要在创建 Iceberg catalog 时设置如下属性：

| **属性**               | **必选** | **说明**                                                     |
| ---------------------- | -------- | ------------------------------------------------------------ |
| type                   | 是       | 数据源类型，取值为`iceberg`。                                |
| starrocks.catalog.type | 是       | Iceberg 中 catalog 的类型。如使用 HiveCatalog， 则设置该参数为`HIVE`。 |
| hive.metastore.uris    | 是       | Hive metastore 的 URI。格式为`thrift://<Hive metastore的IP地址>:<端口号>`，端口号默认为 9083。 |

#### Custom catalog

如在 Iceberg 中使用 custom catalog，则您需要在 StarRocks 中开发一个 custom catalog 类，并实现相关接口。Custom catalog 类需要继承抽象类 BaseMetastoreCatalog 。有关 custom catalog 开发和相关接口实现的具体信息，参考 [IcebergHiveCatalog](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/external/iceberg/IcebergHiveCatalog.java)。开发完成后，您需要将 custom catalog 及其相关文件打包并放到所有 FE 节点的 **fe/lib** 路径下，然后重启所有 FE 节点，以便 FE 识别这个类。

> 注意：custom catalog 类名不能与 StarRocks 中已存在的类名重复。

以上操作完成后即可创建 Iceberg catalog 并配置其相关属性，具体如下：

| **属性**               | **必选** | **说明**                                                     |
| ---------------------- | -------- | ------------------------------------------------------------ |
| type                   | 是       | 数据源类型，取值为`iceberg`。                                |
| starrocks.catalog.type | 是       | Iceberg 中 catalog 的类型。如使用 custom catalog，则设置该参数为`CUSTOM`。 |
| iceberg.catalog-impl   | 是       | Custom catalog 的全限定类名。FE 会根据该类名查找开发的 custom catalog。如果您在 custom catalog 中自定义了配置项，且希望在查询外部数据时这些配置项能生效，您可以在创建 Iceberg catalog 时将这些配置项以键值对的形式添加到 SQL 语句的 `PROPERTIES` 中。 |

## 元数据同步

StarRocks 不缓存 Iceberg 元数据，因此不需要维护元数据更新。每次查询默认请求最新的 Iceberg 数据。

## 下一步

在创建完 Iceberg catalog 并做完相关的配置后即可查询 Iceberg 集群中的数据。详细信息，请参见[查询外部数据](../catalog/query_external_data.md)。

## 相关操作

- 如要查看有关创建 external catalog 的示例， 请参见 [CREATE EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md)。
- 如要看查看当前集群中的所有 catalog， 请参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md)。
- 如要删除指定 external catalog， 请参见 [DROP EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/DROP%20CATALOG.md)。
