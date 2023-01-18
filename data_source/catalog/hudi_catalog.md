# Hudi catalog

本文介绍如何创建 Hudi catalog，以及需要做哪些相应的配置。

Hudi catalog 是一个外部数据目录 (external catalog)。StarRocks 2.4 及以上版本支持通过该目录直接查询 Apache Hudi 集群中的数据，无需数据导入或创建外部表。

## 使用说明

- StarRocks 支持查询如下格式的 Hudi 数据文件：Parquet 和 ORC。
- StarRocks 支持查询如下压缩格式的 Hudi 数据文件：gzip、Zstd、LZ4 和 Snappy。
- StarRocks 支持查询如下类型的 Hudi 数据：BOOLEAN、INTEGER、DATE、TIME、BIGINT、FLOAT、DOUBLE、DECIMAL、CHAR、VARCHAR、MAP 和 STRUCT。其中 MAP 和 STRUCT 从 2.5 版本开始支持。注意查询命中不支持的数据类型 ARRAY 会报错。
- StarRocks 支持查询 Copy on write 表和 Merge on read 表。有关这两种表的详细信息，请参见 [Table & Query Types](https://hudi.apache.org/docs/table_types)。
- StarRocks 支持的 Hudi 查询类型有 Snapshot Queries 和 Read Optimized Queries（Hudi 仅支持对 Merge on read 执行 Read Optimized Queries），暂不支持 Incremental Queries。有关 Hudi 查询类型的说明，请参见 [Table & Query Types](https://hudi.apache.org/docs/next/table_types/#query-types)。
- StarRocks 2.4 及以上版本支持创建 Hudi catalog，以及使用 [DESC](/sql-reference/sql-statements/Utility/DESCRIBE.md) 语句查看 Hudi 表结构。查看时，不支持的数据类型会显示成 `unknown`。

## 前提条件

在创建 Hudi catalog 前，您需要在 StarRocks 中进行相应的配置，以便能够访问 Hudi 的存储系统和元数据服务。StarRocks 当前支持的 Hudi 存储系统包括：HDFS、Amazon S3、阿里云对象存储 OSS 和腾讯云对象存储 COS；支持的 Hudi 元数据服务包括 Hive metastore 和 AWS Glue。具体配置步骤和 Hive catalog 相同，详细信息请参见 [Hive catalog](../catalog/hive_catalog.md#前提条件)。

## 创建 Hudi catalog

以上相关配置完成后，即可创建 Hudi catalog，语法和参数说明如下。

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
PROPERTIES ("key"="value", ...);
```

### catalog_name

Hudi catalog 的名称，必选参数。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 64 个字符。

### PROPERTIES

Hudi catalog 的属性，必选参数。当前支持配置如下属性：

- 根据 Hudi 使用的元数据服务类型来配置不同属性。
- 设置该 Hudi catalog 用于更新元数据缓存的策略。

#### 元数据服务属性配置

- 如 Hudi 使用 Hive metastore 作为元数据服务，则需要在创建 Hudi catalog 时设置如下属性：

  | **属性**            | **必选** | **说明**                                                     |
  | ------------------- | -------- | ------------------------------------------------------------ |
  | type                | 是       | 数据源类型，取值为 `hudi`。                                   |
  | hive.metastore.uris | 是       | Hive metastore 的 URI。格式为 `thrift://<Hive metastore的IP地址>:<端口号>`，端口号默认为 9083。 |

  > **注意**
  >
  > 查询前，需要将 Hive metastore 节点域名和其 IP 的映射关系配置到 **/etc/hosts** 路径中，否则查询时可能会因为域名无法识别而访问失败。

- 如 Hudi 使用 AWS Glue 作为元数据服务，则需要在创建 Hudi catalog 时设置如下属性：

  | **属性**                               | **必选** | **说明**                                                     |
  | -------------------------------------- | -------- | ------------------------------------------------------------ |
  | type                                   | 是       | 数据源类型，取值为 `hudi`。                                  |
  | hive.metastore.type                    | 是       | 元数据服务类型，取值为 `glue`。                              |
  | aws.hive.metastore.glue.aws-access-key | 是       | IAM 用户的 access key ID（即访问密钥 ID）。             |
  | aws.hive.metastore.glue.aws-secret-key | 是       | IAM 用户的 secret access key（即秘密访问密钥）。        |
  | aws.hive.metastore.glue.endpoint       | 是       | AWS Glue 服务所在地域的 endpoint。您可以根据 endpoint 与地域的对应关系进行查找，详情参见 [AWS Glue 端点和限额](https://docs.aws.amazon.com/zh_cn/general/latest/gr/glue.html)。 |

#### 元数据缓存更新属性配置

StarRocks 需要利用元数据服务中的 Hudi 表或分区的元数据（例如表结构）和存储系统中表或分区的数据文件元数据（例如文件大小）来生成查询执行计划。因此请求访问 Hudi 元数据服务和存储系统的时间直接影响了查询所消耗的时间。为了降低这种影响，StarRocks 提供元数据缓存能力，即将 Hudi 表或分区的元数据和其数据文件元数据缓存在 StarRocks 中并维护更新。

Hudi catalog 使用异步更新策略来更新所有缓存的 Hudi 元数据。有关该策略的原理说明，请参见[异步更新](../catalog/hive_catalog.md#异步更新)。大部分情况下，如下默认配置就能够保证较优的查询性能，无需调整。但如果您的 Hudi 数据更新相对频繁，也可以通过调节缓存更新和淘汰的间隔时间来适配 Hudi 数据的更新频率。

| **属性**                               | **必选** | **说明**                                                     |
| -------------------------------------- | -------- | ------------------------------------------------------------ |
| enable_hive_metastore_cache            | 否       | 是否缓存 Hudi 表或分区的元数据，取值包括：<ul><li>`true`：缓存，为默认值。</li><li>`false`：不缓存。</li></ul> |
| enable_remote_file_cache               | 否       | 是否缓存 Hudi 表或分区的数据文件元数据，取值包括：<ul><li>`true`：缓存，为默认值。</li><li>`false`：不缓存。</li></ul> |
| metastore_cache_refresh_interval_sec   | 否       | Hudi 表或分区元数据进行缓存异步更新的间隔时间。单位：秒，默认值为 `7200`，即 2 小时。 |
| remote_file_cache_refresh_interval_sec | 否       | Hudi 表或分区的数据文件元数据进行缓存异步更新的间隔时间。默认值为 `60`，单位：秒。 |
| metastore_cache_ttl_sec                | 否       | Hudi 表或分区元数据缓存自动淘汰的间隔时间。单位：秒，默认值为 `86400`，即 24 小时。 |
| remote_file_cache_ttl_sec              | 否       | Hudi 表或分区的数据文件元数据缓存自动淘汰的间隔时间。单位：秒，默认值为 `129600`，即 36 小时。 |

异步更新无法保证最新的 Hudi 数据。StarRocks 提供手动更新的方式帮助您获取最新数据。如果您想要在缓存更新间隔还未到来时更新数据，可以进行手动更新。例如，有一张 Hudi 表 `table1`，其包含 4 个分区：`p1`、`p2`、`p3` 和 `p4`。如当前  StarRocks 中只缓存了 `p1`、`p2` 和 `p3` 分区元数据和其数据文件元数据，那么手动更新有以下两种方式：

- 同时更新表 `table1` 所有缓存在 StarRocks 中的分区元数据和其数据文件元数据，即包括 `p1`、`p2` 和 `p3`。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- 更新指定分区元数据和其数据文件元数据缓存。

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

有关 REFRESH EXTERNAL TABEL 语句的参数说明和示例，请参见 [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH%20EXTERNAL%20TABLE.md)。

## 下一步

在创建完 Hudi catalog 并做完相关的配置后即可查询 Hudi 集群中的数据。详细信息，请参见[查询外部数据](../catalog/query_external_data.md)。

## 相关操作

- 如要查看有关创建 external catalog 的示例， 请参见 [CREATE EXTERNAL CATALOG](/sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md)。
- 如要看查看当前集群中的所有 catalog， 请参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md)。
- 如要删除指定 external catalog， 请参见 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP%20CATALOG.md)。
