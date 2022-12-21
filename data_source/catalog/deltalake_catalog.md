# Delta Lake catalog【公测中】

本文介绍如何创建 Delta Lake catalog，以及需要做哪些相应的配置。

Delta Lake catalog 是一个外部数据目录 (external catalog)。从 StarRocks 2.5 版本开始，您可以通过该目录直接查询 Delta Lake 中的数据，无需数据导入或创建外部表。

## **使用说明**

- StarRocks 支持查询 Parquet 格式的 Delta Lake 数据。
- StarRocks 不支持查询 BINARY 类型的 Delta Lake 数据。注意查询命中不支持的数据类型时会报错。
- StarRocks 2.4 及以上版本支持使用 [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) 语句查看 Delta Lake 表结构。查看时，不支持的数据类型会显示成 `unknown`。

## **前提条件**

在创建 Delta Lake catalog 前，您需要在 StarRocks 中进行相应的配置，以便能够访问 Delta Lake 的分布式文件系统或对象存储（以下简称“存储系统”）和元数据服务。StarRocks 当前支持的 Delta Lake 分存储系统包括：HDFS、Amazon S3、阿里云对象存储 OSS 和腾讯云对象存储 COS；支持的 Delta Lake 元数据服务包括 Hive metastore 和 AWS Glue。具体配置步骤和 Hive catalog 相同，详细信息请参见 [Hive catalog](../catalog/hive_catalog.md#前提条件)。

## **创建 Delta Lake catalog**

### 语法

以上相关配置完成后，即可创建 Delta Lake catalog，语法如下。

```SQL
CREATE EXTERNAL CATALOG <catalog_name> 
PROPERTIES ("key"="value", ...);
```

> **说明**
>
> StarRocks 不缓存 Delta Lake 元数据，因此不需要维护元数据更新。每次查询默认请求最新的 Delta Lake 数据。

### 参数说明

- `catalog_name`：Delta Lake catalog 的名称，必选参数。命名要求如下：
  - 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
  - 总长度不能超过 64 个字符。

- `PROPERTIES`：Delta Lake catalog 的属性，必选参数。Delta Lake 使用的元数据服务不同，该参数的配置也不同。

#### Hive metastore

如 Delta Lake 使用 Hive metastore 作为元数据服务，则需要在创建 Delta Lake catalog 时设置如下属性：

| **属性**            | **必选** | **说明**                                                     |
| ------------------- | -------- | ------------------------------------------------------------ |
| type                | 是       | 数据源类型，取值为 `deltalake`。                             |
| hive.metastore.uris | 是       | Hive metastore 的 URI。格式为 `thrift://<Hive metastore的IP地址>:<端口号>`，端口号默认为 9083。 |

> **注意**
>
> 查询前，需要将 Hive metastore 节点域名和其 IP 的映射关系配置到 **/****etc****/hosts** 路径中，否则查询时可能会因为域名无法识别而访问失败。

#### AWS Glue

如 Delta Lake 使用 AWS Glue 作为元数据服务，则需要在创建 Delta Lake catalog 时设置如下属性：

| **属性**                               | **必选** | **说明**                                                     |
| -------------------------------------- | -------- | ------------------------------------------------------------ |
| type                                   | 是       | 数据源类型，取值为 `deltalake`。                             |
| hive.metastore.type                    | 是       | 元数据服务类型，取值为 `glue`。                              |
| aws.hive.metastore.glue.aws-access-key | 是       | IAM 用户的 access key ID（即访问密钥 ID）。             |
| aws.hive.metastore.glue.aws-secret-key | 是       | IAM 用户的 secret access key（即秘密访问密钥）。        |
| aws.hive.metastore.glue.endpoint       | 是       | AWS Glue 服务所在地域的 endpoint。例如，服务在美国东部（俄亥俄州），那么 endpoint 为 `glue.us-east-2.amazonaws.com` 。您可以根据 endpoint 与地域的对应关系进行查找，详情参见 [AWS Glue 端点和限额](https://docs.aws.amazon.com/zh_cn/general/latest/gr/glue.html)。 |

## **下一步**

创建完 Delta Lake catalog 并做完相关的配置后即可查询 Delta Lake 数据。详细信息，请参见[查询外部数据](../catalog/query_external_data.md)。

## **相关操作**

- 如要查看有关创建 external catalog 的示例， 请参见 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md)。
- 如要查看当前集群中的所有 catalog， 请参见 [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md)。
- 如要删除指定 external catalog， 请参见 [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP%20CATALOG.md)。
