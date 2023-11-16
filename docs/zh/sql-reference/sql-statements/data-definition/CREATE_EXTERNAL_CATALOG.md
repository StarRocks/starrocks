---
displayed_sidebar: "Chinese"
---

# CREATE EXTERNAL CATALOG

## 功能

该语句用于创建 External Catalog。创建后，无需数据导入或创建外部表即可查询外部数据。当前支持创建如下 External Catalog：

- [Hive catalog](../../../data_source/catalog/hive_catalog.md)：用于查询 Apache Hive™ 集群中的数据。
- [Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md)：用于查询 Apache Iceberg 集群中的数据。
- [Hudi catalog](../../../data_source/catalog/hudi_catalog.md)：用于查询 Apache Hudi 集群中的数据。
- [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md)：用于查询 Delta Lake 数据。
- [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md)：用于查询 JDBC 数据源的数据。

> **注意**
>
> - 在 3.0 及以后版本，使用该语句需要有 System 级 CREATE EXTERNAL CATALOG 权限。
> - 在创建 External Catalog 前，需要根据数据源的存储系统（如 AWS S3）、元数据服务（如 Hive metastore）和认证方式（如 Kerberos）在 StarRocks 中做相应的配置。详细信息，请参见以上各个 External Catalog 文档中的「前提条件」小节。

## 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

## 参数说明

| 参数         | 必选 | 说明                                                         |
| ------------ | ---- | ------------------------------------------------------------ |
| catalog_name | 是   | External catalog 的名称，命名要求如下：<ul><li>必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。</li><li>总长度不能超过 1023 个字符。</li><li>Catalog 名称大小写敏感。</li></ul> |
| comment      | 否   | External catalog 的描述。 |
| PROPERTIES   | 是   | External catalog 的属性，不同的 external catalog 需要设置不同属性。详细配置信息，请参见 [Hive catalog](../../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md)、[Hudi catalog](../../../data_source/catalog/hudi_catalog.md)、[Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md) 和 [JDBC Catalog](../../../data_source/catalog/jdbc_catalog.md)。 |

## 示例

示例一：创建名为 `hive_metastore_catalog` 的 Hive catalog。其对应的 Hive 集群使用 Hive metastore 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG hive_metastore_catalog
PROPERTIES(
   "type"="hive", 
   "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例二：创建名为 `hive_glue_catalog` 的 Hive catalog。其对应的 Hive 集群使用 AWS Glue 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG hive_glue_catalog
PROPERTIES(
    "type"="hive", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

示例三：创建名为 `iceberg_metastore_catalog` 的 Iceberg catalog。其对应的 Iceberg 集群使用 Hive metastore 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG iceberg_metastore_catalog
PROPERTIES(
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "iceberg.catalog.hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例四：创建名为 `iceberg_glue_catalog` 的 Iceberg catalog。其对应的 Iceberg 集群使用  AWS Glue 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG iceberg_glue_catalog
PROPERTIES(
    "type"="iceberg", 
    "iceberg.catalog.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

示例五：创建名为 `hudi_metastore_catalog` 的 Hudi catalog。其对应的 Hudi 集群使用 Hive metastore 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG hudi_metastore_catalog
PROPERTIES(
    "type"="hudi",
    "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例六：创建名为 `hudi_glue_catalog` 的 Hudi catalog。其对应的 Hudi 集群使用 AWS Glue 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG hudi_glue_catalog
PROPERTIES(
    "type"="hudi", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

示例七：创建名为 `delta_metastore_catalog` 的 Delta Lake catalog。其对应的 Delta Lake 使用 Hive metastore 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG delta_metastore_catalog
PROPERTIES(
    "type"="deltalake",
    "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例八：创建名为 `delta_glue_catalog` 的 Delta Lake catalog。其对应的 Delta Lake 使用 AWS Glue 作为元数据服务。

```SQL
CREATE EXTERNAL CATALOG delta_glue_catalog
PROPERTIES(
    "type"="deltalake", 
    "hive.metastore.type"="glue",
    "aws.hive.metastore.glue.aws-access-key"="xxxxxx",
    "aws.hive.metastore.glue.aws-secret-key"="xxxxxxxxxxxx",
    "aws.hive.metastore.glue.endpoint"="https://glue.x-x-x.amazonaws.com"
);
```

## 相关操作

- 如要查看所有 Catalog，参见 [SHOW CATALOGS](../../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md)。
- 如要查看某个 External Catalog 的创建语句，参见 [SHOW CREATE CATALOG](../../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md)。
- 如要删除 External Catalog，参见 [DROP CATALOG](../../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md)。
