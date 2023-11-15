# CREATE EXTERNAL CATALOG

## 功能

该语句用于创建 External Catalog。创建后，无需数据导入或创建外部表即可查询外部数据。当前支持创建如下 External Catalog：

- [Hive catalog](/data_source/catalog/hive_catalog.md)：用于查询 Apache Hive™ 集群中的数据。
- [Iceberg catalog](/data_source/catalog/iceberg_catalog.md)：用于查询 Apache Iceberg 集群中的数据。
- [Hudi catalog](/data_source/catalog/hudi_catalog.md)：用于查询 Apache Hudi 集群中的数据。

使用该创建语句无权限限制。在创建 External Catalog 前，需要根据数据源的存储系统（如 Amazon S3）、元数据服务（如 Hive metastore）和认证方式（如 Kerberos）在 StarRocks 中做相应的配置。详细信息，请参见以上各个 External Catalog 文档中的「前提条件」小节 。

## 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
PROPERTIES ("key"="value", ...)
```

## 参数说明

| 参数         | 必选 | 说明                                                         |
| ------------ | ---- | ------------------------------------------------------------ |
| catalog_name | 是   | External catalog 的名称，命名要求如下：<ul><li>必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。</li><li>总长度不能超过 64 个字符。</li></ul> |
| PROPERTIES   | 是   | External catalog 的属性，不同的 external catalog 需要设置不同属性。详细配置信息，请参见 [Hive catalog](/data_source/catalog/hive_catalog.md)、[Iceberg catalog](/data_source/catalog/iceberg_catalog.md) 和 [Hudi catalog](/data_source/catalog/hudi_catalog.md)。 |

## 示例

示例一：创建名为 `hive1` 的 Hive catalog。

```SQL
CREATE EXTERNAL CATALOG hive1
PROPERTIES(
   "type"="hive", 
   "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例二：创建名为 `iceberg1` 的 Iceberg catalog。

```SQL
CREATE EXTERNAL CATALOG iceberg1
PROPERTIES(
    "type"="iceberg",
    "iceberg.catalog.type"="HIVE",
    "iceberg.catalog.hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

示例三：创建名为 `hudi1` 的 Hudi catalog。

```SQL
CREATE EXTERNAL CATALOG hudi1
PROPERTIES(
    "type"="hudi",
    "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

## 相关操作

- 如要查看所有 Catalog，参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md)。
- 如要删除 External Catalog，参见 [DROP CATALOG](/sql-reference/sql-statements/data-definition/DROP_CATALOG.md)。
