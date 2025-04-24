---
displayed_sidebar: docs
---
import Experimental from '../../_assets/commonMarkdown/_experimental.mdx'

# Kudu catalog

<Experimental />

StarRocks 从 v3.3 开始支持 Kudu catalog。

Kudu catalog 是一种 external catalog，允许您在不导入数据的情况下查询 Apache Kudu 的数据。

此外，您还可以基于 Kudu catalog 使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 直接转换和导入 Kudu 的数据。

为了确保在您的 Kudu 集群上成功执行 SQL 工作负载，您的 StarRocks 集群需要与以下重要组件集成：

- 像 Kudu 文件系统或 Hive metastore 这样的 Metastore

## 使用注意事项

您只能使用 Kudu catalog 查询数据。您不能使用 Kudu catalog 删除、删除或插入数据到您的 Kudu 集群中。

## 集成准备

在创建 Kudu catalog 之前，请确保您的 StarRocks 集群可以与 Kudu 集群的存储系统和 metastore 集成。

> **注意**
>
> 如果在发送查询时返回未知主机的错误，您必须将 KUDU 集群节点的主机名和 IP 地址之间的映射添加到 **/etc/hosts** 路径中。

### Kerberos 认证

如果您的 KUDU 集群或 Hive metastore 启用了 Kerberos 认证，请按如下方式配置您的 StarRocks 集群：

- 在每个 FE 和每个 BE 上运行 `kinit -kt keytab_path principal` 命令，从密钥分发中心 (KDC) 获取票证授予票证 (TGT)。要运行此命令，您必须具有访问 KUDU 集群和 Hive metastore 的权限。请注意，使用此命令访问 KDC 是时间敏感的。因此，您需要使用 cron 定期运行此命令。
- 将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 的 **$BE_HOME/conf/be.conf** 文件中。在此示例中，`/etc/krb5.conf` 是 **krb5.conf** 文件的保存路径。您可以根据需要修改路径。

## 创建 Kudu catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "kudu",
    CatalogParams
)
```

### 参数

#### catalog_name

Kudu catalog 的名称。命名约定如下：

- 名称可以包含字母、数字 (0-9) 和下划线 (_)。必须以字母开头。
- 名称区分大小写，且不能超过 1023 个字符。

#### comment

Kudu catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `kudu`。

#### CatalogParams

StarRocks 访问 Kudu 集群元数据的一组参数。

下表描述了您需要在 `CatalogParams` 中配置的参数。

| 参数                     | 是否必需 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                 |
|--------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kudu.catalog.type        | 是       | 您用于 Kudu 集群的 metastore 类型。将此参数设置为 `kudu` 或 `hive`。                                                                                                                                                                                                                                                                                                                                                 |
| kudu.master              | 否       | 指定 Kudu Master 地址，默认为 `localhost:7051`。                                                                                                                                                                                                                                                                                                                                                                     |
| hive.metastore.uris      | 否       | Hive metastore 的 URI。格式：`thrift://<metastore_IP_address>:<metastore_port>`。如果您的 Hive metastore 启用了高可用 (HA)，您可以指定多个 metastore URI，并用逗号 (`,`) 分隔，例如，`"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |
| kudu.schema-emulation.enabled | 否       | 启用或禁用 `schema` 仿真的选项。默认情况下，它是关闭的 (false)，这意味着所有表都属于 `default` `schema`。                                                                                                                                                                                                                                                                                                           |
| kudu.schema-emulation.prefix | 否       | 仅当 `kudu.schema-emulation.enabled` = `true` 时才设置 `schema` 仿真的前缀。默认前缀为空字符串：` `。                                                                                                                                                                                                                                                                                                                   |

> **注意**
>
> 如果您使用 Hive metastore，必须在查询 Kudu 数据之前将 Hive metastore 节点的主机名和 IP 地址之间的映射添加到 `/etc/hosts` 路径中。否则，StarRocks 在启动查询时可能无法访问您的 Hive metastore。

### 示例

- 以下示例创建一个名为 `kudu_catalog` 的 Kudu catalog，其 metastore 类型 `kudu.catalog.type` 设置为 `kudu`，用于查询 Kudu 集群的数据。

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "kudu",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

- 以下示例创建一个名为 `kudu_catalog` 的 Kudu catalog，其 metastore 类型 `kudu.catalog.type` 设置为 `hive`，用于查询 Kudu 集群的数据。

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

## 查看 Kudu catalog

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前 StarRocks 集群中的所有 catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询 external catalog 的创建语句。以下示例查询名为 `kudu_catalog` 的 Kudu catalog 的创建语句：

```SQL
SHOW CREATE CATALOG kudu_catalog;
```

## 删除 Kudu catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除 external catalog。

以下示例删除名为 `kudu_catalog` 的 Kudu catalog：

```SQL
DROP Catalog kudu_catalog;
```

## 查看 Kudu 表的 schema

您可以使用以下语法之一查看 Kudu 表的 schema：

- 查看 schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 语句查看 schema 和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 Kudu 表

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看 Kudu 集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 在当前会话中切换到目标 catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   然后，使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话中的活动数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，您可以使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接指定目标 catalog 中的活动数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 从 Kudu 导入数据

假设您有一个名为 `olap_tbl` 的 OLAP 表，您可以像下面这样转换和导入数据：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM kudu_table;
```