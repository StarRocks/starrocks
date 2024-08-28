---
displayed_sidebar: docs
---

# [Experimental] Kudu catalog

StarRocks 从 3.3 版本开始支持 Kudu Catalog。

Kudu Catalog 是一种 External Catalog。通过 Kudu Catalog，您不需要执行数据导入就可以直接查询 Apache Kudu 里的数据。

此外，您还可以基于 Kudu Catalog ，结合 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 能力来实现数据转换和导入。

为保证正常访问 Kudu 内的数据，StarRocks 集群必须集成以下关键组件：

- 元数据服务。当前支持的元数据服务包括 Kudu 文件系统 (File System)、Hive Metastore（以下简称 HMS）。

## 使用说明

Kudu Catalog 仅支持查询 Kudu 数据，不支持针对 Kudu 的写/删操作。

## 准备工作

在创建 Kudu Catalog 之前，请确保 StarRocks 集群能够正常访问 Kudu 集群及元数据服务。

> **注意**
>
> 如果查询时因为域名无法识别 (Unknown Host) 而发生访问失败，您需要将 KUDU 集群中各节点的主机名及 IP 地址之间的映射关系配置到 **/etc/hosts** 路径中。

### Kerberos 认证

如果 KUDU 集群或 HMS 开启了 Kerberos 认证，则需要在 StarRocks 集群中做如下配置：

- 在每个 FE 和 每个 BE 上执行 `kinit -kt keytab_path principal` 命令，从 Key Distribution Center (KDC) 获取到 Ticket Granting Ticket (TGT)。执行命令的用户必须拥有访问 HMS 和 KUDU 的权限。注意，使用该命令访问 KDC 具有时效性，因此需要使用 cron 定期执行该命令。
- 在每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 的 **$BE_HOME/conf/be.conf** 文件中添加 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"`。其中，`/etc/krb5.conf` 是 **krb5.conf** 文件的路径，可以根据文件的实际路径进行修改。

## 创建 Kudu Catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "kudu",
    CatalogParams,
)
```

### 参数说明

#### catalog_name

Kudu Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### comment

Kudu Catalog 的描述。此参数为可选。

#### type

数据源的类型。设置为 `kudu`。

#### CatalogParams

StarRocks 访问 Kudu 集群元数据的相关参数配置。

`CatalogParams` 包含如下参数。

| 参数                            | 是否必须 | 说明                                                                                                                                                                                                                                                |
|-------------------------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kudu.catalog.type             | 是    | Kudu 使用的元数据类型。设置为 `kudu`、`hive`。                                                                                                                                                                                                                  |
| kudu.master                   | 否    | 指定 `Kudu Master` 连接地址，默认为：`localhost:7051`。                                                                                                                                                                                                                  |
| hive.metastore.uris           | 否    | HMS 的 URI， 格式：`thrift://<HMS IP 地址>:<HMS 端口号>`。仅在 `kudu.catalog.type` = `hive` 时设置。<br />如果您的 HMS 开启了高可用模式，此处可以填写多个 HMS 地址并用逗号分隔，例如：`"thrift://<HMS IP 地址 1>:<HMS 端口号 1>,thrift://<HMS IP 地址 2>:<HMS 端口号 2>,thrift://<HMS IP 地址 3>:<HMS 端口号 3>"`。 |
| kudu.schema-emulation.enabled | 否    | 是否启用模拟 `schema` 功能，默认处于关闭状态（`false`），即所有表都属于 `default` `schema`。                                                                                                                                                                                  |
| kudu.schema-emulation.prefix  | 否    | 仅在 `kudu.schema-emulation.enabled` = `true` 即启用模拟 `schema` 功能时，需设置匹配前缀，默认采用前缀空字符串：` `。                                                                                                                                                            |

> **说明**
>
> 若使用 HMS 作为元数据服务，则在查询 Kudu 数据之前，必须将所有 HMS 节点的主机名及 IP 地址之间的映射关系添加到 **/etc/hosts** 路径。否则，发起查询时，StarRocks 可能无法访问 HMS。

### 示例

- 以下示例创建了一个名为 `kudu_catalog` 的 Kudu Catalog，其元数据类型 `kudu.catalog.type` 为 `kudu`，用于查询 Kudu 集群里的数据。

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

- 以下示例创建了一个名为 `kudu_catalog` 的 Kudu Catalog，其元数据类型 `kudu.catalog.type` 为 `hive`，用于查询 Kudu 集群里的数据。

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

## 查看 Kudu Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 Kudu Catalog `kudu_catalog` 的创建语句：

```SQL
SHOW CREATE CATALOG kudu_catalog;
```

## 删除 Kudu Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除某个 External Catalog。

例如，通过如下命令删除 Kudu Catalog `kudu_catalog`：

```SQL
DROP Catalog kudu_catalog;
```

## 查看 Kudu 表结构

您可以通过如下方法查看 Kudu 表的表结构：

- 查看表结构

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 命令查看表结构和表文件存放位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 Kudu 表数据

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看指定 Catalog 所属的 Kudu Catalog 中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 切换当前会话生效的 Catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话生效的数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，也可以通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 导入 Kudu 数据

假设有一个 OLAP 表，表名为 `olap_tbl`。您可以这样来转换该表中的数据，并把数据导入到 StarRocks 中：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM kudu_table;
```
