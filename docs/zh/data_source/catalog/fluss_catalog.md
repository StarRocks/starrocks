---
displayed_sidebar: docs
description: "使用 Fluss catalog 查询 Apache Fluss 表中的数据。"
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Fluss catalog

<Beta />

Fluss catalog 是一种 External Catalog，支持您无需将数据导入 StarRocks，即可查询 Apache Fluss 表。

## 使用说明

当前版本中，Fluss catalog 仅支持查询已启用 DataLake 且使用 Apache Paimon 作为湖存储的 Fluss 表。

## 创建 Fluss catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "fluss",
    "bootstrap.servers" = "<fluss_bootstrap_server>",
    "fluss.option.<fluss_client_option>" = "<value>",
    "table.datalake.paimon.<paimon_option>" = "<value>"
);
```

### 参数说明

#### catalog_name

Fluss catalog 的名称。命名规范如下：

- 名称可以包含字母、数字（0-9）和下划线（_），且必须以字母开头。
- 名称区分大小写，且长度不能超过 1023 个字符。

#### comment

Fluss catalog 的描述。此参数为可选。

#### PROPERTIES

| 参数 | 是否必填 | 说明 |
| --- | --- | --- |
| `type` | 是 | 数据源类型。设置为 `fluss`。 |
| `bootstrap.servers` | 是 | StarRocks 连接 Fluss 使用的地址，例如 `fluss-host:9123`。 |
| `fluss.option.<fluss_client_option>` | 否 | Fluss 客户端配置项。去掉 `fluss.option.` 前缀后即为对应的 Fluss 客户端配置键。例如，`fluss.option.client.security.protocol` 对应 `client.security.protocol`。 |
| `table.datalake.paimon.<paimon_option>` | 否 | StarRocks 通过 Paimon 访问 Fluss 表湖存储时使用的配置项。 |

### 示例

以下示例创建一个使用 Paimon filesystem metastore 和 S3 存储的 Fluss catalog：

```SQL
CREATE EXTERNAL CATALOG fluss_catalog
PROPERTIES
(
    "type" = "fluss",
    "bootstrap.servers" = "fluss-host:9123",

    -- 可选。Fluss 客户端启用认证时配置。
    "fluss.option.client.security.protocol" = "SASL",
    "fluss.option.client.security.sasl.mechanism" = "PLAIN",
    "fluss.option.client.security.sasl.username" = "<fluss_user>",
    "fluss.option.client.security.sasl.password" = "<fluss_password>",

    -- Paimon 湖存储。
    "table.datalake.paimon.metastore" = "filesystem",
    "table.datalake.paimon.warehouse" = "s3://bucket/path",
    "table.datalake.paimon.s3.endpoint" = "https://s3.<region>.amazonaws.com",
    "table.datalake.paimon.s3.access-key" = "<access_key>",
    "table.datalake.paimon.s3.secret-key" = "<secret_key>"

    -- 如需使用阿里云 OSS，请将上述 S3 warehouse 和配置项
    -- 替换为以下配置：
    -- "table.datalake.paimon.warehouse" = "oss://<bucket>/<path>",
    -- "table.datalake.paimon.fs.oss.endpoint" = "oss-cn-hangzhou.aliyuncs.com",
    -- "table.datalake.paimon.fs.oss.accessKeyId" = "<access_key_id>",
    -- "table.datalake.paimon.fs.oss.accessKeySecret" = "<access_key_secret>"
);
```

## 查看 Fluss catalog

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查看当前 StarRocks 集群中的所有 Catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查看 External Catalog 的创建语句：

```SQL
SHOW CREATE CATALOG fluss_catalog;
```

## 删除 Fluss catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除 External Catalog：

```SQL
DROP CATALOG fluss_catalog;
```

## 查看 Fluss 表的 schema

您可以使用以下任一语法查看 Fluss 表的 schema：

- 查看 schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 通过 CREATE 语句查看 schema 和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 Fluss 表

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看 Fluss 中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 在当前会话中切换到 Fluss catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   然后，使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前数据库：

   ```SQL
   USE <db_name>;
   ```

   您也可以直接指定 Fluss catalog 中的当前数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) 查询 Fluss 表：

   ```SQL
   -- Union read：湖中的历史数据和 Fluss 中的实时数据。
   SELECT * FROM <table_name>;

   -- Lake read：仅查询湖中的历史数据。
   SELECT * FROM <table_name>$lake;

   -- Real-time read：仅查询 Fluss 中的实时数据。
   SELECT * FROM <table_name>$rt;
   ```
