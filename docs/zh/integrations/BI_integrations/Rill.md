---
displayed_sidebar: docs
---

# Rill

Rill 支持连接到 StarRocks ，作为一个 OLAP 连接器，通过外部表为 Rill 仪表板提供支持。Rill 可以查询和可视化 StarRocks 中的内部数据和外部数据。

## 连接

Rill 使用 MySQL 协议连接到 StarRocks。您可以使用连接参数或 DSN 连接字符串来配置连接。

### 连接参数

在 Rill 中添加数据源时，选择 **StarRocks** 并配置以下参数：

- **Host**: 您的 StarRocks 集群的 FE host IP 地址或主机名。
- **Port**: StarRocks FE 的 MySQL 协议端口（默认值：`9030`）。
- **Username**: 用于身份验证的用户名（默认值：`root`）。
- **Password**: 用于身份验证的密码。
- **Catalog**: StarRocks 的 catalog 名称（默认值：`default_catalog`）。支持内部和 external catalog（例如，Iceberg、 Hive）。
- **Database**: StarRocks 数据库名称。
- **SSL**: 启用 SSL/TLS 加密（默认值：`false`）。

### 连接字符串 (DSN)

或者，您可以使用 MySQL 格式的 DSN 连接字符串：

```
user:password@tcp(host:9030)/database?parseTime=true
```

对于 external catalog，将 catalog 和 database 指定为单独的 property。

## External Catalog

StarRocks 支持查询来自 external catalog 的数据，包括 Hive、Iceberg、Delta Lake 和其他外部数据源。将 `catalog` 属性设置为您的 external catalog 名称（例如，`iceberg_catalog`），并将 `database` 属性设置为该 catalog 中的数据库。

## 更多信息

如需了解详细的配置选项、示例、问题排查以及最新信息，请参阅 [Rill Data StarRocks 连接器文档](https://docs.rilldata.com/developers/build/connectors/olap/starrocks) 。
