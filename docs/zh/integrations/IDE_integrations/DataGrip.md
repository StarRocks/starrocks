# DataGrip

DataGrip 支持查询 StarRocks 中的内部数据和外部数据。

在 DataGrip 中创建数据源。注意创建过程中需要选择 **MySQL** 作为数据源 (**Data** **Source**)。

![DataGrip - 1](../../assets/BI_datagrip_1.png)

![DataGrip - 2](../../assets/BI_datagrip_2.png)

需要设置的参数说明如下：

- **Host**：StarRocks 集群的 FE 主机 IP 地址。
- **Port**：StarRocks 集群的 FE 查询端口，如 `9030`。
- **Authentication**：鉴权方式。选择 **Username & Password**。
- **User**：用于登录 StarRocks 集群的用户名，如 `admin`。
- **Password**：用于登录 StarRocks 集群的用户密码。
- **Database**：StarRocks 集群中要访问的数据源。格式为 `<catalog_name>.<database_name>`。
  - `catalog_name`：StarRocks 集群中目标 Catalog 的名称。Internal Catalog 和 External Catalog 均支持。
  - `database_name`：StarRocks 集群中目标数据库的名称。内部数据库和外部数据库均支持。
