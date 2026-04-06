---
displayed_sidebar: docs
---

# DataGrip

DataGrip 支持查询 StarRocks 中的内部数据和外部数据。

:::tip
[DataGrip 文档](https://www.jetbrains.com/help/datagrip/getting-started.html)
:::

您可以使用原生的 StarRocks JDBC 驱动（推荐）或 MySQL 驱动从 DataGrip 连接到 StarRocks。

## 使用 StarRocks JDBC 驱动连接（推荐）

StarRocks JDBC 驱动提供准确的元数据发现，这使得 DataGrip 中能够进行模式浏览、自动完成和表内省。

### 先决条件

下载 StarRocks JDBC 驱动 JAR 包。请参阅 [StarRocks JDBC 驱动](../JDBC_driver.md) 获取下载说明。

### 步骤

1. 在 DataGrip 中，转到 **文件** > **数据源** （或点击 **数据库** 工具栏中的图标）。

2. 点击 **+** 并选择 **驱动和数据源**。

3. 在 **驱动程序** 选项卡中，点击 **+** 以创建新驱动。

   - 将 **名称** 设置为 `StarRocks`。
   - 在 **驱动文件** 下，点击 **+** 并添加您下载的 StarRocks JDBC 驱动 JAR 包。
   - 将 **类** 设置为 `com.starrocks.cj.jdbc.Driver`。
   - 将**URL 模板**设置为：
     ```
     jdbc:starrocks://{host}:{port}/{catalog}.{database}
     ```
   - 点击**确定**保存驱动程序。

   ![DataGrip - StarRocks 驱动程序配置](../../_assets/IDE_datagrip_starrocks_driver.png)

4. 回到**数据源**选项卡，点击**+**并选择**StarRocks**您刚刚创建的驱动程序。

5. 配置连接设置：

   - **主机**：您的 StarRocks 集群的 FE 主机 IP 地址。
   - **端口**：您的 StarRocks 集群的 FE 查询端口，例如 `9030`。
   - **目录**：目标目录的名称。内部表使用 `default_catalog`，外部目录使用其名称。
   - **数据库**：目录中目标数据库的名称。
   - **用户**：登录 StarRocks 集群的用户名，例如 `admin`。
   - **密码**：登录 StarRocks 集群的密码。

6. 点击**测试连接**验证设置，然后点击**确定**。

## 使用 MySQL 驱动程序连接

MySQL 驱动程序是 StarRocks JDBC 驱动程序的备用方案。

在 DataGrip 中创建数据源。请注意，您必须选择 MySQL 作为数据源。

![DataGrip - 1](../../_assets/BI_datagrip_1.png)

![DataGrip - 2](../../_assets/BI_datagrip_2.png)

您需要配置的参数如下所述：

- **主机**：您的 StarRocks 集群的 FE 主机 IP 地址。
- **端口**：您的 StarRocks 集群的 FE 查询端口，例如 `9030`。
- **身份验证**: 您要使用的身份验证方法。选择**用户名和密码**。
- **用户**: 用于登录您的 StarRocks 集群的用户名，例如 `admin`。
- **密码**: 用于登录您的 StarRocks 集群的密码。
- **数据库**: 您要在 StarRocks 集群中访问的数据源。此参数的值采用 `<catalog_name>.<database_name>` 格式。
  - `catalog_name`：您的 StarRocks 集群中目标目录的名称。支持内部和外部目录。
  - `database_name`：您的 StarRocks 集群中目标数据库的名称。支持内部和外部数据库。
