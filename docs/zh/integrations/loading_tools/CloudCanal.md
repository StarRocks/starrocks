---
displayed_sidebar: docs
---

# 使用 CloudCanal 导入

## 介绍

CloudCanal 是一款全自研、可视化、自动化的数据迁移、同步工具，支持 30+ 款流行关系型数据库、实时数仓、消息中间件、缓存数据库和搜索引擎之间的数据互通，具备实时高效、精确互联、稳定可拓展、一站式、混合部署、复杂数据转换等优点，助力企业打破数据孤岛，从而更好地使用数据。

![image.png](../../_assets/3.11-1.png)

## 功能说明

CloudCanal 提供可视化的界面，可轻松实现数据的结构迁移、全量迁移、增量同步、校验与订正等，此外还可通过设置参数，完成更多精细化、自定义的数据同步配置。目前支持从以下数据源集成数据到 StarRocks:
  
| 源端数据源                    | 结构迁移 | 全量迁移 | 增量同步 | 校验订正 |
| --------------------------- | ------- | ------- | ------ | ------- |
| MySQL/MariaDB/AuroraMySQL   | 支持     | 支持    | 支持    | 支持     |
| Oracle                      | 支持     | 支持    | 支持    | 支持     |
| PostgreSQL/AuroraPostgreSQL | 支持     | 支持    | 支持    | 支持     |
| SQL Server                  | 支持     | 支持    | 支持    | 支持     |
| Kafka                       | 不支持   | 不支持   | 支持    | 不支持   |
| AutoMQ                      | 不支持   | 不支持   | 支持    | 不支持   |
| TiDB                        | 支持     | 支持    | 支持    | 支持     |
| Hana                        | 支持     | 支持    | 支持    | 支持     |
| PolarDB for MySQL           | 支持     | 支持    | 支持    | 支持     |
| Db2                         | 支持     | 支持    | 支持    | 支持     |

:::info

更多功能及参数设置，请参考 [CloudCanal 数据链路说明](https://www.clougence.com/cc-doc/dataMigrationAndSync/connection/mysql2?target=StarRocks)。

:::

## 下载安装

请参考 [全新安装 (Docker Linux/MacOS)](https://www.clougence.com/cc-doc/productOP/docker/install_linux_macos)，前往 [CloudCanal 官网](https://www.clougence.com/) 下载安装私有部署版本。

## 操作示例

以下以 MySQL 为例，演示如何实现 MySQL 到 StarRocks 的数据迁移同步。

### 添加数据源

1. 登录 CloudCanal 平台，点击 **数据源管理** > **新增数据源**。
2. 添加 StarRocks 数据源，并填写相应信息。
   - **Client 地址**：StarRocks 提供给 MySQL Client 的服务端口，CloudCanal 主要用其查询库表的元数据信息。
   - **账号**：StarRocks 集群用户名。导入操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。
   - **Http 地址**：主要用于接收 CloudCanal 数据导入的请求。

   ![image.png](../../_assets/3.11-2.png)

3. 点击 **测试连接**，连接成功后，点击 **新增数据源**。
4. 按照相同步骤添加 MySQL 数据源。

### 创建任务

1. 点击 **同步任务** > **创建任务**。
2. 选择源和目标数据源，并分别点击 **测试连接**。

   ![image.png](../../_assets/3.11-3.png)

3. 选择 **数据同步** 并勾选 **全量初始化**。

   ![image.png](../../_assets/3.11-4.png)

4. 选择需要同步的表。**结构迁移自动创建的表为主键表，因此暂不支持无主键表**。

   ![image.png](../../_assets/3.11-5.png)

5. 选择需要同步的列。

   ![image.png](../../_assets/3.11-6.png)

6. 确认创建任务。

   ![image.png](../../_assets/3.11-7.png)

7. 任务自动运行。CloudCanal 会自动进行任务流转，其中的步骤包括：
   - **结构迁移**: 将源端的表结构迁移到对端，如果同名表在对端已存在，则忽略。
   - **全量数据迁移**: 已存在的存量数据将会完整迁移到对端，支持断点续传。
   - **增量数据同步**: 增量数据将会持续地同步到对端数据库，并且保持实时（秒级别延迟）。

![image.png](../../_assets/3.11-8.png)
