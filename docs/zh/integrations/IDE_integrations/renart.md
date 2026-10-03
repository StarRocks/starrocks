---
sidebar_position: 60
displayed_sidebar: docs
description: 将 Renart 开源数据平台连接到 StarRocks，在基于 Git 的本地工作区中查询数据表。
---

# Renart

[Renart](https://github.com/renart-data/renart) 是一个开源数据平台，用于在数据库和数据仓库之间移动、转换和分析数据。它将 SQL 和 Python 流水线、笔记本及仪表板整合到一个本地工作区中，提供类型检查和调度功能，并将各项定义以代码形式保存在 Git 中。本指南介绍如何连接 StarRocks 数据库、浏览表和列、预览数据行并运行查询。

## 前提条件

- 按照 [Renart 安装指南](https://getrenart.com/docs/installation/)安装 Renart v0.5.5。
- 一个用作 Renart 工作区的本地 Git 仓库。
- 一个正在运行的 StarRocks 集群、FE 主机名和查询端口，以及有权查询目标数据库的凭据。

本连接和查询流程已在 Renart v0.5.5 和 StarRocks 3.5.21 上测试。

## 连接 StarRocks

1. 在本地 Git 仓库中运行 `renart web`，打开 Renart。
2. 打开 **Build → Connections**，添加连接并选择 **StarRocks**。
3. 输入连接名称（例如 `starrocks-analytics`）以及以下信息：

   | 字段 | 值 |
   | --- | --- |
   | Host | StarRocks FE 的主机名或 IP 地址 |
   | Port | FE 查询端口，通常为 `9030` |
   | Database | 要查询的数据库 |
   | Username | StarRocks 用户名 |
   | Password | 该用户的密码 |

4. 为密码选择凭据来源。Renart 支持操作系统凭据存储、加密本地保管库或环境变量绑定。请参阅 [Renart 连接凭据](https://getrenart.com/docs/connections-environments/managing-credentials/)。
5. 对于仅用于查询的连接，选择 **Access → Read-only**。
6. 点击 **Verify**，然后将连接保存到目标环境。

:::note
只读访问限制由 Renart 管理的操作。还应使用具有适当权限的数据库账号。可写连接上的临时 SQL 查询可以执行写操作。
:::

## 浏览表和列

在 Renart 中打开 **Data Browser**，选择已保存的 StarRocks 连接并进入目标数据库。选择表以查看列名和数据库类型，并预览数据行。下面的 SQL 查询使用同一个连接和环境。

## 运行查询

在 Renart 的 SQL 查询工作区中选择已保存的连接和环境。首先检查已连接的 StarRocks 版本：

```sql
SELECT current_version();
```

然后查询该账号有权读取的表，将示例数据库名和表名替换为实际名称：

```sql
SELECT * FROM my_database.my_table LIMIT 100;
```

浏览大表时，应显式设置 SQL 行数限制。结果显示上限不一定限制数据库实际执行的工作量。

## 故障排查

- 如果验证失败，请检查 FE 连通性、查询端口、数据库名和用户权限。查询端口不是 HTTP 加载端点。
- 表物化和数据加载需要可写连接及相应配置。本查询指南不配置 Stream Load。

Renart 相关问题请提交到 [Renart 仓库](https://github.com/renart-data/renart/issues)。
