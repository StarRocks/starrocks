---
sidebar_position: 32
displayed_sidebar: docs
description: "DBine 是一款免费、开源的桌面数据库管理工具，通过 MySQL 协议连接 StarRocks，用于浏览 Schema 和运行查询。"
---

# DBine

[DBine](https://dbine.com) 是一款免费、开源（Apache 2.0 许可证）的桌面数据库管理工具，支持 Windows、macOS 和 Linux，
可以在同一个窗口中使用 130 多种数据库和云服务。它提供 StarRocks 连接类型，通过 MySQL 协议连接到 FE。

## 前提条件

- 一个可以从您的电脑访问的 StarRocks 集群，以及它的 FE 查询端口（MySQL 协议，默认为 `9030`）。
- 从 [发布页面](https://github.com/addlayer-io/dbine/releases) 下载的 DBine。

## 集成

1. 在 DBine 中，点击 **New connection**，然后在引擎列表中选择 **StarRocks**。
2. 填写连接设置：
   - **Server**：FE 的主机名或 IP 地址
   - **Port**：FE 查询端口，默认为 `9030`
   - **Database**：可选，首先打开的数据库
   - **User** / **Password**：您的 StarRocks 用户名和密码
3. 点击 **Test connection** 进行验证，然后点击 **Save and connect**。

连接会显示在 **Explorer** 中。您可以：

- 浏览数据库、表、视图、列和索引。
- 右键点击数据库，选择 **New query**，在带有自动补全的编辑器中运行 SQL。查询结果可以生成图表，并导出为
  CSV、JSON、XLSX 或 SQL。
- 查看 `EXPLAIN` 返回的查询计划。
- 在表设计器中创建表，并设置数据模型、分桶方式和分桶数。
- 通过 `SHOW FULL PROCESSLIST` 查看当前连接的 FE 上正在运行的查询。
- 将连接设置为只读，DBine 会拒绝执行写入语句。

![DBine - 在 StarRocks 上的查询结果](../../_assets/IDE_dbine_1.png)
