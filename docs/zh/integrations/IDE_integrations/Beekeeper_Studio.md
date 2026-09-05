---
sidebar_position: 5
displayed_sidebar: docs
description: "Beekeeper Studio 是一款开源、跨平台的 SQL 编辑器和数据库管理工具，原生支持 StarRocks。"
---

# Beekeeper Studio

Beekeeper Studio 是一款[开源](https://github.com/beekeeper-studio/beekeeper-studio)、跨平台的 SQL 编辑器和数据库管理工具，支持 Windows、macOS 和 Linux，并内置原生 StarRocks 驱动。

![Beekeeper Studio 连接到 StarRocks](../../_assets/IDE_beekeeper_2.png)

Beekeeper Studio 是一家独立软件企业，其收入来自应用内高级功能的销售。该应用不含任何跟踪程序，并高度专注于易用性。

StarRocks 支持包含在 Beekeeper Studio 的免费社区版中，因此您可以免费地像使用其他受支持的数据库一样浏览 StarRocks 的 Schema 并运行查询。

## 前提条件

确保您已安装 Beekeeper Studio 6.0 或更高版本。
您可以从 [Beekeeper Studio 官网](https://www.beekeeperstudio.io/get)下载。

## 用法

按照以下步骤连接到 StarRocks：

1. 启动 Beekeeper Studio。

2. 在连接页面的 **Connection Type** 下拉列表中选择 **StarRocks**。

   ![Beekeeper Studio - 连接页面](../../_assets/IDE_beekeeper_1.png)

3. 配置以下连接信息：

   - **Host**：您的 StarRocks 集群的 FE 主机 IP 地址。
   - **Port**：StarRocks 集群的 FE 查询端口，例如 `9030`。
   - **User**：用于登录 StarRocks 集群的用户名，例如 `admin`。
   - **Password**：用于登录 StarRocks 集群的密码。
   - **Default Database**：（可选）连接后默认使用的数据库。

4. 点击 **Test** 验证连接设置的准确性，然后点击 **Connect**。您也可以在 **Save Connection** 中为该连接命名并保存，以便日后复用。

5. 连接建立后，您可以在左侧边栏中浏览数据库和表，并在 SQL 编辑器中编写和运行查询。

   ![Beekeeper Studio - SQL 编辑器](../../_assets/IDE_beekeeper_2.png)
