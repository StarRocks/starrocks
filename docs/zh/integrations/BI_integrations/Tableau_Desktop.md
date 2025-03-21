---
displayed_sidebar: docs
---

# Tableau

本文介绍如何使用 StarRocks Tableau JDBC Connector 连接 StarRocks 与 Tableau Desktop 和 Tableau Server。

## 概述

StarRocks Tableau JDBC Connector 是一个用于 Tableau Desktop 和 Tableau Server 的定制扩展组件。它简化了 Tableau 连接 StarRocks 的流程，并增强了对标准 Tableau 功能的支持，其性能优于默认的通用 ODBC/JDBC 连接。

### 主要特性

- 支持 LDAP：支持通过密码提示进行 LDAP 登录，实现安全认证。
- 高兼容性：在 TDVT（Tableau Design Verification Tool）测试中达到 99.99% 的兼容性，仅有一个轻微的失败案例。

## 前提条件

在开始之前，请确保满足以下要求：

- Tableau 版本：Tableau 2020.4 及以上版本
- StarRocks 版本：StarRocks v3.2 及以上版本

## 为 Tableau Desktop 安装连接器

1. 下载 [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/)。
2. 将驱动文件存放在以下目录（如果目录不存在，请手动创建）：

   - macOS：`~/Library/Tableau/Drivers`
   - Windows：`C:\Program Files\Tableau\Drivers`

3. 下载 [StarRocks JDBC Connector](https://drive.google.com/file/d/1dsCyZNanceHD93vAY9fMjG9I18UGvpPl/view) 文件。
4. 将 Connector 文件存放在以下目录：

   - macOS：`~/Documents/My Tableau Repository/Connectors`
   - Windows：`C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors`

5. 启动 Tableau Desktop。
6. 进入 **Connect** -> **To a Server** -> **StarRocks** **JDBC** **by CelerData**。

## 为 Tableau Server 安装连接器

1. 下载 [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/)。
2. 将驱动文件存放在以下目录（如果目录不存在，请手动创建）：

   - Linux：`/opt/tableau/tableau_driver/jdbc`
   - Windows：`C:\Program Files\Tableau\Drivers`

   :::info

   在 Linux 系统中，必须为 "Tableau" 用户授予访问该目录的权限。

   请按以下步骤操作：

   1. 创建目录并复制驱动文件到指定目录：

      ```Bash
      sudo mkdir -p /opt/tableau/tableau_driver/jdbc

      # 将 <path_to_driver_file_name> 替换为驱动文件的绝对路径。
      sudo cp /<path_to_driver_file_name>.jar /opt/tableau/tableau_driver/jdbc
      ```

   2. 为 "Tableau" 用户授予权限：

      ```Bash
      # 将 <driver_file_name> 替换为驱动文件的名称。
      sudo chmod 755 /opt/tableau/tableau_driver/jdbc/<driver_file_name>.jar
      ```

   :::

3. 下载 [StarRocks JDBC Connector](https://drive.google.com/file/d/1dsCyZNanceHD93vAY9fMjG9I18UGvpPl/view) 文件。
4. 将连接器文件存放在每个节点的以下目录：

   - Linux: `/opt/tableau/connectors`
   - Windows: `C:\Program Files\Tableau\Connectors`

5. 重启 Tableau Server。

   ```Bash
   tsm restart
   ```

   :::info

   每次添加、删除或更新 Connector 后，必须重启 Tableau Server 才能使更改生效。

   :::

## 使用说明

如果需要支持 LDAP 登录，可在配置过程中切换到 **Advanced** 选项卡，并勾选 **Enable LDAP** 开关。
