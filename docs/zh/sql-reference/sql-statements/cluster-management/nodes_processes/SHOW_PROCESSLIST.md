---
displayed_sidebar: docs
description: "SHOW PROCESSLIST 列出服务器内线程当前正在执行的操作。"
---

# SHOW PROCESSLIST

SHOW PROCESSLIST 列出服务器内线程当前正在执行的操作。当前版本的 StarRocks 仅支持列出查询。

:::tip

此操作不需要权限。

:::

## 语法

```SQL
SHOW [FULL] PROCESSLIST
```

## 参数

| 参数 | 是否必填 | 描述 |
| --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| FULL      | 否       | 如果指定此参数，将显示完整的 SQL 语句。否则，仅显示语句的前 100 个字符。|

## 返回值

| 返回值              | 描述                                                  |
| ------------------- | ------------------------------------------------------------ |
| Server              | 服务器 ID。                                                   |
| Id                  | 连接 ID。                                               |
| User                | 执行操作的用户名。                 |
| Host                | 执行操作的客户端主机名。         |
| Db                  | 执行操作的数据库名称。    |
| Command             | 命令类型。                                     |
| ConnectionStartTime | 连接开始时间。                             |
| Time                | 操作进入当前状态以来的时间（秒）。 |
| State               | 操作的状态。                                  |
| Info                | 操作正在执行的命令。                 |
| IsPending           | 查询是否在队列中等待。有效值：`true` 和 `false`。 |
| Warehouse           | 执行查询的仓库名称。       |
| CNGroup             | 运行查询的计算节点组名称。   |
| Catalog             | Catalog 名称。                                     |
| QueryId             | 当 Command 为 "Query" 时的查询 ID。                       |

## 使用说明

如果当前用户为 `root`，该语句将列出集群中所有用户的操作。否则，仅列出当前用户的操作。

`IsPending`、`Warehouse` 和 `CNGroup` 字段提供了有关在仓库环境中查询执行的附加信息：

- `IsPending`：显示查询是否在队列中等待（`true`）或正在执行（`false`）
- `Warehouse`：显示执行查询的仓库名称
- `CNGroup`：显示负责执行查询的计算节点组名称

## 示例

示例 1：通过用户 `root` 列出操作状态。

```SQL
SHOW PROCESSLIST\G

*************************** 6. row ***************************
         ServerName: starrocks-fe_9010_1782850099498
                 Id: 33560554
               User: root
               Host: 172.18.0.4:54818
                 Db:
            Command: Query
ConnectionStartTime: 2026-07-02 01:21:14
               Time: 0
              State: OK
               Info: show processlist
          IsPending: false
          Warehouse: default_warehouse
            CNGroup:
            Catalog: default_catalog
            QueryId: 019f1eb3-246c-7899-ab3e-40a018645bba
```
