---
displayed_sidebar: docs
---

# SHOW COMPUTE NODES

## 功能

查看当前集群中所有 CN 节点的相关信息。

> **注意**
>
> 该操作需要 SYSTEM 级 OPERATE 权限或 `cluster_admin` 角色。

## 语法

```sql
SHOW COMPUTE NODES
```

## 返回信息说明

```SQL
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
| ComputeNodeId | IP           | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | ErrMsg | Version            | CpuCores | NumRunningQueries | MemUsedPct | CpuUsedPct | HasStoragePath | StarletPort | WorkerId | WarehouseName     | TabletNum |
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
```

返回信息中的参数说明如下：

| **参数**              | **说明**                                                          |
| -------------------- | ----------------------------------------------------------------- |
| ComputeNodeId        | CN 的 ID。                                                         |
| IP                   | CN 的 IP 地址。                                                    |
| HeartbeatPort        | CN 上的心跳端口，用于接收来自 FE 的心跳。                               |
| BePort               | CN 上的 Thrift server 端口， 用于接收来自 FE 的请求。                  |
| HttpPort             | CN 上的 HTTP server 端口，用于网页访问 BE。                           |
| BrpcPort             | CN 上的 bRPC 端口，用于 CN 之间通讯。                                 |
| LastStartTime        | CN 最后一次启动的时间。                                              |
| LastHeartbeat        | FE 最后一次发心跳给 BE，且 CN 成功回复的时间。                          |
| Alive                | CN 是否存活。<ul><li>`true`：表示存活。</li><li>`false`：表示没有存活。 </li></ul> |
| SystemDecommissioned | CN 是否已下线。`true`：表示集群已经标记该 CN 下线。                     |
| ClusterDecommissioned | 该参数用于系统兼容。                                                |
| ErrMsg               | 接收 FE 心跳失败时的错误信息。                                        |
| Version              | CN 的 StarRocks 版本。                                             |
| CpuCores             | CN 机器的 CPU 核数。                                                |
| NumRunningQueries    | CN 上正在运行的查询数量。                                             |
| MemUsedPct           | 内存使用量百分比。                                                   |
| CpuUsedPct           | CPU 使用量百分比。                                                  |
| HasStoragePath       | CN 是否配置有存储路径。                                               |
| StarletPort          | CN 的 `starlet_port`。该端口为额外 Agent 服务端口。                   |
| WorkerId             | CN 的内部调度 ID。                                                  |
| WarehouseName        | CN 所属的 Warehouse 名。该值恒为 `default_warehouse`。               |
| TabletNum            | CN 上 Tablet 的数量（仅针对 Cache 中的数据）。                         |

## 示例

查看当前集群中的所有 CN 节点的相关信息。

```Plain
MySQL > SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10001
                   IP: x.x.x.x
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2024-05-14 15:45:34
        LastHeartbeat: 2024-05-14 15:47:59
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
               ErrMsg: 
              Version: 3.3.0-rc01-3b8cb0c
             CpuCores: 4
    NumRunningQueries: 0
           MemUsedPct: 1.95 %
           CpuUsedPct: 0.0 %
       HasStoragePath: true
          StarletPort: 8167
             WorkerId: 1
        WarehouseName: default_warehouse
            TabletNum: 58
1 row in set (0.00 sec)
```
