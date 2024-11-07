---
displayed_sidebar: docs
---

# SHOW COMPUTE NODES

## Description

Shows the information of all CN nodes in the cluster.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```SQL
SHOW COMPUTE NODES
```

## Return

```SQL
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
| ComputeNodeId | IP           | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | ErrMsg | Version            | CpuCores | NumRunningQueries | MemUsedPct | CpuUsedPct | HasStoragePath | StarletPort | WorkerId | WarehouseName     | TabletNum |
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
```

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                                   |
| -------------------- | ----------------------------------------------------------------- |
| ComputeNodeId        | The ID of the CN node.                                            |
| IP                   | The IP address of the CN node.                                    |
| HeartbeatPort        | The heartbeat port of the CN node. It is used to receive heartbeats from the FE node. |
| BePort               | Thrift server port of the CN node. It is used to receive requests from the FE node. |
| HttpPort             | HTTP server port of the CN node. It is used to access the CN node via web page. |
| BrpcPort             | bRPC port of the CN node. It is used for communication across CN nodes. |
| LastStartTime        | The last time at which the CN node starts.                        |
| LastHeartbeat        | The last time at which the CN node sends a heartbeat.             |
| Alive                | Whether the CN node is alive or not.<ul><li>`true`: the CN node is alive.</li><li>`false`: the CN node is not alive. </li></ul> |
| SystemDecommissioned | If the value of the parameter is `true`, the CN node is removed from your StarRocks cluster. |
| ClusterDecommissioned | This parameter is used for system compatibility.                 |
| ErrMsg               | The error message if the CN node fails to send a heartbeat.       |
| Version              | The StarRocks version of the CN node.                             |
| CpuCores             | The number of CPU cores in the CN node.                           |
| NumRunningQueries    | The number of running queries on the CN node.                     |
| MemUsedPct           | The percentage of used memory.                                    |
| CpuUsedPct           | The percentage of used CPU cores.                                 |
| HasStoragePath       | Whether the CN node has storage paths configured.                 |
| StarletPort          | The `starlet_port` of the CN node. It is an extra agent service port. |
| WorkerId             | The ID of the CN node for internal scheduling.                    |
| WarehouseName        | The name of the warehouse to which the CN node belongs. The value is always `default_warehouse`. |
| TabletNum            | The number of tablets (of cached data) on the CN node.            |

## Example

View the information of all CN nodes in the cluster.

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
