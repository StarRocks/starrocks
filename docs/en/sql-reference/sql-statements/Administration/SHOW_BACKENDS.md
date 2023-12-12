---
displayed_sidebar: "English"
---

# SHOW BACKENDS

## Description

Shows the information of all BE nodes in the cluster.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```SQL
SHOW BACKENDS
```

## Return

```SQL
+-----------+-----+---------------+--------+----------+----------+---------------+---------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+----------+--------+-------------------+-------------+----------+
| BackendId | IP  | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime | LastHeartbeat | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | MaxDiskUsedPct | ErrMsg | Version  | Status | DataTotalCapacity | DataUsedPct | CpuCores |
+-----------+-----+---------------+--------+----------+----------+---------------+---------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+----------+--------+-------------------+-------------+----------+
```

| **Return**            | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| BackendId             | The ID of the BE node.                                       |
| IP                    | The IP address of the BE node.                               |
| HeartbeatPort         | The heartbeat port of the BE node. It is used to receive heartbeats from the FE node. |
| BePort                | Thrift server port of the BE node. It is used to receive requests from the FE node. |
| HttpPort              | HTTP server port of the BE node. It is used to access the BE node via web page. |
| BrpcPort              | bRPC port of the BE node. It is used for communication across BE nodes. |
| LastStartTime         | The last time when the BE node is started.                   |
| LastHeartbeat         | The last time when the FE node sends a heartbeat and the BE responds.|
| Alive                 | Whether the BE node is alive or not.<ul><li>`true`: the BE node is alive.</li><li>`false`: the BE node is not alive. </li></ul> |
| SystemDecommissioned  | Whether the BE node is decommissioned or not. Before it is decommissioned, the BE node migrates the data to other BE nodes. Data loading and queries are not affected during migration.<ul><li>`true`: the cluster has marked the BE node as decommissioned. The data could be successfully migrated or migrating.</li><li>`false`: the BE node is running.</li></ul> |
| ClusterDecommissioned | This parameter is used for system compatibility.             |
| TabletNum             | The number of tablets on the BE node.                        |
| DataUsedCapacity      | The storage capacity occupied by the data file.              |
| AvailCapacity         | Available storage capacity in the BE node.                   |
| TotalCapacity         | The total storage capacity. It is equivalent to `DataUsedCapacity` + `AvailCapacity` + Storage capacity occupied by the non-data files. |
| UsedPct               | The percentage of the used storage capacity.                 |
| MaxDiskUsedPct        | If a BE node has multiple directories to store data, this parameter shows the maximum percentage of the used storage capacity among all directories. |
| ErrMsg                | The error message returned when the BE node fails to receive the heartbeat from the FE node. |
| Version               | The StarRocks version of cluster.                            |
| Status                | The last time when the BE node reports the tablet number to the FE node. Displayed in JSON format. |
| DataTotalCapacity     | It is equivalent to `DataUsedCapacity` + `AvailCapacity`. It indicates the sum of the storage capacity occupied by the data file and the available storage capacity in the BE node. |
| DataUsedPct           |  It is equivalent to `DataUsedCapacity`/`DataTotalCapacity`. It indicates the proportion of the storage capacity occupied by the data file to the sum of the data-occupied storage capacity and the available storage capacity. |
| CpuCores              | The number of CPU cores in the BE node.                      |

## Example

View the information of all BE nodes in the cluster.

```Plain
SHOW BACKENDS;

+-----------+---------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+----------------------+--------------------------------------------------------+-------------------+-------------+----------+
| BackendId | IP      | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | MaxDiskUsedPct | ErrMsg | Version              | Status                                                 | DataTotalCapacity | DataUsedPct | CpuCores |
+-----------+---------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+----------------------+--------------------------------------------------------+-------------------+-------------+----------+
| 10002     | x.x.x.x | 9254          | 9260   | 8238     | 8260     | 2022-09-08 14:37:10 | 2022-09-08 15:14:21 | true  | false                | false                 | 21753     | 25.122 GB        | 1.088 TB      | 1.968 TB      | 44.72 % | 44.72 %        |        | MAIN-RELEASE-d094052 | {"lastSuccessReportTabletsTime":"2022-09-08 15:14:13"} | 1.113 TB          | 2.20 %      | 16       |
| 10003     | x.x.x.x | 9254          | 9260   | 8238     | 8260     | 2022-09-08 14:37:20 | 2022-09-08 15:14:21 | true  | false                | false                 | 21754     | 25.121 GB        | 1.169 TB      | 1.968 TB      | 40.61 % | 40.61 %        |        | MAIN-RELEASE-d094052 | {"lastSuccessReportTabletsTime":"2022-09-08 15:14:22"} | 1.194 TB          | 2.06 %      | 16       |
| 10004     | x.x.x.x | 9254          | 9260   | 8238     | 8260     | 2022-09-08 14:37:39 | 2022-09-08 15:14:21 | true  | false                | false                 | 21754     | 25.112 GB        | 1.172 TB      | 1.968 TB      | 40.46 % | 40.46 %        |        | MAIN-RELEASE-d094052 | {"lastSuccessReportTabletsTime":"2022-09-08 15:13:42"} | 1.197 TB          | 2.05 %      | 16       |
+-----------+---------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------+----------------------+--------------------------------------------------------+-------------------+-------------+----------+
```
