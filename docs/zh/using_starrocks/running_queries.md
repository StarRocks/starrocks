---
displayed_sidebar: docs
sidebar_position: 9
---

# 查看运行中的查询

本文介绍 StarRocks 如何查看运行中的查询，分析查询的资源消耗情况。

## current_queries

`show proc '/current_queries'` 可以查看当前 FE 节点的运行的查询，包含以下信息：


| **列名** | **说明** |
| --- | --- | 
| StartTime | 查询开始时间 | 
| feIp | 执行查询的 FE 节点 IP 地址 |
| QueryId | 查询 ID |
| ConnectionId | 连接 ID |
| Database | 查询所在的数据库 |
| User | 执行查询的用户 |
| ScanBytes | 扫描的数据量 |
| ScanRows | 扫描的行数 |
| MemoryUsage | 查询使用的内存量 |
| DiskSpillSize | 查询溢出到磁盘的数据量 |
| CPUTime | 查询消耗的 CPU 时间 |
| ExecTime | 查询执行时间 |
| Warehouse | 查询使用的 Warehouse |
| CustomQueryId | 用户自定义的查询 ID |
| ResourceGroup | 查询使用的资源组 |



**样例**:
```sql

MySQL > show proc '/current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2025-03-07 02:16:04
feIp          | 172.26.92.227
QueryId       | 10db481c-fab7-11ef-8063-461f20abc3f0
ConnectionId  | 13
Database      | tpcds_2
User          | root
ScanBytes     | 120.573 MB
ScanRows      | 5859503 rows
MemoryUsage   | 225.893 MB
DiskSpillSize | 0.000 B
CPUTime       | 47.878 s
ExecTime      | 4.077 s
Warehouse     | default_warehouse
CustomQueryId |
ResourceGroup | rg1
```


## global_current_queries

与 `current_queries` 类似，`show proc '/global_current_queries'` 可以查看到有 FE 节点正在执行的查询信息。此命令从 3.4 版本开始支持。



**样例**：
```sql
MySQL root@127.1:(none)> show proc '/global_current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2025-03-07 02:21:48
feIp          | 172.26.92.227
QueryId       | de516505-fab7-11ef-8063-461f20abc3f0
ConnectionId  | 14
Database      | tpcds_2
User          | root
ScanBytes     | 120.573 MB
ScanRows      | 5859503 rows
MemoryUsage   | 346.915 MB
DiskSpillSize | 0.000 B
CPUTime       | 33.265 s
ExecTime      | 3.032 s
Warehouse     | default_warehouse
CustomQueryId |
ResourceGroup | rg1

```

## running queries

`SHOW RUNNING QUERIES` 主要用于查看 Query Queue 的情况， 如果 Query 在队列中，会处于 `PENDING` 状态。

| 字段名称 | 说明 |
|---|---|
| QueryId | 查询 ID |
| ResourceGroupId | 资源组 ID |
| StartTime | 查询开始时间 |
| PendingTimeout | 等待超时时间 |
| QueryTimeout | 查询超时时间 |
| State | 查询状态，包括 PENDING 和 RUNNING |
| Slots | 占用的 slot 数量 |
| Fragments | 查询计划中的 Fragment 数量 |
| DOP | 并行度 |
| Frontend | FE 节点信息 |
| FeStartTime | FE 启动时间 |


**样例**：

```sql
MySQL root@127.1:(none)> show running queries\G
***************************[ 1. row ]***************************
QueryId         | 50029ec1-fab8-11ef-8063-461f20abc3f0
ResourceGroupId | 562275
StartTime       | 2025-03-07 02:24:59
PendingTimeout  | 2025-03-07 02:27:29
QueryTimeout    | 2025-03-07 02:27:29
State           | RUNNING
Slots           | 1
Fragments       | 11
DOP             | 0
Frontend        | 172.26.92.227_8034_1709578860161
FeStartTime     | 2025-03-06 23:39:00
```