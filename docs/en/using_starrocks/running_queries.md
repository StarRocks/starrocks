---
displayed_sidebar: docs
sidebar_position: 9
---

# View Running Queries

This article describes how to view running queries in StarRocks and analyze their resource consumption.

## current_queries

`show proc '/current_queries'` allows you to view the queries running on the current FE node, including the following information:

| **Column Name** | **Description** |
| --- | --- |
| StartTime | Query start time |
| feIp | IP address of the FE node executing the query |
| QueryId | Query ID |
| ConnectionId | Connection ID |
| Database | Database where the query is running |
| User | User executing the query |
| ScanBytes | Amount of data scanned |
| ScanRows | Number of rows scanned |
| MemoryUsage | Amount of memory used by the query |
| DiskSpillSize | Amount of data spilled to disk |
| CPUTime | CPU time consumed by the query |
| ExecTime | Query execution time |
| Warehouse | Warehouse used by the query |
| CustomQueryId | User-defined query ID |
| ResourceGroup | Resource group used by the query |

**Example**:
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

Similar to `current_queries`, `show proc '/global_current_queries'` shows information about queries running on all FE nodes. This command is supported since version 3.4.

**Example**:
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

`SHOW RUNNING QUERIES` is mainly used to view the Query Queue status. If a query is in the queue, it will be in a `PENDING` state.

| Field Name | Description |
|---|---|
| QueryId | Query ID |
| ResourceGroupId | Resource group ID |
| StartTime | Query start time |
| PendingTimeout | Waiting timeout |
| QueryTimeout | Query timeout |
| State | Query state, including PENDING and RUNNING |
| Slots | Number of slots occupied |
| Fragments | Number of fragments in the query plan |
| DOP | Degree of parallelism |
| Frontend | FE node information |
| FeStartTime | FE start time |

**Example**:
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