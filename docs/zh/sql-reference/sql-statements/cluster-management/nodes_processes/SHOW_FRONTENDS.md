---
displayed_sidebar: docs
---

# SHOW FRONTENDS

SHOW FRONTENDS 用于查看集群中所有 FE 节点的信息，包括 IP 地址、端口、角色和状态。

:::tip

只有拥有 SYSTEM 级 OPERATE 权限或 `cluster_admin` 角色的用户才能执行此操作。

:::

## 语法

```sql
SHOW FRONTENDS
```

## 返回值

```sql
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
| Id   | Name                         | IP        | EditLogPort | HttpPort | QueryPort | RpcPort | Role   | ClusterId | Join | Alive | ReplayedJournalId | LastHeartbeat       | IsHelper | ErrMsg | StartTime           | Version       |
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
```

下表描述了该语句返回的参数：

| **参数**              | **说明**                                                          |
| -------------------- | ----------------------------------------------------------------- |
| Id                   | FE 节点的唯一 ID。                                                  |
| Name                 | BDBJE 中 FE 节点的名称（通常为 IP_EditLogPort_Timestamp）。            |
| IP                   | FE 节点的 IP 地址。                                                 |
| EditLogPort          | 用于 BDBJE 通信的端口（默认为 9010）。                                 |
| HttpPort             | FE 节点的 HTTP 服务器端口（默认为 8030）。用于访问 FE Web UI。            |
| QueryPort            | MySQL 协议端口（默认为 9030）。MySQL 客户端用于连接 StarRocks。           |
| RpcPort              | Thrift 服务器端口（默认为 9020）。用于内部 RPC 通信。                    |
| Role                 | FE 节点的角色。 <ul><li>LEADER: 主节点，处理元数据写入。</li><li>FOLLOWER: 参与选举和元数据同步。</li><li>OBSERVER: 同步元数据但不参与选举。</li></ul> |
| ClusterId            | 集群的唯一 ID。同一集群中的所有节点必须具有相同的 ClusterId。              |
| Join                 | 该节点是否已加入 BDBJE 组。 <ul><li>true: 已加入。</li><li>false: 未加入（注意：节点被移除后可能暂时仍显示为 true）。</li></ul> |
| Alive                | 节点是否存活并响应。 <ul><li>true: 存活。</li><li>false: 不存活。</li></ul> |
| ReplayedJournalId    | 该节点当前已回放的最大元数据日志 ID。用于检查节点是否与 Leader 同步。       |
| LastHeartbeat        | 最近一次收到心跳的时间戳。                                            |
| IsHelper             | 是否为 Helper 节点（在集群启动期间指定，用于帮助其他节点加入）。            |
| ErrMsg               | 心跳失败时显示的错误信息。                                            |
| StartTime            | 该 FE 进程启动的时间。                                               |
| Version              | 该 FE 节点上运行的 StarRocks 版本。                                  |

## 示例

查看集群中所有 FE 节点的信息。

```Plain
mysql> SHOW FRONTENDS\G;
*************************** 1. row ***************************
               Id: 1
             Name: 127.0.0.1_9010_1766983864084
               IP: 127.0.0.1
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 391198626
             Join: true
            Alive: true
ReplayedJournalId: 59720
    LastHeartbeat: 2026-01-21 13:24:20
         IsHelper: true
           ErrMsg:
        StartTime: 2026-01-20 12:58:48
          Version: 4.0.2-1f1aa9c
1 row in set (0.01 sec)
```
