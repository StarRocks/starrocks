---
displayed_sidebar: docs
---

# SHOW FRONTENDS

SHOW FRONTENDS views the information of all FE nodes in the cluster, including their IP addresses, ports, roles, and status.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```sql
SHOW FRONTENDS
```

## Return

```sql
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
| Id   | Name                         | IP        | EditLogPort | HttpPort | QueryPort | RpcPort | Role   | ClusterId | Join | Alive | ReplayedJournalId | LastHeartbeat       | IsHelper | ErrMsg | StartTime           | Version       |
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
```

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                                   |
| -------------------- | ----------------------------------------------------------------- |
| Id                   | The unique ID of the FE node.                                     |
| Name                 | The name of the FE node in BDBJE (typically IP_EditLogPort_Timestamp). |
| IP                   | The IP address of the FE node.                                    |
| EditLogPort          | The port used for BDBJE communication (default: 9010).            |
| HttpPort             | The HTTP server port of the FE node (default: 8030). Used for accessing the FE web UI. |
| QueryPort            | The MySQL protocol port (default: 9030). Used by MySQL clients to connect to StarRocks. |
| RpcPort              | The Thrift server port (default: 9020). Used for internal RPC communication. |
| Role                 | The role of the FE node. <ul><li>LEADER: The master node, handles metadata writes.</li><li>FOLLOWER: Participates in elections and metadata syncing.</li><li>OBSERVER: Syncs metadata but does not participate in elections.</li></ul> |
| ClusterId            | The unique ID of the cluster. All nodes in the same cluster must have the same ClusterId. |
| Join                 | Whether the node has joined the BDBJE group. <ul><li>true: Joined.</li><li>false: Not joined (note: a node might be removed but still show true temporarily).</li></ul> |
| Alive                | Whether the node is alive and responding. <ul><li>true: Alive.</li><li>false: Not alive.</li></ul> |
| ReplayedJournalId    | The maximum metadata journal ID that the node has currently replayed. Used to check if the node is in sync with the Leader. |
| LastHeartbeat        | The timestamp of the last heartbeat received.                     |
| IsHelper             | Whether the node is a Helper node (specified during cluster startup to help other nodes join). |
| ErrMsg               | Error message displayed if the heartbeat fails                    |
| StartTime            | The time when this FE process was started.                        |
| Version              | The StarRocks version running on this FE node.                    |

## Example

View the information of all FE nodes in the cluster.

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
