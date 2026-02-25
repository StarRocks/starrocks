---
displayed_sidebar: docs
---

# SHOW BROKER

SHOW BROKER views the information of all brokers that have been added to the StarRocks cluster.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```sql
SHOW BROKER
```

## Return

```sql
+-----------+-----------+------+-------+---------------------+---------------------+--------+
| Name      | IP        | Port | Alive | LastStartTime       | LastUpdateTime      | ErrMsg |
+-----------+-----------+------+-------+---------------------+---------------------+--------+
```

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                                   |
| -------------------- | ----------------------------------------------------------------- |
| Name                 | The name of the broker.                                           |
| IP                   | The IP address where the broker is running.                       |
| Port                 | The port used by the broker (default:  8000).                     |
| Alive                | Whether the broker node is reachable. <ul><li>true: The broker is alive.</li><li>false: The broker is not reachable.</li></ul> |
| LastStartTime        | The last time when the broker process was started.                |
| LastUpdateTime       | The last time the broker reported a heartbeat to the Frontend (FE). |
| ErrMsg               | Displays error messages if the heartbeat fails or connection is lost. |

## Example

View the information of all brokers in the cluster.

```Plain
SHOW BROKER;

+-----------+-----------+------+-------+---------------------+---------------------+--------+
| Name      | IP        | Port | Alive | LastStartTime       | LastUpdateTime      | ErrMsg |
+-----------+-----------+------+-------+---------------------+---------------------+--------+
| my_broker | 127.0.0.1 | 8000 | true  | 2026-01-21 12:40:00 | 2026-01-21 12:40:05 |        |
+-----------+-----------+------+-------+---------------------+---------------------+--------+
```
