---
displayed_sidebar: "English"
---

# SHOW COMPUTE NODES

## Description

View all compute nodes in your StarRocks cluster.

:::tip

Only users with the SYSTEM-level OPERATE privilege or the `cluster_admin` role can perform this operation.

:::

## Syntax

```SQL
SHOW COMPUTE NODES
```

## Output

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| LastStartTime        | The last time at which the compute node starts.                   |
| LastHeartbeat        | The last time at which the compute node sends a heartbeat.        |
| Alive                | Whether the compute node is available or not.                     |
| SystemDecommissioned | If the value of the parameter is `true`, the compute node is removed from your StarRocks cluster. Before removal, the data in the compute node is cloned. |
| ErrMsg               | The error message if the compute node fails to send a heartbeat.  |
| Status               | The state of the compute node, which displays in the JSON format. Currently, you can only see the last time at which the compute node sends its state. |
