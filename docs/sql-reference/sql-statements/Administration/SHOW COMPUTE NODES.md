# SHOW COMPUTE NODES

## Description

View all CN nodes in your StarRocks cluster.

## Syntax

```SQL
SHOW COMPUTE NODES;
```

## Output

The following table describes the parameters returned by this statement.

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| LastStartTime        | The last time at which the CN node starts.                   |
| LastHeartbeat        | The last time at which the CN node sends a heartbeat.        |
| Alive                | Whether the CN node is available or not.                     |
| SystemDecommissioned | If the value of the parameter is `true`, this means the CN node is removed from your StarRocks cluster. Before removal, the data in the CN node is cloned. |
| ErrMsg               | The error message if the CN node fails to send a heartbeat.  |
| Status               | The state information of the CN node, which displays in the JSON format. Currently, you can only see the last time at which the CN node sends its state. |
