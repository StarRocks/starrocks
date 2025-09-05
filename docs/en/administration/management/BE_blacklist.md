---
displayed_sidebar: docs
---

# Manage BE and CN Blacklist


From v3.3.0 onwards, StarRocks supports the BE Blacklist feature, which allows you to forbid the usage of certain BE nodes in query execution, thereby avoiding frequent query failures or other unexpected behaviors caused by the failed connections to the BE nodes. A network issue preventing connections to one or more BEs would be an example of when to use the blacklist.

From v4.0 onwards, StarRocks supports adding Compute Nodes (CNs) to the Blacklist.

By default, StarRocks can automatically manage the BE and CN Blacklist, adding the BE or CN nodes that have lost connection to the blacklist and removing them from the blacklist when the connection is reestablished. However, StarRocks will not remove the node from the Blacklist if it is manually blacklisted.

:::note

- Only users with the SYSTEM-level BLACKLIST privilege can use this feature.
- Each FE node keeps its own BE and CN Blacklist, and will not share it with other FE nodes.

:::

## Add a BE/CN to the blacklist

You can manually add a BE/CN node to the Blacklist using [ADD BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md). In this statement, you must specify the ID of the BE/CN node to be blacklisted. You can obtain the BE ID by executing [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) and CN ID by executing [SHOW COMPUTE NODES](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_COMPUTE_NODES.md).

Example:

```SQL
-- Obtain BE ID.
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- Add BE to the blacklist.
ADD BACKEND BLACKLIST 10001;

-- Obtain CN ID.
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- Add CN to the blacklist.
ADD COMPUTE NODE BLACKLIST 10005;
```

## Remove a BE/CN from blacklist

You can manually remove a BE/CN node from the Blacklist using [DELETE BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md). In this statement, you must also specify the ID of the BE/CN node.

Example:

```SQL
-- Remove a BE from the Blacklist.
DELETE BACKEND BLACKLIST 10001;

-- Remove a CN from the Blacklist.
DELETE COMPUTE NODE BLACKLIST 10005;
```

## View BE/CN Blacklist

You can view the BE/CN nodes in the Blacklist using [SHOW BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md).

Example:

```SQL
-- View the BE Blacklist.
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+

-- View the CN Blacklist.
SHOW COMPUTE NODE BLACKLIST;
+---------------+------------------+---------------------+------------------------------+--------------------+
| ComputeNodeId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+---------------+------------------+---------------------+------------------------------+--------------------+
| 10005         | MANUAL           | 2025-08-18 10:47:51 | 0                            | 5                  |
+---------------+------------------+---------------------+------------------------------+--------------------+
```

The following fields are returned:

- `AddBlackListType`: How the BE/CN node was added to the blacklist. `MANUAL` indicates it is manually blacklisted by the user. `AUTO` indicates it is automatically blacklisted by StarRocks.
- `LostConnectionTime`:
  - For the `MANUAL` type, it indicates the time when the BE/CN node was manually added to the blacklist.
  - For the `AUTO` type, it indicates the time when the last successful connection was established.
- `LostConnectionNumberInPeriod`: The number of disconnections detected within `CheckTimePeriod(s)`, which is the interval at which StarRocks checks the connection status of the BE/CN nodes in the blacklist.
- `CheckTimePeriod(s)`: The interval at which StarRocks checks the connection status of the blacklisted BE/CN nodes. Its value is evaluated to the value you specified for the FE configuration item `black_host_history_sec`. Unit: Seconds.

## Configure automatic management of BE/CN Blacklist

Each time a BE/CN node loses connection to the FE node, or a query fails due to timeout on a BE/CN node, the FE node adds the BE/CN node to its BE and CN Blacklist. The FE node will constantly assess the connectivity of the BE/CN node in the blacklist by counting its connection failures within a certain duration of time. StarRocks will remove a blacklisted BE/CN node only if the number of its connection failures is below a pre-specified threshold.

You can configure the automatic management of the BE and CN Blacklist using the following [FE configurations](./FE_configuration.md):

- `black_host_history_sec`: The time duration for retaining historical connection failures of BE/CN nodes in the Blacklist.
- `black_host_connect_failures_within_time`: The threshold of connection failures allowed for a blacklisted BE/CN node.

If a BE/CN node is added to the Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the Blacklist. Within `black_host_history_sec`, only if a blacklisted BE/CN node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the Blacklist.
