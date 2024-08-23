---
displayed_sidebar: docs
---

# Manage BE Blacklist

This topic describes how to manage the BE Blacklist.

From v3.3.0 onwards, StarRocks supports the BE Blacklist feature, which allows you to forbid the usage of certain BE nodes in query execution, thereby avoiding frequent query failures or other unexpected behaviors caused by the failed connections to the BE nodes.

By default, StarRocks can automatically manage the BE Blacklist, adding the BE nodes that have lost connection to the blacklist and removing them from the blacklist when the connection is reestablished. However, StarRocks will not remove the BE node from the blacklist if the node is manually blacklisted.

:::note

- Only users with the SYSTEM-level BLACKLIST privilege can use this feature.
- Each FE node keeps its own BE Blacklist, and will not share it with other FE nodes.

:::

## Add a BE to the blacklist

You can manually add a BE node to the BE Blacklist using [ADD BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md). In this statement, you must specify the ID of the BE node to be blacklisted. You can obtain the BE ID by executing [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md).

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
```

## Remove a BE from blacklist

You can manually remove a BE node from the BE Blacklist using [DELETE BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md). In this statement, you must also specify the ID of the BE node.

Example:

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## View BE Blacklist

You can view the BE nodes in the BE Blacklist using [SHOW BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md).

Example:

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

The following fields are returned:

- `AddBlackListType`: How the BE node was added to the blacklist. `MANUAL` indicates it is manually blacklisted by the user. `AUTO` indicates it is automatically blacklisted by StarRocks.
- `LostConnectionTime`:
  - For the `MANUAL` type, it indicates the time when the BE node was manually added to the blacklist.
  - For the `AUTO` type, it indicates the time when the last successful connection was established.
- `LostConnectionNumberInPeriod`: The number of disconnections detected within `CheckTimePeriod(s)`, which is the interval at which StarRocks checks the connection status of the BE nodes in the blacklist.
- `CheckTimePeriod(s)`: The interval at which StarRocks checks the connection status of the blacklisted BE nodes. Its value is evaluated to the value you specified for the FE configuration item `black_host_history_sec`. Unit: Seconds.

## Configure automatic management of BE Blacklist

Each time a BE node loses connection to the FE node, or a query fails due to timeout on a BE node, the FE node adds the BE node to its the BE Blacklist. The FE node will constantly assess the connectivity of the BE node in the blacklist by counting its connection failures within a certain duration of time. StarRocks will remove a blacklisted BE node only if the number of its connection failures is below a pre-specified threshold.

You can configure the automatic management of the BE Blacklist using the following [FE configurations](./FE_configuration.md):

- `black_host_history_sec`: The time duration for retaining historical connection failures of BE nodes in the BE Blacklist.
- `black_host_connect_failures_within_time`: The threshold of connection failures allowed for a blacklisted BE node.

If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
